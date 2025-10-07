package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"unicode/utf8"

	flov1 "github.com/rzbill/flo/api/flo/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// grpcAddrFromEnv returns the gRPC server address from FLO_GRPC or a default.
func grpcAddrFromEnv() string {
	if addr := os.Getenv("FLO_GRPC"); addr != "" {
		return addr
	}
	return "127.0.0.1:50051"
}

// dialGRPCContext dials the Flo gRPC endpoint with insecure transport for local/dev.
func dialGRPCContext(ctx context.Context) (*grpc.ClientConn, error) {
	addr := grpcAddrFromEnv()
	return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// withStreamsClient provides a StreamsService client and ensures the connection is closed.
func withStreamsClient(ctx context.Context, fn func(flov1.StreamsServiceClient) error) error {
	conn, err := dialGRPCContext(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	cli := flov1.NewStreamsServiceClient(conn)
	return fn(cli)
}

// withWorkQueuesClient provides a WorkQueuesService client and ensures the connection is closed.
func withWorkQueuesClient(ctx context.Context, fn func(flov1.WorkQueuesServiceClient) error) error {
	conn, err := dialGRPCContext(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	cli := flov1.NewWorkQueuesServiceClient(conn)
	return fn(cli)
}

// decodedMessage returns a map with id_b64 and one of payload_json, payload_text, or payload_b64.
func decodedMessage(id, payload []byte) map[string]any {
	out := map[string]any{
		"id_b64": base64.StdEncoding.EncodeToString(id),
	}
	// Try JSON first if it looks like JSON
	if len(payload) > 0 && (payload[0] == '{' || payload[0] == '[') {
		var v any
		if json.Unmarshal(payload, &v) == nil {
			out["payload_json"] = v
			return out
		}
	}
	// Then UTF-8 text if valid and mostly printable
	if utf8.Valid(payload) {
		out["payload_text"] = string(payload)
		return out
	}
	// Fallback to base64
	out["payload_b64"] = base64.StdEncoding.EncodeToString(payload)
	return out
}
