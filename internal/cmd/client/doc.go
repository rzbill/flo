// Package client provides the `flo` command-line client.
//
// The CLI talks to the Flo HTTP and gRPC endpoints to perform common
// stream operations from a terminal. It is primarily intended for
// developers and operators.
//
// Installation
//
//	go install github.com/rzbill/flo/cmd/flo@latest
//
// Or build from this repo and use the embedded `flo` binary.
//
// # Address configuration
//
// The HTTP base URL is discovered by the application that embeds the
// commands via a BaseURLFunc. When using the standalone binary, it
// defaults to http://127.0.0.1:8080. The gRPC address is read from the
// FLO_GRPC environment variable (default 127.0.0.1:50051).
//
// Usage
//
//	flo stream create --namespace default --name demo --partitions 16
//
//	flo stream publish \
//	    --namespace default --stream demo \
//	    --data '{"hello":"world"}' \
//	    --header idempotencyKey=pub-123 \
//	    --header-json '{"eventType":"user.registered"}'
//
//	flo stream messages --namespace default --name demo --partition 0 --limit 10
//
//	flo stream stats --namespace default --name demo
//	flo stream stats --all-ns --name demo        # aggregate across namespaces
//
//	# Subscribe via gRPC; auto-acks by default when a group is provided
//	flo stream subscribe \
//	    --namespace default --stream demo --group ui --from latest --limit 5
//	flo stream subscribe --namespace default --stream demo --no-ack
//
//	# Start position options
//	# from earliest
//	flo stream subscribe --namespace default --stream demo --from earliest
//	# from an absolute time (RFC3339)
//	flo stream subscribe --namespace default --stream demo --at 2025-09-20T12:00:00Z
//	# from a unix epoch in milliseconds
//	flo stream subscribe --namespace default --stream demo --at 1726833600000
//
//	# Delete a specific message by base64 ID
//	flo stream delete --namespace default --stream demo --message-id BASE64_ID
//
//	# Flush all messages in a stream (requires --confirm)
//	flo stream flush --namespace default --stream demo --confirm
//	flo stream flush --namespace default --stream demo --partition 0 --confirm
//
// Notes
//
//   - subscribe connects to the gRPC StreamsService.Subscribe stream.
//     When --group is set and --no-ack is not, the client will Ack each
//     message after printing it. Use --no-ack to test retry/DLQ behavior.
//     Start precedence: --start-token (if provided) > --at (RFC3339 or ms) > --from.
//   - messages and stats use the HTTP API exposed by the Flo server.
//   - publish accepts repeated --header key=value flags or a single
//     --header-json with a JSON object to populate message headers.
package client
