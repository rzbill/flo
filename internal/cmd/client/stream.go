// Package client contains Cobra CLI commands for Flo.
package client

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	transports "github.com/rzbill/flo/internal/cmd/client/transports"
	"github.com/spf13/cobra"
)

// BaseURLFunc provides the base HTTP API URL (e.g., from env or flag).
type BaseURLFunc func() string

func getTransport() transports.StreamsTransport {
	// For now, only gRPC transport; can add HTTP in future.
	return transports.NewGrpcTransport(dialGRPCContext)
}

// NewStreamCommand constructs the `stream` command group and subcommands.
func NewStreamCommand(baseURL BaseURLFunc) *cobra.Command {
	streamCmd := &cobra.Command{Use: "stream", Short: "Stream operations"}

	streamCmd.AddCommand(
		newStreamCreateCommand(),
		newStreamMessagesCommand(),
		newStreamStatsCommand(baseURL),
		newStreamPublishCommand(),
		newStreamSubscribeCommand(),
		newStreamFlushCommand(),
		newStreamDeleteCommand(),
		newStreamTailCommand(),
	)

	return streamCmd
}

// newStreamTailCommand constructs the `stream tail` subcommand.
func newStreamTailCommand() *cobra.Command {
	tailCmd := &cobra.Command{
		Use:   "tail",
		Short: "Tail a stream without a group (no acks/retries/cursor)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			st, _ := cmd.Flags().GetString("stream")
			from, _ := cmd.Flags().GetString("from")
			at, _ := cmd.Flags().GetString("at")
			startTokenB64, _ := cmd.Flags().GetString("start-token")
			limit, _ := cmd.Flags().GetInt("limit")
			filter, _ := cmd.Flags().GetString("filter")

			var startTok []byte
			var fromOpt string
			var atMs int64
			if startTokenB64 != "" {
				b, err := base64.StdEncoding.DecodeString(startTokenB64)
				if err != nil {
					return fmt.Errorf("invalid start-token: %w", err)
				}
				startTok = b
			}
			if from == "earliest" {
				fromOpt = "earliest"
			}
			if at != "" {
				if ms, err := strconv.ParseInt(at, 10, 64); err == nil {
					atMs = ms
				} else if t, err := time.Parse(time.RFC3339, at); err == nil {
					atMs = t.UnixMilli()
				} else {
					return fmt.Errorf("invalid --at; expected ms or RFC3339")
				}
			}

			enc := json.NewEncoder(cmd.OutOrStdout())
			t := getTransport()
			err := t.Tail(cmd.Context(), transports.TailRequest{
				Namespace:  ns,
				Stream:     st,
				StartToken: startTok,
				From:       fromOpt,
				AtMs:       atMs,
				Limit:      limit,
				Filter:     filter,
			}, func(id, payload []byte) error {
				dm := decodedMessage(id, payload)
				_ = enc.Encode(dm)
				return nil
			})
			return err
		},
	}
	tailCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	tailCmd.Flags().String("stream", "", "Stream")
	tailCmd.Flags().String("from", "latest", "Start position: latest|earliest")
	tailCmd.Flags().String("at", "", "Start at timestamp: RFC3339 or ms")
	tailCmd.Flags().String("start-token", "", "Start token (base64)")
	tailCmd.Flags().Int("limit", 0, "Stop after N messages (0 = infinite)")
	tailCmd.Flags().String("filter", "", "CEL filter (server-side)")
	return tailCmd
}

// newStreamCreateCommand constructs the `stream create` subcommand.
func newStreamCreateCommand() *cobra.Command {
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create stream",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			parts, _ := cmd.Flags().GetInt("partitions")
			if err := getTransport().Create(cmd.Context(), ns, name, parts); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "status:", "OK")
			return nil
		},
	}
	createCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	createCmd.Flags().String("name", "", "Stream name")
	createCmd.Flags().Int("partitions", 0, "Partitions override")
	return createCmd
}

// newStreamMessagesCommand constructs the `stream messages` subcommand.
func newStreamMessagesCommand() *cobra.Command {
	messagesCmd := &cobra.Command{
		Use:   "messages",
		Short: "List messages from a stream partition",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			partition, _ := cmd.Flags().GetInt("partition")
			limit, _ := cmd.Flags().GetInt("limit")
			reverse, _ := cmd.Flags().GetBool("reverse")
			startTokenB64, _ := cmd.Flags().GetString("start-token")

			var startTok []byte
			if startTokenB64 != "" {
				b, err := base64.StdEncoding.DecodeString(startTokenB64)
				if err != nil {
					return fmt.Errorf("invalid start-token: %w", err)
				}
				startTok = b
			}

			items, nextTok, err := getTransport().ListMessages(cmd.Context(), ns, name, partition, startTok, limit, reverse)
			if err != nil {
				return err
			}

			var out struct {
				Namespace string `json:"namespace"`
				Stream    string `json:"stream"`
				Partition uint32 `json:"partition"`
				Items     []struct {
					ID, Payload, Header []byte
					Seq                 uint64
					Partition           uint32
				} `json:"items"`
				NextToken string `json:"next_token"`
			}
			out.Namespace = ns
			out.Stream = name
			out.Partition = uint32(partition)
			out.Items = make([]struct {
				ID, Payload, Header []byte
				Seq                 uint64
				Partition           uint32
			}, 0, len(items))
			for _, it := range items {
				out.Items = append(out.Items, struct {
					ID, Payload, Header []byte
					Seq                 uint64
					Partition           uint32
				}{ID: it.ID, Payload: it.Payload, Header: it.Header, Seq: it.Seq, Partition: it.Partition})
			}
			if len(nextTok) > 0 {
				out.NextToken = base64.StdEncoding.EncodeToString(nextTok)
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(out)
		},
	}
	messagesCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	messagesCmd.Flags().String("name", "", "Stream name")
	messagesCmd.Flags().Int("partition", 0, "Partition number")
	messagesCmd.Flags().Int("limit", 100, "Max items to return")
	messagesCmd.Flags().Bool("reverse", false, "Read newest-to-oldest")
	messagesCmd.Flags().String("start-token", "", "Start token (base64)")
	return messagesCmd
}

// newStreamStatsCommand constructs the `stream stats` subcommand.
func newStreamStatsCommand(baseURL BaseURLFunc) *cobra.Command {
	statsCmd := &cobra.Command{
		Use:   "stats",
		Short: "Get per-partition stream stats",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			all, _ := cmd.Flags().GetBool("all-ns")

			if all {
				// Keep HTTP for cross-namespace aggregator for now (no gRPC method for all-ns aggregate in one call).
				url := fmt.Sprintf("%s/v1/streams/stats?stream=%s&all=true", baseURL(), name)
				if ns != "" {
					url += "&namespace=" + ns
				}
				resp, err := http.Get(url)
				if err != nil {
					return err
				}
				defer func() { _ = resp.Body.Close() }()
				if resp.StatusCode >= 300 {
					_, _ = io.Copy(io.Discard, resp.Body)
					return fmt.Errorf("http error: %s", resp.Status)
				}
				var data struct {
					Stream     string `json:"stream"`
					Namespaces []struct {
						Namespace  string `json:"namespace"`
						Partitions []struct {
							Partition                uint32
							FirstSeq, LastSeq, Count uint64
							Bytes                    uint64
						} `json:"partitions"`
						TotalCount uint64 `json:"total_count"`
					} `json:"namespaces"`
					GrandTotal uint64 `json:"grand_total"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
					return err
				}
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(data)
			}

			stats, err := getTransport().GetStats(cmd.Context(), ns, name)
			if err != nil {
				return err
			}
			var data struct {
				Namespace  string `json:"namespace"`
				Stream     string `json:"stream"`
				Partitions []struct {
					Partition                uint32
					FirstSeq, LastSeq, Count uint64
					Bytes                    uint64
				} `json:"partitions"`
				TotalCount        uint64 `json:"total_count"`
				TotalBytes        uint64 `json:"total_bytes"`
				LastPublishMs     uint64 `json:"last_publish_ms"`
				LastDeliveredMs   uint64 `json:"last_delivered_ms"`
				ActiveSubscribers int    `json:"active_subscribers"`
				GroupsCount       int    `json:"groups_count"`
			}
			data.Namespace = ns
			data.Stream = name
			data.TotalCount = stats.TotalCount
			data.TotalBytes = stats.TotalBytes
			data.LastPublishMs = stats.LastPublishMs
			data.LastDeliveredMs = stats.LastDeliveredMs
			data.ActiveSubscribers = stats.ActiveSubscribers
			data.GroupsCount = stats.GroupsCount
			data.Partitions = make([]struct {
				Partition                uint32
				FirstSeq, LastSeq, Count uint64
				Bytes                    uint64
			}, 0, len(stats.Partitions))
			for _, ps := range stats.Partitions {
				data.Partitions = append(data.Partitions, struct {
					Partition                uint32
					FirstSeq, LastSeq, Count uint64
					Bytes                    uint64
				}{Partition: ps.Partition, FirstSeq: ps.FirstSeq, LastSeq: ps.LastSeq, Count: ps.Count, Bytes: ps.Bytes})
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(data)
		},
	}
	statsCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	statsCmd.Flags().String("name", "", "Stream name")
	statsCmd.Flags().Bool("all-ns", false, "Aggregate across all namespaces")
	return statsCmd
}

// newStreamPublishCommand constructs the `stream publish` subcommand.
func newStreamPublishCommand() *cobra.Command {
	publishCmd := &cobra.Command{
		Use:   "publish",
		Short: "Publish a message to a stream",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			st, _ := cmd.Flags().GetString("stream")
			data, _ := cmd.Flags().GetString("data")
			// Parse headers from flags
			rawHeaders, _ := cmd.Flags().GetStringArray("header")
			headersJSON, _ := cmd.Flags().GetString("header-json")
			headers := map[string]string{}
			for _, hv := range rawHeaders {
				if hv == "" {
					continue
				}
				parts := strings.SplitN(hv, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid --header, expected key=value: %s", hv)
				}
				headers[strings.TrimSpace(parts[0])] = parts[1]
			}
			if headersJSON != "" {
				var m map[string]string
				if err := json.Unmarshal([]byte(headersJSON), &m); err != nil {
					return fmt.Errorf("invalid --header-json: %w", err)
				}
				for k, v := range m {
					headers[k] = v
				}
			}

			if err := getTransport().Publish(cmd.Context(), ns, st, []byte(data), headers); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "status:", "OK")
			return nil
		},
	}
	publishCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	publishCmd.Flags().String("stream", "", "Stream")
	publishCmd.Flags().String("data", "", "Payload data")
	// Header flags
	publishCmd.Flags().StringArray("header", []string{}, "Message header key=value (repeat)")
	publishCmd.Flags().String("header-json", "", "Headers as JSON object, e.g. '{\"k\":\"v\"}'")
	return publishCmd
}

// newStreamSubscribeCommand constructs the `stream subscribe` subcommand.
func newStreamSubscribeCommand() *cobra.Command {
	subscribeCmd := &cobra.Command{
		Use:   "subscribe",
		Short: "Subscribe to a stream stream",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			st, _ := cmd.Flags().GetString("stream")
			group, _ := cmd.Flags().GetString("group")
			from, _ := cmd.Flags().GetString("from")
			at, _ := cmd.Flags().GetString("at")
			startTokenB64, _ := cmd.Flags().GetString("start-token")
			limit, _ := cmd.Flags().GetInt("limit")
			noAck, _ := cmd.Flags().GetBool("no-ack")
			ackFlag, _ := cmd.Flags().GetBool("ack")
			nackFlag, _ := cmd.Flags().GetBool("nack")
			nackErrMsg, _ := cmd.Flags().GetString("nack-error")
			backoffType, _ := cmd.Flags().GetString("backoff-type")
			backoffBaseMs, _ := cmd.Flags().GetInt64("backoff-base-ms")
			backoffCapMs, _ := cmd.Flags().GetInt64("backoff-cap-ms")
			backoffFactor, _ := cmd.Flags().GetFloat64("backoff-factor")
			maxAttempts, _ := cmd.Flags().GetUint32("max-attempts")
			retryPaceMs, _ := cmd.Flags().GetInt64("retry-pace-ms")

			// Validate mutually exclusive ack/nack/no-ack combinations
			if ackFlag && nackFlag {
				return fmt.Errorf("--ack and --nack cannot be used together")
			}
			if noAck && (ackFlag || nackFlag) {
				return fmt.Errorf("--no-ack cannot be combined with --ack or --nack")
			}

			// Compute start token and options for gRPC
			var startTok []byte
			var fromOpt string
			var atMs int64
			if startTokenB64 != "" {
				// explicit start token overrides others
				if b, err := base64.StdEncoding.DecodeString(startTokenB64); err == nil {
					startTok = b
				} else {
					return fmt.Errorf("invalid start-token: %w", err)
				}
			}
			if from == "earliest" {
				fromOpt = "earliest"
			}
			if at != "" {
				// parse ms or RFC3339
				if ms, err := strconv.ParseInt(at, 10, 64); err == nil {
					atMs = ms
				} else if t, err := time.Parse(time.RFC3339, at); err == nil {
					atMs = t.UnixMilli()
				} else {
					return fmt.Errorf("invalid --at; expected ms or RFC3339")
				}
			}

			enc := json.NewEncoder(cmd.OutOrStdout())
			t := getTransport()
			sreq := transports.SubscribeRequest{
				Namespace:  ns,
				Stream:     st,
				Group:      group,
				StartToken: startTok,
				From:       fromOpt,
				AtMs:       atMs,
				Limit:      limit,
			}
			// Optional policy
			if backoffType != "" || backoffBaseMs > 0 || backoffCapMs > 0 || backoffFactor > 0 || maxAttempts > 0 {
				sreq.Policy = &transports.RetryPolicy{Type: backoffType, BaseMs: backoffBaseMs, CapMs: backoffCapMs, Factor: backoffFactor, MaxAttempts: maxAttempts}
			}
			if retryPaceMs > 0 {
				sreq.RetryPaceMs = retryPaceMs
			}

			err := t.Subscribe(cmd.Context(), sreq, func(id, payload []byte) error {
				dm := decodedMessage(id, payload)
				_ = enc.Encode(dm)
				if group != "" {
					if nackFlag {
						if err := t.Nack(cmd.Context(), ns, st, group, id, nackErrMsg); err != nil {
							return err
						}
						return nil
					}
					if ackFlag || (!noAck && !nackFlag) {
						if err := t.Ack(cmd.Context(), ns, st, group, id); err != nil {
							return err
						}
					}
				}
				return nil
			})
			return err
		},
	}
	subscribeCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	subscribeCmd.Flags().String("stream", "", "Stream")
	subscribeCmd.Flags().String("group", "", "Subscription group (durable cursor)")
	subscribeCmd.Flags().String("from", "latest", "Start position: latest|earliest")
	subscribeCmd.Flags().String("at", "", "Start at timestamp: RFC3339 or ms")
	subscribeCmd.Flags().String("start-token", "", "Start token (base64)")
	subscribeCmd.Flags().Int("limit", 0, "Stop after N messages (0 = infinite)")
	subscribeCmd.Flags().Bool("no-ack", false, "Don't auto-ack messages")
	subscribeCmd.Flags().Bool("ack", false, "Explicitly ack each received message (requires --group)")
	subscribeCmd.Flags().Bool("nack", false, "Explicitly nack each received message (requires --group)")
	subscribeCmd.Flags().String("nack-error", "", "Optional error message to include with --nack")
	// Retry policy flags
	subscribeCmd.Flags().String("backoff-type", "", "Retry backoff type: exp|exp-jitter|fixed|none")
	subscribeCmd.Flags().Int64("backoff-base-ms", 0, "Retry backoff base in ms")
	subscribeCmd.Flags().Int64("backoff-cap-ms", 0, "Retry backoff cap in ms")
	subscribeCmd.Flags().Float64("backoff-factor", 0, "Retry backoff exponential factor")
	subscribeCmd.Flags().Uint32("max-attempts", 0, "Maximum retry attempts before DLQ")
	subscribeCmd.Flags().Int64("retry-pace-ms", 0, "Pacing between multiple due retries in ms")
	return subscribeCmd
}

// newStreamFlushCommand constructs the `stream flush` subcommand.
func newStreamFlushCommand() *cobra.Command {
	flushCmd := &cobra.Command{
		Use:   "flush",
		Short: "Flush all messages from a stream (or specific partition)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			st, _ := cmd.Flags().GetString("stream")
			partition, _ := cmd.Flags().GetInt("partition")
			confirm, _ := cmd.Flags().GetBool("confirm")

			if st == "" {
				return fmt.Errorf("stream name is required")
			}

			if !confirm {
				var partitionStr string
				if partition >= 0 {
					partitionStr = fmt.Sprintf(" (partition %d)", partition)
				}
				return fmt.Errorf("use --confirm to flush all messages from stream %s%s", st, partitionStr)
			}

			deleted, err := getTransport().Flush(cmd.Context(), ns, st, partition)
			if err != nil {
				return err
			}
			var data struct {
				Namespace    string  `json:"namespace"`
				Stream       string  `json:"stream"`
				DeletedCount uint64  `json:"deleted_count"`
				Partition    *uint32 `json:"partition"`
			}
			data.Namespace = ns
			data.Stream = st
			data.DeletedCount = deleted
			if partition >= 0 {
				p := uint32(partition)
				data.Partition = &p
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(data)
		},
	}
	flushCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	flushCmd.Flags().String("stream", "", "Stream name")
	flushCmd.Flags().Int("partition", -1, "Partition to flush (default: all partitions)")
	flushCmd.Flags().Bool("confirm", false, "Confirm the flush operation")
	return flushCmd
}

// newStreamDeleteCommand constructs the `stream delete` subcommand.
func newStreamDeleteCommand() *cobra.Command {
	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a specific message by ID from a stream",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			st, _ := cmd.Flags().GetString("stream")
			messageID, _ := cmd.Flags().GetString("message-id")

			if st == "" || messageID == "" {
				return fmt.Errorf("stream and message-id are required")
			}

			// Validate and decode base64 message ID
			decodedID, err := base64.StdEncoding.DecodeString(messageID)
			if err != nil {
				return fmt.Errorf("invalid message-id format: %w", err)
			}

			if err := getTransport().DeleteMessage(cmd.Context(), ns, st, decodedID); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Message %s deleted from stream %s\n", messageID, st)
			return nil
		},
	}
	deleteCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	deleteCmd.Flags().String("stream", "", "Stream name")
	deleteCmd.Flags().String("message-id", "", "Message ID (base64)")
	return deleteCmd
}
