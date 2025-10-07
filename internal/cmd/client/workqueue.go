// Package client contains Cobra CLI commands for Flo.
package client

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	flov1 "github.com/rzbill/flo/api/flo/v1"
	"github.com/spf13/cobra"
)

// NewWorkQueueCommand constructs the `workqueue` command group and subcommands.
func NewWorkQueueCommand() *cobra.Command {
	wqCmd := &cobra.Command{
		Use:     "workqueue",
		Aliases: []string{"wq"},
		Short:   "WorkQueue operations (task queues with leases and retry)",
		Long: `WorkQueue operations for one-of-N task processing.

Message Lifecycle:
  Ready → [Dequeue] → Pending → [Complete] → Completed
                         ↓ (fail after retries)
                        DLQ

State Commands:
  ready       List messages waiting in queue (available for dequeue)
  pending     List messages being processed (leased to consumers)
  completed   List recently completed messages (last 1K per partition)
  dlq         List failed messages in dead letter queue
  messages    Unified view across states (--all or --state)

Core Operations:
  create      Create a new WorkQueue
  enqueue     Add a message to the queue
  dequeue     Remove and lease a message (gRPC streaming)
  complete    Mark a message as successfully processed
  fail        Mark a message as failed (retry or DLQ)

Consumer Management:
  register-consumer    Register a new consumer
  consumers            List active consumers
  heartbeat            Send consumer heartbeat
  unregister-consumer  Remove a consumer

Advanced:
  extend-lease    Extend lease on a message
  claim           Manually claim messages from PEL
  stats           Get WorkQueue statistics
  flush           Delete all messages`,
	}

	wqCmd.AddCommand(
		newWorkQueueCreateCommand(),
		newWorkQueueEnqueueCommand(),
		newWorkQueueDequeueCommand(),
		newWorkQueueCompleteCommand(),
		newWorkQueueFailCommand(),
		newWorkQueueExtendLeaseCommand(),
		newWorkQueueMessagesCommand(),
		newWorkQueueStatsCommand(),
		newWorkQueueMessagesReadyCommand(),
		newWorkQueueMessagePendingCommand(),
		newWorkQueueMessageCompletedCommand(),
		newWorkQueueConsumersCommand(),
		newWorkQueueRegisterConsumerCommand(),
		newWorkQueueHeartbeatCommand(),
		newWorkQueueUnregisterConsumerCommand(),
		newWorkQueueClaimCommand(),
		newWorkQueueFlushCommand(),
	)

	return wqCmd
}

// newWorkQueueCreateCommand constructs the `workqueue create` subcommand.
func newWorkQueueCreateCommand() *cobra.Command {
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a work queue",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			partitions, _ := cmd.Flags().GetInt32("partitions")

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.CreateWorkQueueRequest{
					Namespace:  ns,
					Name:       name,
					Partitions: partitions,
				}
				if _, err := cli.CreateWorkQueue(cmd.Context(), req); err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "status:", "OK")
				return nil
			})
		},
	}
	createCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	createCmd.Flags().String("name", "", "WorkQueue name")
	createCmd.Flags().Int32("partitions", 16, "Number of partitions")
	return createCmd
}

// newWorkQueueEnqueueCommand constructs the `workqueue enqueue` subcommand.
func newWorkQueueEnqueueCommand() *cobra.Command {
	enqueueCmd := &cobra.Command{
		Use:   "enqueue",
		Short: "Enqueue a message to a work queue",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			data, _ := cmd.Flags().GetString("data")
			priority, _ := cmd.Flags().GetInt32("priority")
			delayMs, _ := cmd.Flags().GetInt64("delay-ms")
			key, _ := cmd.Flags().GetString("key")

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

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.EnqueueRequest{
					Namespace: ns,
					Name:      name,
					Payload:   []byte(data),
					Headers:   headers,
					Priority:  priority,
					DelayMs:   delayMs,
					Key:       key,
				}

				resp, err := cli.Enqueue(cmd.Context(), req)
				if err != nil {
					return err
				}

				out := map[string]interface{}{
					"status": "OK",
					"id":     base64.StdEncoding.EncodeToString(resp.Id),
				}
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(out)
			})
		},
	}
	enqueueCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	enqueueCmd.Flags().String("name", "", "WorkQueue name")
	enqueueCmd.Flags().String("data", "", "Payload data")
	enqueueCmd.Flags().String("key", "", "Partitioning key (optional)")
	enqueueCmd.Flags().Int32("priority", 0, "Message priority (higher = processed first)")
	enqueueCmd.Flags().Int64("delay-ms", 0, "Delay in milliseconds before message is available")
	enqueueCmd.Flags().StringArray("header", []string{}, "Message header key=value (repeat)")
	enqueueCmd.Flags().String("header-json", "", "Headers as JSON object")
	return enqueueCmd
}

// newWorkQueueDequeueCommand constructs the `workqueue dequeue` subcommand.
func newWorkQueueDequeueCommand() *cobra.Command {
	dequeueCmd := &cobra.Command{
		Use:   "dequeue",
		Short: "Dequeue messages (worker mode)",
		Long: `Dequeue messages from a work queue and process them.
Messages will be automatically acknowledged unless --no-auto-ack is set.
Use Ctrl+C to stop.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")
			consumerID, _ := cmd.Flags().GetString("consumer-id")
			count, _ := cmd.Flags().GetInt32("count")
			blockMs, _ := cmd.Flags().GetInt64("block-ms")
			leaseMs, _ := cmd.Flags().GetInt64("lease-ms")
			autoAck, _ := cmd.Flags().GetBool("auto-ack")

			if consumerID == "" {
				consumerID = fmt.Sprintf("cli-%d", time.Now().Unix())
			}

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.DequeueRequest{
					Namespace:  ns,
					Name:       name,
					Group:      group,
					ConsumerId: consumerID,
					Count:      count,
					BlockMs:    blockMs,
					LeaseMs:    leaseMs,
				}

				stream, err := cli.Dequeue(cmd.Context(), req)
				if err != nil {
					return err
				}

				enc := json.NewEncoder(cmd.OutOrStdout())
				for {
					resp, err := stream.Recv()
					if err != nil {
						// EOF means the stream ended normally (no more messages or timeout)
						if err == io.EOF {
							return nil
						}
						return err
					}

					dm := decodedMessage(resp.Id, resp.Payload)
					dm["partition"] = resp.Partition
					dm["delivery_count"] = resp.DeliveryCount
					dm["lease_expires_at_ms"] = resp.LeaseExpiresAtMs
					dm["enqueued_at_ms"] = resp.EnqueuedAtMs
					if len(resp.Headers) > 0 {
						dm["headers"] = resp.Headers
					}

					_ = enc.Encode(dm)

					// Auto-acknowledge if enabled
					if autoAck {
						completeReq := &flov1.CompleteRequest{
							Namespace: ns,
							Name:      name,
							Group:     group,
							Id:        resp.Id,
						}
						if _, err := cli.Complete(cmd.Context(), completeReq); err != nil {
							_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Warning: failed to auto-ack: %v\n", err)
						}
					}
				}
			})
		},
	}
	dequeueCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	dequeueCmd.Flags().String("name", "", "WorkQueue name")
	dequeueCmd.Flags().StringP("group", "g", "default", "Consumer group")
	dequeueCmd.Flags().String("consumer-id", "", "Consumer ID (auto-generated if empty)")
	dequeueCmd.Flags().Int32("count", 1, "Number of messages to dequeue at once")
	dequeueCmd.Flags().Int64("block-ms", 5000, "Block time in milliseconds")
	dequeueCmd.Flags().Int64("lease-ms", 30000, "Lease duration in milliseconds")
	dequeueCmd.Flags().Bool("auto-ack", false, "Automatically acknowledge messages")
	return dequeueCmd
}

// newWorkQueueCompleteCommand constructs the `workqueue complete` subcommand.
func newWorkQueueCompleteCommand() *cobra.Command {
	completeCmd := &cobra.Command{
		Use:   "complete",
		Short: "Mark a message as successfully completed",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")
			idB64, _ := cmd.Flags().GetString("id")

			id, err := base64.StdEncoding.DecodeString(idB64)
			if err != nil {
				return fmt.Errorf("invalid --id: %w", err)
			}

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.CompleteRequest{
					Namespace: ns,
					Name:      name,
					Group:     group,
					Id:        id,
				}
				if _, err := cli.Complete(cmd.Context(), req); err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "status:", "OK")
				return nil
			})
		},
	}
	completeCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	completeCmd.Flags().String("name", "", "WorkQueue name")
	completeCmd.Flags().StringP("group", "g", "default", "Consumer group")
	completeCmd.Flags().String("id", "", "Message ID (base64)")
	return completeCmd
}

// newWorkQueueFailCommand constructs the `workqueue fail` subcommand.
func newWorkQueueFailCommand() *cobra.Command {
	failCmd := &cobra.Command{
		Use:   "fail",
		Short: "Mark a message as failed (will retry if policy allows)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")
			idB64, _ := cmd.Flags().GetString("id")
			errorMsg, _ := cmd.Flags().GetString("error")
			retryAfterMs, _ := cmd.Flags().GetInt64("retry-after-ms")

			id, err := base64.StdEncoding.DecodeString(idB64)
			if err != nil {
				return fmt.Errorf("invalid --id: %w", err)
			}

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.FailRequest{
					Namespace:    ns,
					Name:         name,
					Group:        group,
					Id:           id,
					Error:        errorMsg,
					RetryAfterMs: retryAfterMs,
				}
				if _, err := cli.Fail(cmd.Context(), req); err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "status:", "OK")
				return nil
			})
		},
	}
	failCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	failCmd.Flags().String("name", "", "WorkQueue name")
	failCmd.Flags().StringP("group", "g", "default", "Consumer group")
	failCmd.Flags().String("id", "", "Message ID (base64)")
	failCmd.Flags().String("error", "", "Error message")
	failCmd.Flags().Int64("retry-after-ms", 0, "Retry after delay override (milliseconds)")
	return failCmd
}

// newWorkQueueExtendLeaseCommand constructs the `workqueue extend-lease` subcommand.
func newWorkQueueExtendLeaseCommand() *cobra.Command {
	extendCmd := &cobra.Command{
		Use:   "extend-lease",
		Short: "Extend the lease on a message",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")
			idB64, _ := cmd.Flags().GetString("id")
			extensionMs, _ := cmd.Flags().GetInt64("extension-ms")

			id, err := base64.StdEncoding.DecodeString(idB64)
			if err != nil {
				return fmt.Errorf("invalid --id: %w", err)
			}

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.ExtendLeaseRequest{
					Namespace:   ns,
					Name:        name,
					Group:       group,
					Id:          id,
					ExtensionMs: extensionMs,
				}
				resp, err := cli.ExtendLease(cmd.Context(), req)
				if err != nil {
					return err
				}

				out := map[string]interface{}{
					"status":            "OK",
					"new_expires_at_ms": resp.NewExpiresAtMs,
				}
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(out)
			})
		},
	}
	extendCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	extendCmd.Flags().String("name", "", "WorkQueue name")
	extendCmd.Flags().StringP("group", "g", "default", "Consumer group")
	extendCmd.Flags().String("id", "", "Message ID (base64)")
	extendCmd.Flags().Int64("extension-ms", 30000, "Additional lease time in milliseconds")
	return extendCmd
}

// newWorkQueueStatsCommand constructs the `workqueue stats` subcommand.
func newWorkQueueStatsCommand() *cobra.Command {
	statsCmd := &cobra.Command{
		Use:   "stats",
		Short: "Get work queue statistics",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.GetWorkQueueStatsRequest{
					Namespace: ns,
					Name:      name,
					Group:     group,
				}
				resp, err := cli.GetWorkQueueStats(cmd.Context(), req)
				if err != nil {
					return err
				}

				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(resp)
			})
		},
	}
	statsCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	statsCmd.Flags().String("name", "", "WorkQueue name")
	statsCmd.Flags().StringP("group", "g", "", "Consumer group (optional, for group-specific stats)")
	return statsCmd
}

// newWorkQueueMessagePendingCommand constructs the `workqueue pending` subcommand.
func newWorkQueueMessagePendingCommand() *cobra.Command {
	pendingCmd := &cobra.Command{
		Use:   "pending",
		Short: "List pending (in-flight) messages",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")
			consumerID, _ := cmd.Flags().GetString("consumer-id")
			limit, _ := cmd.Flags().GetInt32("limit")

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.ListPendingRequest{
					Namespace:  ns,
					Name:       name,
					Group:      group,
					ConsumerId: consumerID,
					Limit:      limit,
				}
				resp, err := cli.ListPending(cmd.Context(), req)
				if err != nil {
					return err
				}

				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(resp)
			})
		},
	}
	pendingCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	pendingCmd.Flags().String("name", "", "WorkQueue name")
	pendingCmd.Flags().StringP("group", "g", "default", "Consumer group")
	pendingCmd.Flags().String("consumer-id", "", "Filter by consumer ID")
	pendingCmd.Flags().Int32("limit", 100, "Max items to return")
	return pendingCmd
}

// newWorkQueueConsumersCommand constructs the `workqueue consumers` subcommand.
func newWorkQueueConsumersCommand() *cobra.Command {
	consumersCmd := &cobra.Command{
		Use:   "consumers",
		Short: "List consumers in a group",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.ListConsumersRequest{
					Namespace: ns,
					Name:      name,
					Group:     group,
				}
				resp, err := cli.ListConsumers(cmd.Context(), req)
				if err != nil {
					return err
				}

				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(resp)
			})
		},
	}
	consumersCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	consumersCmd.Flags().String("name", "", "WorkQueue name")
	consumersCmd.Flags().StringP("group", "g", "default", "Consumer group")
	return consumersCmd
}

// newWorkQueueRegisterConsumerCommand constructs the `workqueue register-consumer` subcommand.
func newWorkQueueRegisterConsumerCommand() *cobra.Command {
	registerCmd := &cobra.Command{
		Use:   "register-consumer",
		Short: "Register a consumer with the registry",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")
			consumerID, _ := cmd.Flags().GetString("consumer-id")

			if consumerID == "" {
				consumerID = fmt.Sprintf("cli-%d", time.Now().Unix())
			}

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.RegisterConsumerRequest{
					Namespace:  ns,
					Name:       name,
					Group:      group,
					ConsumerId: consumerID,
				}
				if _, err := cli.RegisterConsumer(cmd.Context(), req); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "status: OK\nconsumer_id: %s\n", consumerID)
				return nil
			})
		},
	}
	registerCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	registerCmd.Flags().String("name", "", "WorkQueue name")
	registerCmd.Flags().StringP("group", "g", "default", "Consumer group")
	registerCmd.Flags().String("consumer-id", "", "Consumer ID (auto-generated if empty)")
	return registerCmd
}

// newWorkQueueHeartbeatCommand constructs the `workqueue heartbeat` subcommand.
func newWorkQueueHeartbeatCommand() *cobra.Command {
	heartbeatCmd := &cobra.Command{
		Use:   "heartbeat",
		Short: "Send a consumer heartbeat",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")
			consumerID, _ := cmd.Flags().GetString("consumer-id")

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.HeartbeatRequest{
					Namespace:  ns,
					Name:       name,
					Group:      group,
					ConsumerId: consumerID,
				}
				if _, err := cli.Heartbeat(cmd.Context(), req); err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "status:", "OK")
				return nil
			})
		},
	}
	heartbeatCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	heartbeatCmd.Flags().String("name", "", "WorkQueue name")
	heartbeatCmd.Flags().StringP("group", "g", "default", "Consumer group")
	heartbeatCmd.Flags().String("consumer-id", "", "Consumer ID")
	return heartbeatCmd
}

// newWorkQueueUnregisterConsumerCommand constructs the `workqueue unregister-consumer` subcommand.
func newWorkQueueUnregisterConsumerCommand() *cobra.Command {
	unregisterCmd := &cobra.Command{
		Use:   "unregister-consumer",
		Short: "Unregister a consumer from the registry",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")
			consumerID, _ := cmd.Flags().GetString("consumer-id")

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.UnregisterConsumerRequest{
					Namespace:  ns,
					Name:       name,
					Group:      group,
					ConsumerId: consumerID,
				}
				if _, err := cli.UnregisterConsumer(cmd.Context(), req); err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "status:", "OK")
				return nil
			})
		},
	}
	unregisterCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	unregisterCmd.Flags().String("name", "", "WorkQueue name")
	unregisterCmd.Flags().StringP("group", "g", "default", "Consumer group")
	unregisterCmd.Flags().String("consumer-id", "", "Consumer ID")
	return unregisterCmd
}

// newWorkQueueClaimCommand constructs the `workqueue claim` subcommand.
func newWorkQueueClaimCommand() *cobra.Command {
	claimCmd := &cobra.Command{
		Use:   "claim",
		Short: "Manually claim (reassign) specific messages to a different consumer",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")
			newConsumerID, _ := cmd.Flags().GetString("new-consumer-id")
			leaseMs, _ := cmd.Flags().GetInt64("lease-ms")
			idsB64, _ := cmd.Flags().GetStringArray("id")

			if len(idsB64) == 0 {
				return fmt.Errorf("at least one --id is required")
			}

			ids := make([][]byte, 0, len(idsB64))
			for _, idB64 := range idsB64 {
				id, err := base64.StdEncoding.DecodeString(idB64)
				if err != nil {
					return fmt.Errorf("invalid --id %s: %w", idB64, err)
				}
				ids = append(ids, id)
			}

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.ClaimRequest{
					Namespace:     ns,
					Name:          name,
					Group:         group,
					NewConsumerId: newConsumerID,
					Ids:           ids,
					LeaseMs:       leaseMs,
				}
				resp, err := cli.Claim(cmd.Context(), req)
				if err != nil {
					return err
				}

				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(resp)
			})
		},
	}
	claimCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	claimCmd.Flags().String("name", "", "WorkQueue name")
	claimCmd.Flags().StringP("group", "g", "default", "Consumer group")
	claimCmd.Flags().String("new-consumer-id", "", "New consumer ID to assign to")
	claimCmd.Flags().StringArray("id", []string{}, "Message IDs to claim (base64, repeat)")
	claimCmd.Flags().Int64("lease-ms", 30000, "New lease duration in milliseconds")
	return claimCmd
}

// newWorkQueueFlushCommand constructs the `workqueue flush` subcommand.
func newWorkQueueFlushCommand() *cobra.Command {
	flushCmd := &cobra.Command{
		Use:   "flush",
		Short: "Flush (clear) a work queue",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ns, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")

			return withWorkQueuesClient(cmd.Context(), func(cli flov1.WorkQueuesServiceClient) error {
				req := &flov1.FlushWorkQueueRequest{
					Namespace: ns,
					Name:      name,
					Group:     group,
				}
				if _, err := cli.Flush(cmd.Context(), req); err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "status:", "OK")
				return nil
			})
		},
	}
	flushCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	flushCmd.Flags().String("name", "", "WorkQueue name")
	flushCmd.Flags().StringP("group", "g", "", "Consumer group (optional, flush specific group only)")
	return flushCmd
}

// newWorkQueueMessageCompletedCommand constructs the `workqueue completed` subcommand.
func newWorkQueueMessageCompletedCommand() *cobra.Command {
	completedCmd := &cobra.Command{
		Use:   "completed",
		Short: "List recently completed messages",
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			group, _ := cmd.Flags().GetString("group")
			partition, _ := cmd.Flags().GetInt32("partition")
			limit, _ := cmd.Flags().GetInt32("limit")

			return withWorkQueuesClient(cmd.Context(), func(client flov1.WorkQueuesServiceClient) error {
				req := &flov1.ListCompletedRequest{
					Namespace: namespace,
					Name:      name,
					Group:     group,
					Partition: partition,
					Limit:     limit,
				}

				resp, err := client.ListCompleted(cmd.Context(), req)
				if err != nil {
					return fmt.Errorf("list completed: %w", err)
				}

				if len(resp.Entries) == 0 {
					fmt.Println("No completed messages found")
					return nil
				}

				// Print completed entries as JSON
				for _, entry := range resp.Entries {
					durationMs := entry.DurationMs
					durationSec := float64(durationMs) / 1000.0

					out := map[string]interface{}{
						"id_b64":          base64.StdEncoding.EncodeToString(entry.Id),
						"seq":             entry.Seq,
						"partition":       entry.Partition,
						"group":           entry.Group,
						"consumer_id":     entry.ConsumerId,
						"dequeued_at_ms":  entry.DequeuedAtMs,
						"completed_at_ms": entry.CompletedAtMs,
						"duration_sec":    fmt.Sprintf("%.3f", durationSec),
						"delivery_count":  entry.DeliveryCount,
					}
					if entry.EnqueuedAtMs > 0 {
						out["enqueued_at_ms"] = entry.EnqueuedAtMs
					}
					if entry.PayloadSize > 0 {
						out["payload_size"] = entry.PayloadSize
					}
					if len(entry.Headers) > 0 {
						out["headers"] = entry.Headers
					}

					data, _ := json.Marshal(out)
					fmt.Println(string(data))
				}

				return nil
			})
		},
	}
	completedCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	completedCmd.Flags().String("name", "", "WorkQueue name")
	completedCmd.Flags().StringP("group", "g", "", "Consumer group (optional filter)")
	completedCmd.Flags().Int32("partition", 0, "Partition to query")
	completedCmd.Flags().Int32("limit", 100, "Max entries to return")
	return completedCmd
}

// newWorkQueueMessagesReadyCommand constructs the `workqueue ready` subcommand.
func newWorkQueueMessagesReadyCommand() *cobra.Command {
	readyCmd := &cobra.Command{
		Use:     "ready",
		Aliases: []string{"queued", "waiting"},
		Short:   "List messages waiting in queue (ready to be dequeued)",
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			partition, _ := cmd.Flags().GetInt32("partition")
			limit, _ := cmd.Flags().GetInt32("limit")
			includePayload, _ := cmd.Flags().GetBool("include-payload")

			return withWorkQueuesClient(cmd.Context(), func(client flov1.WorkQueuesServiceClient) error {
				req := &flov1.ListReadyMessagesRequest{
					Namespace:      namespace,
					Name:           name,
					Partition:      partition,
					Limit:          limit,
					IncludePayload: includePayload,
				}

				resp, err := client.ListReadyMessages(cmd.Context(), req)
				if err != nil {
					return fmt.Errorf("list queued messages: %w", err)
				}

				if len(resp.Messages) == 0 {
					fmt.Println("No messages in queue")
					return nil
				}

				// Print messages as JSON
				for _, msg := range resp.Messages {
					out := map[string]interface{}{
						"id_b64":    base64.StdEncoding.EncodeToString(msg.Id),
						"seq":       msg.Seq,
						"partition": msg.Partition,
						"priority":  msg.Priority,
					}

					if msg.EnqueuedAtMs > 0 {
						out["enqueued_at_ms"] = msg.EnqueuedAtMs
					}
					if msg.DelayUntilMs > 0 {
						out["delay_until_ms"] = msg.DelayUntilMs
					}
					if len(msg.Headers) > 0 {
						out["headers"] = msg.Headers
					}
					if msg.PayloadSize > 0 {
						out["payload_size"] = msg.PayloadSize
					}
					if includePayload && len(msg.Payload) > 0 {
						// Try to parse as JSON
						var payloadJSON interface{}
						if err := json.Unmarshal(msg.Payload, &payloadJSON); err == nil {
							out["payload_json"] = payloadJSON
						} else {
							out["payload_b64"] = base64.StdEncoding.EncodeToString(msg.Payload)
						}
					}

					data, _ := json.Marshal(out)
					fmt.Println(string(data))
				}

				return nil
			})
		},
	}
	readyCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	readyCmd.Flags().String("name", "", "WorkQueue name")
	readyCmd.Flags().Int32("partition", 0, "Partition to query")
	readyCmd.Flags().Int32("limit", 100, "Max entries to return")
	readyCmd.Flags().Bool("include-payload", false, "Include message payload in output")
	return readyCmd
}

// newWorkQueueMessagesCommand constructs the unified `workqueue list` subcommand.
func newWorkQueueMessagesCommand() *cobra.Command {
	listCmd := &cobra.Command{
		Use:   "messages",
		Short: "List messages across multiple states (unified view)",
		Long: `List messages from one or more states: ready, pending, completed, dlq.

State Lifecycle:
  Ready → [Dequeue] → Pending → [Complete] → Completed
                         ↓ (fail after retries)
                        DLQ

Examples:
  # Show counts across all states
  flo wq messages --name orders --all

  # Show specific states
  flo wq messages --name orders --state ready,pending
  
  # Power user: detailed view
  flo wq messages --name orders --state all --include-payload`,
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			name, _ := cmd.Flags().GetString("name")
			state, _ := cmd.Flags().GetString("state")
			showAll, _ := cmd.Flags().GetBool("all")
			limit, _ := cmd.Flags().GetInt32("limit")
			partition, _ := cmd.Flags().GetInt32("partition")
			group, _ := cmd.Flags().GetString("group")

			if group == "" {
				group = "default"
			}

			if name == "" {
				return fmt.Errorf("--name is required")
			}

			return withWorkQueuesClient(cmd.Context(), func(client flov1.WorkQueuesServiceClient) error {
				// If --all, show counts for each state
				if showAll || state == "all" {
					fmt.Printf("WorkQueue: %s (namespace: %s)\n", name, namespace)
					fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
					fmt.Println()

					// Ready
					readyReq := &flov1.ListReadyMessagesRequest{
						Namespace:      namespace,
						Name:           name,
						Partition:      partition,
						Limit:          limit,
						IncludePayload: false,
					}
					readyResp, err := client.ListReadyMessages(cmd.Context(), readyReq)
					readyCount := 0
					if err == nil && readyResp != nil {
						readyCount = len(readyResp.Messages)
					}

					// Pending
					pendingReq := &flov1.ListPendingRequest{
						Namespace: namespace,
						Name:      name,
						Group:     group,
						Limit:     limit,
					}
					pendingResp, err := client.ListPending(cmd.Context(), pendingReq)
					pendingCount := 0
					if err == nil && pendingResp != nil {
						pendingCount = len(pendingResp.Entries)
					}

					// Completed
					completedReq := &flov1.ListCompletedRequest{
						Namespace: namespace,
						Name:      name,
						Group:     group,
						Partition: partition,
						Limit:     limit,
					}
					completedResp, err := client.ListCompleted(cmd.Context(), completedReq)
					completedCount := 0
					if err == nil && completedResp != nil {
						completedCount = len(completedResp.Entries)
					}

					// DLQ (TODO: implement when DLQ is ready)
					dlqCount := 0

					// Print summary
					fmt.Printf("  🟢 Ready:     %d messages (waiting to be dequeued)\n", readyCount)
					fmt.Printf("  🟡 Pending:   %d messages (being processed)\n", pendingCount)
					fmt.Printf("  🔵 Completed: %d messages (recently finished)\n", completedCount)
					fmt.Printf("  🔴 DLQ:       %d messages (failed)\n", dlqCount)
					fmt.Println()

					total := readyCount + pendingCount + completedCount + dlqCount
					fmt.Printf("Total visible: %d messages\n", total)
					fmt.Println()
					fmt.Println("Lifecycle: Ready → Pending → Completed (or DLQ if failed)")

					return nil
				}

				// Show specific state(s)
				states := []string{"ready"}
				if state != "" {
					states = strings.Split(state, ",")
				}

				for _, s := range states {
					s = strings.TrimSpace(s)
					fmt.Printf("State: %s\n", s)
					fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

					switch s {
					case "ready", "queued", "waiting":
						req := &flov1.ListReadyMessagesRequest{
							Namespace:      namespace,
							Name:           name,
							Partition:      partition,
							Limit:          limit,
							IncludePayload: true,
						}
						resp, err := client.ListReadyMessages(cmd.Context(), req)
						if err != nil {
							return fmt.Errorf("list ready: %w", err)
						}
						if len(resp.Messages) == 0 {
							fmt.Println("No ready messages")
						} else {
							for _, msg := range resp.Messages {
								out := map[string]interface{}{
									"id_b64":    base64.StdEncoding.EncodeToString(msg.Id),
									"seq":       msg.Seq,
									"partition": msg.Partition,
									"priority":  msg.Priority,
								}
								if len(msg.Payload) > 0 {
									var payloadJSON interface{}
									if err := json.Unmarshal(msg.Payload, &payloadJSON); err == nil {
										out["payload_json"] = payloadJSON
									}
								}
								data, _ := json.Marshal(out)
								fmt.Println(string(data))
							}
						}

					case "pending", "inflight", "in-flight":
						req := &flov1.ListPendingRequest{
							Namespace: namespace,
							Name:      name,
							Group:     group,
							Limit:     limit,
						}
						resp, err := client.ListPending(cmd.Context(), req)
						if err != nil {
							return fmt.Errorf("list pending: %w", err)
						}
						if len(resp.Entries) == 0 {
							fmt.Println("No pending messages")
						} else {
							data, _ := json.MarshalIndent(resp, "", "  ")
							fmt.Println(string(data))
						}

					case "completed", "done":
						req := &flov1.ListCompletedRequest{
							Namespace: namespace,
							Name:      name,
							Group:     group,
							Partition: partition,
							Limit:     limit,
						}
						resp, err := client.ListCompleted(cmd.Context(), req)
						if err != nil {
							return fmt.Errorf("list completed: %w", err)
						}
						if len(resp.Entries) == 0 {
							fmt.Println("No completed messages")
						} else {
							for _, entry := range resp.Entries {
								out := map[string]interface{}{
									"id_b64":          base64.StdEncoding.EncodeToString(entry.Id),
									"seq":             entry.Seq,
									"consumer_id":     entry.ConsumerId,
									"completed_at_ms": entry.CompletedAtMs,
									"duration_sec":    float64(entry.DurationMs) / 1000.0,
								}
								data, _ := json.Marshal(out)
								fmt.Println(string(data))
							}
						}

					case "dlq", "dead-letter":
						fmt.Println("DLQ listing not yet implemented")

					default:
						return fmt.Errorf("unknown state: %s (valid: ready, pending, completed, dlq)", s)
					}

					fmt.Println()
				}

				return nil
			})
		},
		Aliases: []string{"list", "ls"},
	}
	listCmd.Flags().StringP("namespace", "n", "default", "Namespace")
	listCmd.Flags().String("name", "", "WorkQueue name (required)")
	listCmd.Flags().String("state", "", "State(s) to show: ready, pending, completed, dlq (comma-separated)")
	listCmd.Flags().Bool("all", false, "Show counts for all states")
	listCmd.Flags().Int32("partition", 0, "Partition to query")
	listCmd.Flags().Int32("limit", 100, "Max entries per state")
	listCmd.Flags().StringP("group", "g", "", "Consumer group filter (for pending/completed)")
	return listCmd
}
