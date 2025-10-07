package client

import (
	"github.com/spf13/cobra"
)

// NewRoot constructs a root Cobra command for the Flo client.
// It registers the stream and workqueue command groups.
func NewRoot(baseURL BaseURLFunc) *cobra.Command {
	root := &cobra.Command{
		Use:   "flo",
		Short: "Flo client commands",
	}
	root.AddCommand(NewStreamCommand(baseURL))
	root.AddCommand(NewWorkQueueCommand())
	return root
}
