package grid

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// returns a list of available regions
func GetRegions(ctx context.Context, gridClient *GridClient) (*[]string, error) {
	log := log.FromContext(ctx).WithValues("func", "GetRegions")
	log.V(1).Info("Fetching regions")

	return gridClient.Region.List(ctx)
}
