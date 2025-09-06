/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grid

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

func IsOperative(ctx context.Context, gridClient *GridClient) (bool, error) {
	log := log.FromContext(ctx).WithValues("func", "IsOperative")
	log.V(1).Info("Checking if grid is operative")

	// Check if the Grid is healthy and available.
	health, err := gridClient.Health().Get(ctx)
	if err != nil {
		log.Error(err, "Failed to get grid health")
		return false, err
	}

	return health.Operative(), nil
}

func GetOperativeReason(ctx context.Context, gridClient *GridClient) ([]string, error) {
	log := log.FromContext(ctx).WithValues("func", "GetHealthIssues")
	log.V(1).Info("Fetching grid health issues")

	returnedIssues := make([]string, 0)

	// Get the health issues from the Grid.
	health, err := gridClient.Health().Get(ctx)
	if err != nil {
		log.Error(err, "Failed to get grid health")
		return returnedIssues, err
	}

	// check if there are major alerts.
	if health.Alerts != nil {
		log.V(1).Info("Grid health issues found", "issues", health.Alerts)
		if health.Alerts.Major != nil {
			returnedIssues = append(returnedIssues, fmt.Sprintf("Too many major alerts present: %d", *health.Alerts.Major))
		} else {
			log.V(1).Info("No major alerts present")
		}
	}

	if health.Nodes != nil {
		notConnected := 0
		if health.Nodes.AdministrativelyDown != nil {
			notConnected += *health.Nodes.AdministrativelyDown
		}

		if health.Nodes.Unknown != nil {
			notConnected += *health.Nodes.Unknown
		}
		if notConnected > 0 {
			log.V(1).Info("Grid health issues found", "notConnected", notConnected)
			returnedIssues = append(returnedIssues, fmt.Sprintf("Too many nodes not connected: %d", notConnected))
		}
	} else {
		log.V(1).Info("No node health issues found")
	}

	return returnedIssues, nil
}
