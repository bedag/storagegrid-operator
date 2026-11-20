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

	"github.com/bedag/storagegrid-sdk-go/models"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// HealthReport summarizes the grid health endpoint.
//
// Only node connectivity decides whether the grid is operative. Alert counts are purely
// informational: they are surfaced to users but must never block operations, because a grid
// with active alerts (e.g. a pending update or a full disk on one node) is usually still
// perfectly usable for tenant and bucket management.
type HealthReport struct {
	CriticalAlerts int
	MajorAlerts    int
	MinorAlerts    int

	// NodeIssues lists the reasons why the grid is not operative. Empty means operative.
	NodeIssues []string
}

// Operative reports whether the grid is usable, i.e. node connectivity is within limits.
func (h *HealthReport) Operative() bool {
	return len(h.NodeIssues) == 0
}

// HasAlerts reports whether any alert of any severity is active on the grid.
func (h *HealthReport) HasAlerts() bool {
	return h.CriticalAlerts+h.MajorAlerts+h.MinorAlerts > 0
}

// AlertSummary returns a human-readable summary of the active alert counts.
func (h *HealthReport) AlertSummary() string {
	return fmt.Sprintf("critical: %d, major: %d, minor: %d", h.CriticalAlerts, h.MajorAlerts, h.MinorAlerts)
}

// GetHealthReport fetches the grid health and evaluates it against maxUnavailableNodes.
func GetHealthReport(ctx context.Context, gridClient *GridClient, maxUnavailableNodes int) (*HealthReport, error) {
	log := log.FromContext(ctx).WithValues("func", "GetHealthReport")
	log.V(1).Info("Fetching grid health")

	health, err := gridClient.Health().Get(ctx)
	if err != nil {
		log.Error(err, "Failed to get grid health")
		return nil, err
	}

	return newHealthReport(health, maxUnavailableNodes), nil
}

func newHealthReport(health *models.Health, maxUnavailableNodes int) *HealthReport {
	report := &HealthReport{NodeIssues: make([]string, 0)}
	if health == nil {
		return report
	}

	if health.Alerts != nil {
		report.CriticalAlerts = derefInt(health.Alerts.Critical)
		report.MajorAlerts = derefInt(health.Alerts.Major)
		report.MinorAlerts = derefInt(health.Alerts.Minor)
	}

	if health.Nodes != nil {
		notConnected := derefInt(health.Nodes.AdministrativelyDown) + derefInt(health.Nodes.Unknown)
		if notConnected > maxUnavailableNodes {
			report.NodeIssues = append(report.NodeIssues,
				fmt.Sprintf("Too many nodes not connected: %d (max unavailable: %d)", notConnected, maxUnavailableNodes))
		}
	}

	return report
}

func derefInt(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}
