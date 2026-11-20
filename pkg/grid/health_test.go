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
	"testing"

	"github.com/bedag/storagegrid-sdk-go/models"
)

func intPtr(v int) *int { return &v }

// Alerts must never make the grid non-operative: a grid with active alerts used to flip every
// dependent S3Tenant to not ready, although tenant management was still possible.
func TestNewHealthReport(t *testing.T) {
	cases := []struct {
		name          string
		health        *models.Health
		maxUnavail    int
		wantOperative bool
		wantAlerts    bool
	}{
		{"nil health", nil, 1, true, false},
		{"all green", &models.Health{}, 1, true, false},
		{
			"major and critical alerts stay operative",
			&models.Health{Alerts: &models.Alerts{Critical: intPtr(1), Major: intPtr(2), Minor: intPtr(3)}},
			1, true, true,
		},
		{
			"zero alerts are not alerts",
			&models.Health{Alerts: &models.Alerts{Critical: intPtr(0), Major: intPtr(0), Minor: intPtr(0)}},
			1, true, false,
		},
		{
			"unavailable nodes within limit",
			&models.Health{Nodes: &models.Nodes{AdministrativelyDown: intPtr(1)}},
			1, true, false,
		},
		{
			"unavailable nodes above limit",
			&models.Health{Nodes: &models.Nodes{AdministrativelyDown: intPtr(1), Unknown: intPtr(1)}},
			1, false, false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report := newHealthReport(tc.health, tc.maxUnavail)
			if got := report.Operative(); got != tc.wantOperative {
				t.Errorf("Operative() = %v, want %v (issues: %v)", got, tc.wantOperative, report.NodeIssues)
			}
			if got := report.HasAlerts(); got != tc.wantAlerts {
				t.Errorf("HasAlerts() = %v, want %v", got, tc.wantAlerts)
			}
		})
	}

	report := newHealthReport(&models.Health{Alerts: &models.Alerts{Critical: intPtr(1), Major: intPtr(2)}}, 1)
	if got, want := report.AlertSummary(), "critical: 1, major: 2, minor: 0"; got != want {
		t.Errorf("AlertSummary() = %q, want %q", got, want)
	}
}
