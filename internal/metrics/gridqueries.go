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

package metrics

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	ctrl "sigs.k8s.io/controller-runtime"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	"github.com/bedag/storagegrid-operator/pkg/grid"
)

// GridQueryErrors counts grid queries that failed or were skipped. The caller registers it.
var GridQueryErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: metricPrefix + "grid_query_errors_total",
	Help: "Opt-in grid queries that failed or were skipped.",
}, []string{"storagegrid", "query"})

func bySite(metric string) s3v1alpha1.GridMetricQuery {
	return s3v1alpha1.GridMetricQuery{Name: metric, Query: "sum by (site_name) (" + metric + ")"}
}

// DefaultGridQueries are used when spec.metrics.grid.queries is empty.
var DefaultGridQueries = []s3v1alpha1.GridMetricQuery{
	bySite("storagegrid_storage_utilization_data_bytes"),
	bySite("storagegrid_storage_utilization_usable_space_bytes"),
	bySite("storagegrid_storage_utilization_total_space_bytes"),
	bySite("storagegrid_storage_utilization_metadata_bytes"),
	bySite("storagegrid_storage_utilization_metadata_allowed_bytes"),
	{Name: "storagegrid_alerts_firing", Query: `count by (severity) (ALERTS{alertstate="firing"})`},
	{Name: "storagegrid_s3_requests_rate", Query: "sum by (operation, code) (rate(storagegrid_s3_requests_total[5m]))"},
}

// forwardQuery turns samples into gauges named name, keeping only the allowed labels
// and adding storagegrid. Samples that collide after filtering skip the whole query.
func forwardQuery(name string, samples []grid.Sample, allowed []string, sg string) ([]prometheus.Metric, error) {
	names := slices.DeleteFunc(slices.Sorted(slices.Values(allowed)), func(l string) bool { return l == "storagegrid" })
	names = slices.Compact(names)
	desc := prometheus.NewDesc(name, "Forwarded StorageGrid metric.", append(slices.Clone(names), "storagegrid"), nil)

	seen := map[string]bool{}
	out := make([]prometheus.Metric, 0, len(samples))
	for _, s := range samples {
		vals := make([]string, 0, len(names)+1)
		for _, l := range names {
			vals = append(vals, s.Labels[l]) // Missing labels become "", which Prometheus treats as absent.
		}
		key := strings.Join(vals, "\xff")
		if seen[key] {
			GridQueryErrors.WithLabelValues(sg, name).Inc()
			return nil, fmt.Errorf("query %q: results collide after dropping labels, aggregate or allow more labels", name)
		}
		seen[key] = true
		m, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, s.Value, append(vals, sg)...)
		if err != nil {
			GridQueryErrors.WithLabelValues(sg, name).Inc()
			return nil, fmt.Errorf("query %q: %w", name, err)
		}
		out = append(out, m)
	}
	return out, nil
}

// gridQueries runs the configured queries and returns the forwarded series. A failing or
// colliding query is skipped and counted; it returns an error only when every query failed.
func gridQueries(ctx context.Context, sg *s3v1alpha1.StorageGrid, api grid.API) ([]prometheus.Metric, error) {
	cfg := sg.Spec.Metrics.Grid
	queries := cfg.Queries
	if len(queries) == 0 {
		queries = DefaultGridQueries
	}
	var out []prometheus.Metric
	var lastErr error
	failed, used := 0, map[string]bool{}
	for _, q := range queries {
		ms, err := runQuery(ctx, api, q, cfg.AllowedLabels, sg.Name, used)
		if err != nil {
			failed++
			lastErr = err
			if grid.IsUnauthorized(err) {
				return nil, err
			}
			ctrl.Log.WithName("grid-metrics").V(1).Info("grid query skipped", "storagegrid", sg.Name, "query", q.Name, "err", err.Error())
			continue
		}
		used[q.Name] = true
		out = append(out, ms...)
	}
	if failed == len(queries) {
		return nil, lastErr
	}
	return out, nil
}

func runQuery(ctx context.Context, api grid.API, q s3v1alpha1.GridMetricQuery, allowed []string, sg string, used map[string]bool) ([]prometheus.Metric, error) {
	if used[q.Name] {
		GridQueryErrors.WithLabelValues(sg, q.Name).Inc()
		return nil, fmt.Errorf("query name %q is used twice", q.Name)
	}
	samples, err := api.Query(ctx, q.Query)
	if err != nil {
		GridQueryErrors.WithLabelValues(sg, q.Name).Inc()
		return nil, err
	}
	return forwardQuery(q.Name, samples, allowed, sg) // Counts its own errors.
}
