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
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bedag/storagegrid-sdk-go/services"
	"golang.org/x/sync/errgroup"
)

// TenantUsageStats is the usage of one StorageGrid tenant.
// It is named differently from TenantUsage, which aliases the SDK model.
type TenantUsageStats struct {
	Bytes, Objects int64
	// QuotaBytes is nil when the tenant has no quota.
	QuotaBytes *int64
	// Buckets is only filled by TenantUsageWithBuckets.
	Buckets map[string]BucketUsageStats
}

// BucketUsageStats is the usage of one bucket.
type BucketUsageStats struct {
	Bytes, Objects int64
}

// EndpointHealth is the health of one load balancer endpoint.
type EndpointHealth struct {
	Bound bool
	// CertExpiry is nil when the grid reports no expiry for the endpoint.
	CertExpiry *time.Time
}

// Sample is one instant-query result.
type Sample struct {
	Labels map[string]string
	Value  float64
}

// API reads usage and health data from the grid for metrics.
type API interface {
	ListTenantUsage(ctx context.Context) (map[string]TenantUsageStats, error)
	// TenantUsageWithBuckets skips tenants the grid no longer has, and returns the
	// usage it got together with the joined errors of the tenants that failed.
	TenantUsageWithBuckets(ctx context.Context, ids []string, concurrency int) (map[string]TenantUsageStats, error)
	EndpointHealth(ctx context.Context) (map[string]EndpointHealth, error)
	Query(ctx context.Context, promql string) ([]Sample, error)
}

// source is the part of GridClient the API needs, so tests can inject mocks.
type source interface {
	Tenant() services.TenantServiceInterface
	Metrics() services.MetricsServiceInterface
}

type api struct{ src source }

// NewAPI returns an API backed by the given grid client.
func NewAPI(gc *GridClient) API { return newAPI(gc) }

func newAPI(src source) API { return &api{src: src} }

func deref[T int | int64](p *T) int64 {
	if p == nil {
		return 0
	}
	return int64(*p)
}

func (a *api) ListTenantUsage(ctx context.Context) (map[string]TenantUsageStats, error) {
	entries, err := a.src.Tenant().ListUsage(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]TenantUsageStats, len(*entries))
	for _, e := range *entries {
		u := TenantUsageStats{Bytes: deref(e.DataBytes), Objects: deref(e.ObjectCount)}
		if e.Policy != nil {
			u.QuotaBytes = e.Policy.QuotaObjectBytes
		}
		out[e.Id] = u
	}
	return out, nil
}

func (a *api) TenantUsageWithBuckets(ctx context.Context, ids []string, concurrency int) (map[string]TenantUsageStats, error) {
	out := make(map[string]TenantUsageStats, len(ids))
	var errs []error
	var mu sync.Mutex
	// A plain group, so one tenant's failure does not cancel the others.
	var g errgroup.Group
	g.SetLimit(max(concurrency, 1))
	for _, id := range ids {
		g.Go(func() error {
			usage, err := a.src.Tenant().GetUsageWithBuckets(ctx, id)
			// An account can keep its tenant ID after the grid deleted the tenant.
			if hasStatus(err, http.StatusNotFound) {
				return nil
			}
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("tenant %s usage: %w", id, err))
				mu.Unlock()
				return nil
			}
			u := TenantUsageStats{
				Bytes:   deref(usage.DataBytes),
				Objects: deref(usage.ObjectCount),
				Buckets: make(map[string]BucketUsageStats, len(usage.Buckets)),
			}
			for _, b := range usage.Buckets {
				if b != nil && b.Name != nil {
					u.Buckets[*b.Name] = BucketUsageStats{Bytes: deref(b.DataBytes), Objects: deref(b.ObjectCount)}
				}
			}
			mu.Lock()
			out[id] = u
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait() // Goroutines report through errs.
	return out, errors.Join(errs...)
}

func (a *api) EndpointHealth(ctx context.Context) (map[string]EndpointHealth, error) {
	bound, err := a.Query(ctx, "min by (endpoint_id) (storagegrid_load_balancer_endpoint_bound)")
	if err != nil {
		return nil, err
	}
	expiry, err := a.Query(ctx, "min by (endpoint_id) (storagegrid_load_balancer_endpoint_cert_expiry_time)")
	if err != nil {
		return nil, err
	}
	out := make(map[string]EndpointHealth, len(bound))
	for _, s := range bound {
		out[s.Labels["endpoint_id"]] = EndpointHealth{Bound: s.Value != 0}
	}
	for _, s := range expiry {
		id := s.Labels["endpoint_id"]
		h, ok := out[id]
		if !ok {
			continue
		}
		t := time.Unix(int64(s.Value), 0)
		h.CertExpiry = &t
		out[id] = h
	}
	return out, nil
}

func (a *api) Query(ctx context.Context, promql string) ([]Sample, error) {
	res, err := a.src.Metrics().Query(ctx, promql)
	if err != nil {
		return nil, err
	}
	out := make([]Sample, 0, len(res.Result))
	for _, s := range res.Result {
		v, err := s.Float()
		if err != nil {
			return nil, err
		}
		out = append(out, Sample{Labels: s.Metric, Value: v})
	}
	return out, nil
}
