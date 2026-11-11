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
	"sync/atomic"
	"testing"
	"time"

	"github.com/bedag/storagegrid-sdk-go/models"
	"github.com/bedag/storagegrid-sdk-go/services"
	sgtesting "github.com/bedag/storagegrid-sdk-go/testing"
)

type fakeSource struct {
	tenant  *sgtesting.MockTenantService
	metrics *sgtesting.MockMetricsService
}

func (f fakeSource) Tenant() services.TenantServiceInterface   { return f.tenant }
func (f fakeSource) Metrics() services.MetricsServiceInterface { return f.metrics }

func ptr[T any](v T) *T { return &v }

func TestListTenantUsageMapsQuota(t *testing.T) {
	api := newAPI(fakeSource{tenant: &sgtesting.MockTenantService{
		ListUsageFunc: func(context.Context) (*[]models.TenantUsageEntry, error) {
			return &[]models.TenantUsageEntry{
				{Id: "a", ObjectCount: ptr(int64(3)), DataBytes: ptr(int64(10)),
					Policy: &models.TenantPolicy{QuotaObjectBytes: ptr(int64(100))}},
				{Id: "b", ObjectCount: ptr(int64(1)), DataBytes: ptr(int64(2))},
			}, nil
		},
	}})
	got, err := api.ListTenantUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got["a"].QuotaBytes == nil || *got["a"].QuotaBytes != 100 || got["a"].Bytes != 10 || got["a"].Objects != 3 {
		t.Errorf("unexpected a: %+v", got["a"])
	}
	if got["b"].QuotaBytes != nil {
		t.Errorf("quota should stay nil, got %v", *got["b"].QuotaBytes)
	}
}

func TestTenantUsageWithBucketsConcurrencyCap(t *testing.T) {
	var cur, peak atomic.Int32
	api := newAPI(fakeSource{tenant: &sgtesting.MockTenantService{
		GetUsageWithBucketsFunc: func(_ context.Context, id string) (*models.TenantUsage, error) {
			n := cur.Add(1)
			for {
				m := peak.Load()
				if n <= m || peak.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			cur.Add(-1)
			return &models.TenantUsage{
				DataBytes: ptr(int64(5)),
				Buckets:   []*models.BucketStats{{Name: ptr("bkt"), ObjectCount: ptr(2), DataBytes: ptr(int64(4))}},
			}, nil
		},
	}})
	got, err := api.TenantUsageWithBuckets(context.Background(), []string{"1", "2", "3", "4", "5"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 || got["3"].Buckets["bkt"] != (BucketUsageStats{Bytes: 4, Objects: 2}) {
		t.Errorf("unexpected result: %+v", got)
	}
	if peak.Load() > 2 {
		t.Errorf("max concurrency %d > 2", peak.Load())
	}
}

func TestEndpointHealthBound(t *testing.T) {
	sample := func(id, v string) models.MetricSample {
		return models.MetricSample{Metric: map[string]string{"endpoint_id": id}, Value: [2]any{1.0, v}}
	}
	api := newAPI(fakeSource{metrics: &sgtesting.MockMetricsService{
		QueryFunc: func(_ context.Context, q string) (*models.MetricQueryResult, error) {
			if q == "min by (endpoint_id) (storagegrid_load_balancer_endpoint_bound)" {
				return &models.MetricQueryResult{Result: []models.MetricSample{sample("e1", "0"), sample("e2", "1")}}, nil
			}
			return &models.MetricQueryResult{Result: []models.MetricSample{sample("e2", "1900000000")}}, nil
		},
	}})
	got, err := api.EndpointHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got["e1"].Bound || got["e1"].CertExpiry != nil {
		t.Errorf("e1: %+v", got["e1"])
	}
	if !got["e2"].Bound || got["e2"].CertExpiry == nil || got["e2"].CertExpiry.Unix() != 1900000000 {
		t.Errorf("e2: %+v", got["e2"])
	}
}
