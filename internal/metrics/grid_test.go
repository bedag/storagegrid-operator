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
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	"github.com/bedag/storagegrid-operator/pkg/grid"
)

const flatcarBucket = "flatcar-image-store-dev-2e0af0b5"

type fakeAPI struct {
	usage       map[string]grid.TenantUsageStats
	err         error
	block       bool
	listCalls   atomic.Int32
	bucketCalls atomic.Int32
	mu          sync.Mutex
	bucketIDs   []string
}

func newFakeAPI() *fakeAPI {
	quota := int64(10 << 30)
	return &fakeAPI{usage: map[string]grid.TenantUsageStats{
		"7173": {Bytes: 1569128713, Objects: 42, QuotaBytes: &quota, Buckets: map[string]grid.BucketUsageStats{
			flatcarBucket: {Bytes: 1000, Objects: 3},
		}},
		"6815": {Bytes: 5, Objects: 1},
		"7174": {Bytes: 7, Objects: 2},
	}}
}

func (f *fakeAPI) ListTenantUsage(ctx context.Context) (map[string]grid.TenantUsageStats, error) {
	f.listCalls.Add(1)
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.usage, f.err
}

func (f *fakeAPI) TenantUsageWithBuckets(_ context.Context, ids []string, _ int) (map[string]grid.TenantUsageStats, error) {
	f.bucketCalls.Add(1)
	f.mu.Lock()
	f.bucketIDs = ids
	f.mu.Unlock()
	return f.usage, f.err
}

func (f *fakeAPI) EndpointHealth(context.Context) (map[string]grid.EndpointHealth, error) {
	if f.err != nil {
		return nil, f.err
	}
	expiry := time.Unix(1800000000, 0)
	return map[string]grid.EndpointHealth{"c47f902c": {Bound: true, CertExpiry: &expiry}}, nil
}

func (f *fakeAPI) Query(context.Context, string) ([]grid.Sample, error) { return nil, nil }

func storageGrid(name string, bucketUsage bool) *s3v1alpha1.StorageGrid {
	return &s3v1alpha1.StorageGrid{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       s3v1alpha1.StorageGridSpec{Metrics: &s3v1alpha1.StorageGridMetrics{BucketUsage: bucketUsage}},
	}
}

func account(name, sg, tenantID string, bound *corev1.ObjectReference) *s3v1alpha1.S3TenantAccount {
	return &s3v1alpha1.S3TenantAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: s3v1alpha1.S3TenantAccountSpec{CommonTenantSpec: s3v1alpha1.CommonTenantSpec{
			StorageGridRef: corev1.LocalObjectReference{Name: sg},
		}},
		Status: s3v1alpha1.S3TenantAccountStatus{
			CommonTenantStatus: s3v1alpha1.CommonTenantStatus{TenantID: tenantID},
			S3TenantRef:        bound,
		},
	}
}

func bucket(name, bucketName string) *s3v1alpha1.S3Bucket {
	return &s3v1alpha1.S3Bucket{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: name},
		Spec:       s3v1alpha1.S3BucketSpec{S3TenantRef: corev1.ObjectReference{Name: "t1"}},
		Status:     s3v1alpha1.S3BucketStatus{BucketName: bucketName},
	}
}

// gridObjects is one grid with a bound account (7173), an unbound account (7174),
// a bucket in the bound tenant and a tenant class.
func gridObjects(bucketUsage bool) []client.Object {
	return []client.Object{
		storageGrid("primary-grid", bucketUsage),
		account("acc-1", "primary-grid", "7173", &corev1.ObjectReference{Namespace: "team-a", Name: "t1"}),
		account("acc-2", "primary-grid", "7174", nil),
		&s3v1alpha1.S3Tenant{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "t1"},
			Status: s3v1alpha1.S3TenantStatus{
				CommonTenantStatus: s3v1alpha1.CommonTenantStatus{TenantID: "7173"},
				S3TenantAccountRef: &corev1.ObjectReference{Name: "acc-1"},
			},
		},
		bucket("b1", flatcarBucket),
		&s3v1alpha1.S3TenantClass{
			ObjectMeta: metav1.ObjectMeta{Name: "shcofe"},
			Spec:       s3v1alpha1.S3TenantClassSpec{BackingID: "c47f902c", StorageGridRef: corev1.LocalObjectReference{Name: "primary-grid"}},
		},
	}
}

func fakeReader(t *testing.T, objs ...client.Object) client.Reader {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := s3v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func staticAPI(a grid.API) APIProvider {
	return func(context.Context, *s3v1alpha1.StorageGrid) (grid.API, error) { return a, nil }
}

var defaultOpts = GridCollectorOptions{Timeout: time.Second, BucketConcurrency: 8}

// series gathers c and renders each sample as name{sorted labels} value.
func series(t *testing.T, c prometheus.Collector) []string {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	var out []string
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			var labels []string
			for _, l := range m.GetLabel() {
				labels = append(labels, l.GetName()+`="`+l.GetValue()+`"`)
			}
			v := strconv.FormatFloat(m.GetGauge().GetValue(), 'f', -1, 64)
			out = append(out, mf.GetName()+"{"+strings.Join(labels, ",")+"} "+v)
		}
	}
	return out
}

func mustHave(t *testing.T, got []string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !slices.Contains(got, w) {
			t.Errorf("missing series %s\ngot:\n%s", w, strings.Join(got, "\n"))
		}
	}
}

func mustNotContain(t *testing.T, got []string, substr string) {
	t.Helper()
	for _, s := range got {
		if strings.Contains(s, substr) {
			t.Errorf("unexpected series %s", s)
		}
	}
}

const p = "storagegrid_operator_"

func TestForeignTenantsDropped(t *testing.T) {
	got := series(t, NewGridCollector(fakeReader(t, gridObjects(false)...), staticAPI(newFakeAPI()), func(string) {}, nil, defaultOpts))
	mustHave(t, got, p+`s3tenantaccount_usage_bytes{s3tenantaccount="acc-1",storagegrid="primary-grid",tenant_id="7173"} 1569128713`)
	mustNotContain(t, got, "6815")
}

func TestBoundAndUnboundAccount(t *testing.T) {
	got := series(t, NewGridCollector(fakeReader(t, gridObjects(false)...), staticAPI(newFakeAPI()), func(string) {}, nil, defaultOpts))
	mustHave(t, got,
		p+`s3tenant_usage_bytes{namespace="team-a",s3tenant="t1",s3tenantaccount="acc-1",storagegrid="primary-grid",tenant_id="7173"} 1569128713`,
		p+`s3tenant_usage_objects{namespace="team-a",s3tenant="t1",s3tenantaccount="acc-1",storagegrid="primary-grid",tenant_id="7173"} 42`,
		p+`s3tenant_quota_bytes{namespace="team-a",s3tenant="t1",s3tenantaccount="acc-1",storagegrid="primary-grid",tenant_id="7173"} 10737418240`,
		p+`s3tenantaccount_usage_bytes{s3tenantaccount="acc-1",storagegrid="primary-grid",tenant_id="7173"} 1569128713`,
		p+`s3tenantaccount_quota_bytes{s3tenantaccount="acc-1",storagegrid="primary-grid",tenant_id="7173"} 10737418240`,
		p+`s3tenantaccount_usage_bytes{s3tenantaccount="acc-2",storagegrid="primary-grid",tenant_id="7174"} 7`,
		p+`s3tenantaccount_usage_objects{s3tenantaccount="acc-2",storagegrid="primary-grid",tenant_id="7174"} 2`,
		p+`grid_scrape_success{step="tenant_usage",storagegrid="primary-grid"} 1`,
	)
	// The unbound account has no quota and no s3tenant series.
	mustNotContain(t, got, `s3tenant_usage_bytes{namespace="",`)
	mustNotContain(t, got, `s3tenantaccount_quota_bytes{s3tenantaccount="acc-2"`)
	for _, s := range got {
		if strings.HasPrefix(s, p+"s3tenant_") && strings.Contains(s, "acc-2") {
			t.Errorf("unbound account produced %s", s)
		}
	}
}

func TestBucketUsageOptIn(t *testing.T) {
	api := newFakeAPI()
	got := series(t, NewGridCollector(fakeReader(t, gridObjects(false)...), staticAPI(api), func(string) {}, nil, defaultOpts))
	mustNotContain(t, got, "s3bucket_usage_")
	if n := api.listCalls.Load(); n != 1 {
		t.Errorf("ListTenantUsage called %d times, want 1", n)
	}

	api = newFakeAPI()
	got = series(t, NewGridCollector(fakeReader(t, gridObjects(true)...), staticAPI(api), func(string) {}, nil, defaultOpts))
	if n := api.listCalls.Load(); n != 0 {
		t.Errorf("ListTenantUsage called %d times, want 0", n)
	}
	if !slices.Equal(api.bucketIDs, []string{"7173", "7174"}) {
		t.Errorf("TenantUsageWithBuckets ids = %v, want managed ids only", api.bucketIDs)
	}
	mustHave(t, got,
		p+`s3bucket_usage_bytes{bucket_name="`+flatcarBucket+`",namespace="team-a",s3bucket="b1",s3tenant="t1"} 1000`,
		p+`s3bucket_usage_objects{bucket_name="`+flatcarBucket+`",namespace="team-a",s3bucket="b1",s3tenant="t1"} 3`,
		p+`s3tenant_usage_bytes{namespace="team-a",s3tenant="t1",s3tenantaccount="acc-1",storagegrid="primary-grid",tenant_id="7173"} 1569128713`,
		p+`grid_scrape_success{step="bucket_usage",storagegrid="primary-grid"} 1`,
	)
	mustNotContain(t, got, `step="tenant_usage"`)
}

func TestTimeout(t *testing.T) {
	api := newFakeAPI()
	api.block = true
	opts := defaultOpts
	opts.Timeout = 50 * time.Millisecond
	start := time.Now()
	got := series(t, NewGridCollector(fakeReader(t, gridObjects(false)...), staticAPI(api), func(string) {}, nil, opts))
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("Collect took %s", d)
	}
	mustHave(t, got, p+`grid_scrape_success{step="tenant_usage",storagegrid="primary-grid"} 0`)
}

func TestUnauthorizedDropsClient(t *testing.T) {
	api := newFakeAPI()
	api.err = errors.New("API error: 401")
	var dropped []string
	var mu sync.Mutex
	drop := func(name string) { mu.Lock(); dropped = append(dropped, name); mu.Unlock() }
	got := series(t, NewGridCollector(fakeReader(t, gridObjects(false)...), staticAPI(api), drop, nil, defaultOpts))
	if !slices.Contains(dropped, "primary-grid") {
		t.Errorf("drop called with %v, want primary-grid", dropped)
	}
	mustHave(t, got,
		p+`grid_scrape_success{step="tenant_usage",storagegrid="primary-grid"} 0`,
		p+`grid_scrape_success{step="endpoints",storagegrid="primary-grid"} 0`,
	)
}

func TestHalfProvisioned(t *testing.T) {
	objs := append(gridObjects(true),
		account("acc-3", "primary-grid", "", nil),
		bucket("b2", ""),
		bucket("b3", "not-on-the-grid"),
	)
	got := series(t, NewGridCollector(fakeReader(t, objs...), staticAPI(newFakeAPI()), func(string) {}, nil, defaultOpts))
	mustNotContain(t, got, `"acc-3"`)
	mustNotContain(t, got, `s3bucket="b2"`)
	mustNotContain(t, got, `s3bucket="b3"`)
	mustHave(t, got, p+`s3bucket_usage_bytes{bucket_name="`+flatcarBucket+`",namespace="team-a",s3bucket="b1",s3tenant="t1"} 1000`)
}

func TestOneGridFailing(t *testing.T) {
	bad := newFakeAPI()
	bad.err = errors.New("connection refused")
	good := newFakeAPI()
	provider := func(_ context.Context, sg *s3v1alpha1.StorageGrid) (grid.API, error) {
		if sg.Name == "a" {
			return bad, nil
		}
		return good, nil
	}
	objs := []client.Object{
		storageGrid("a", false), storageGrid("b", false),
		account("acc-a", "a", "7174", nil), account("acc-b", "b", "7173", nil),
	}
	got := series(t, NewGridCollector(fakeReader(t, objs...), provider, func(string) {}, nil, defaultOpts))
	mustHave(t, got,
		p+`grid_scrape_success{step="tenant_usage",storagegrid="a"} 0`,
		p+`grid_scrape_success{step="tenant_usage",storagegrid="b"} 1`,
		p+`s3tenantaccount_usage_bytes{s3tenantaccount="acc-b",storagegrid="b",tenant_id="7173"} 1569128713`,
	)
	mustNotContain(t, got, `"acc-a"`)
}

func TestProviderErrorFailsEnabledSteps(t *testing.T) {
	provider := func(context.Context, *s3v1alpha1.StorageGrid) (grid.API, error) {
		return nil, errors.New("secret not found")
	}
	got := series(t, NewGridCollector(fakeReader(t, gridObjects(true)...), provider, func(string) {}, nil, defaultOpts))
	mustHave(t, got,
		p+`grid_scrape_success{step="bucket_usage",storagegrid="primary-grid"} 0`,
		p+`grid_scrape_success{step="endpoints",storagegrid="primary-grid"} 0`,
	)
}

func TestSingleflightTTL(t *testing.T) {
	for _, tc := range []struct {
		ttl  time.Duration
		want int32
	}{{time.Minute, 1}, {0, 2}} {
		api := newFakeAPI()
		opts := defaultOpts
		opts.CacheTTL = tc.ttl
		c := NewGridCollector(fakeReader(t, gridObjects(false)...), staticAPI(api), func(string) {}, nil, opts)
		series(t, c)
		series(t, c)
		if n := api.listCalls.Load(); n != tc.want {
			t.Errorf("ttl %s: ListTenantUsage called %d times, want %d", tc.ttl, n, tc.want)
		}
	}
}

func TestEndpointMatchByBackingID(t *testing.T) {
	got := series(t, NewGridCollector(fakeReader(t, gridObjects(false)...), staticAPI(newFakeAPI()), func(string) {}, nil, defaultOpts))
	mustHave(t, got,
		p+`s3tenantclass_endpoint_bound{s3tenantclass="shcofe",storagegrid="primary-grid"} 1`,
		p+`s3tenantclass_endpoint_cert_expiry_timestamp_seconds{s3tenantclass="shcofe",storagegrid="primary-grid"} 1800000000`,
		p+`grid_scrape_success{step="endpoints",storagegrid="primary-grid"} 1`,
	)
}

func TestGridNotElectedEmitsNothing(t *testing.T) {
	api := newFakeAPI()
	c := NewGridCollector(fakeReader(t, gridObjects(false)...), staticAPI(api), func(string) {}, make(chan struct{}), defaultOpts)
	if n := testutil.CollectAndCount(c); n != 0 || api.listCalls.Load() != 0 {
		t.Fatalf("got %d series and %d grid calls, want none", n, api.listCalls.Load())
	}
}

func TestClientCacheKey(t *testing.T) {
	sg := storageGrid("primary-grid", false)
	sg.UID = "uid-1"
	sg.Spec.ManagementEndpoint = "https://grid.invalid"
	sg.Spec.SecretRef = corev1.ObjectReference{Namespace: "ops", Name: "grid-creds"}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ops", Name: "grid-creds"},
		Data:       map[string][]byte{"username": []byte("u"), "password": []byte("p")},
	}
	r := fakeReader(t, secret)
	cache := NewClientCache(r)
	ctx := context.Background()
	first, err := cache.Get(ctx, sg)
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := cache.Get(ctx, sg); again != first {
		t.Error("same UID and secret version should reuse the client")
	}
	cache.Drop("primary-grid")
	afterDrop, _ := cache.Get(ctx, sg)
	if afterDrop == first {
		t.Error("Drop should discard the client")
	}
	secret.Data["password"] = []byte("rotated")
	if err := r.(client.Client).Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if rotated, _ := cache.Get(ctx, sg); rotated == afterDrop {
		t.Error("a new secret version should create a new client")
	}
}
