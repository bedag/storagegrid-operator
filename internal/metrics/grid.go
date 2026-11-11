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
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	"github.com/bedag/storagegrid-operator/pkg/grid"
)

var (
	tenantLabels  = []string{"namespace", "s3tenant", "s3tenantaccount", "storagegrid", "tenant_id"}
	accountLabels = []string{"s3tenantaccount", "storagegrid", "tenant_id"}
	bucketLabels  = []string{"namespace", "s3bucket", "s3tenant", "bucket_name"}
	classLabels   = []string{"s3tenantclass", "storagegrid"}

	tenantUsageBytes    = gridDesc("s3tenant_usage_bytes", "Bytes stored by the tenant.", tenantLabels)
	tenantUsageObjects  = gridDesc("s3tenant_usage_objects", "Objects stored by the tenant.", tenantLabels)
	tenantQuotaBytes    = gridDesc("s3tenant_quota_bytes", "Quota of the tenant in bytes.", tenantLabels)
	accountUsageBytes   = gridDesc("s3tenantaccount_usage_bytes", "Bytes stored by the tenant account.", accountLabels)
	accountUsageObjects = gridDesc("s3tenantaccount_usage_objects", "Objects stored by the tenant account.", accountLabels)
	accountQuotaBytes   = gridDesc("s3tenantaccount_quota_bytes", "Quota of the tenant account in bytes.", accountLabels)
	bucketUsageBytes    = gridDesc("s3bucket_usage_bytes", "Bytes stored in the bucket.", bucketLabels)
	bucketUsageObjects  = gridDesc("s3bucket_usage_objects", "Objects stored in the bucket.", bucketLabels)
	endpointBound       = gridDesc("s3tenantclass_endpoint_bound", "1 when the endpoint is bound on all gateway nodes.", classLabels)
	endpointCertExpiry  = gridDesc("s3tenantclass_endpoint_cert_expiry_timestamp_seconds",
		"Earliest certificate expiry of the endpoint in Unix seconds.", classLabels)
	scrapeSuccess  = gridDesc("grid_scrape_success", "1 when the step's grid fetch succeeded.", []string{"storagegrid", "step"})
	scrapeDuration = gridDesc("grid_scrape_duration_seconds", "Time spent fetching the grid's metrics.", []string{"storagegrid"})
)

func gridDesc(name, help string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(metricPrefix+name, help, labels, nil)
}

// GridCollectorOptions tunes the grid collector.
type GridCollectorOptions struct {
	// Timeout bounds the collection of one grid.
	Timeout time.Duration
	// CacheTTL is how long a grid's result is reused; 0 disables reuse.
	CacheTTL time.Duration
	// BucketConcurrency caps the bucket usage requests in flight per grid.
	BucketConcurrency int
}

type gridResult struct {
	at      time.Time
	metrics []prometheus.Metric
}

type gridCollector struct {
	reader  client.Reader
	api     APIProvider
	drop    func(string)
	elected <-chan struct{}
	opts    GridCollectorOptions

	flight  singleflight.Group
	mu      sync.Mutex
	results map[string]gridResult // By StorageGrid name.
}

// NewGridCollector returns an unchecked collector exporting live usage and
// endpoint health of every StorageGrid. drop is called with the grid name when
// the grid rejects the client's credentials. It emits nothing until elected is closed.
func NewGridCollector(c client.Reader, api APIProvider, drop func(string), elected <-chan struct{}, opts GridCollectorOptions) prometheus.Collector {
	return &gridCollector{reader: c, api: api, drop: drop, elected: elected, opts: opts, results: map[string]gridResult{}}
}

// Describe sends nothing, which makes this an unchecked collector.
func (g *gridCollector) Describe(chan<- *prometheus.Desc) {}

func (g *gridCollector) Collect(ch chan<- prometheus.Metric) {
	if !isElected(g.elected) {
		return
	}
	var grids s3v1alpha1.StorageGridList
	if err := g.reader.List(context.Background(), &grids); err != nil {
		ctrl.Log.WithName("grid-metrics").Error(err, "listing StorageGrids failed")
		return
	}
	g.pruneResults()
	var wg sync.WaitGroup
	for i := range grids.Items {
		sg := &grids.Items[i]
		wg.Go(func() {
			for _, m := range g.metricsFor(sg) {
				ch <- m
			}
		})
	}
	wg.Wait()
}

// metricsFor shares one in-flight fetch per grid and reuses results younger than CacheTTL.
func (g *gridCollector) metricsFor(sg *s3v1alpha1.StorageGrid) []prometheus.Metric {
	v, _, _ := g.flight.Do(sg.Name, func() (any, error) {
		g.mu.Lock()
		r, ok := g.results[sg.Name]
		g.mu.Unlock()
		if ok && time.Since(r.at) < g.opts.CacheTTL {
			return r.metrics, nil
		}
		m := g.scrape(sg)
		if g.opts.CacheTTL > 0 {
			g.mu.Lock()
			g.results[sg.Name] = gridResult{at: time.Now(), metrics: m}
			g.mu.Unlock()
		}
		return m, nil
	})
	return v.([]prometheus.Metric)
}

// pruneResults drops expired results, so nothing is kept beyond the TTL.
func (g *gridCollector) pruneResults() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for name, r := range g.results {
		if time.Since(r.at) >= g.opts.CacheTTL {
			delete(g.results, name)
		}
	}
}

type emitFunc func(d *prometheus.Desc, v float64, labels ...string)

func (g *gridCollector) scrape(sg *s3v1alpha1.StorageGrid) []prometheus.Metric {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), g.opts.Timeout)
	defer cancel()

	var out []prometheus.Metric
	api, apiErr := g.api(ctx, sg)
	// step emits a step's series only when it succeeds, and always its scrape_success.
	step := func(name string, run func(emit emitFunc) error) {
		var series []prometheus.Metric
		err := apiErr
		if err == nil {
			err = run(func(d *prometheus.Desc, v float64, labels ...string) {
				series = append(series, prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...))
			})
		}
		ok := 1.0
		if err != nil {
			ok, series = 0, nil
			ctrl.Log.WithName("grid-metrics").V(1).Info("grid metrics step failed", "storagegrid", sg.Name, "step", name, "err", err.Error())
			if grid.IsUnauthorized(err) {
				g.drop(sg.Name)
			}
		}
		out = append(out, prometheus.MustNewConstMetric(scrapeSuccess, prometheus.GaugeValue, ok, sg.Name, name))
		out = append(out, series...)
	}

	withBuckets := sg.Spec.Metrics != nil && sg.Spec.Metrics.BucketUsage
	usageStep := "tenant_usage"
	if withBuckets {
		usageStep = "bucket_usage"
	}
	step(usageStep, func(emit emitFunc) error { return g.usage(ctx, sg, api, withBuckets, emit) })
	step("endpoints", func(emit emitFunc) error { return g.endpoints(ctx, sg, api, emit) })
	if m := sg.Spec.Metrics; m != nil && m.Grid != nil && m.Grid.Enabled {
		var forwarded []prometheus.Metric
		step("grid", func(emit emitFunc) (err error) {
			forwarded, err = gridQueries(ctx, sg, api)
			return err
		})
		out = append(out, forwarded...)
	}

	return append(out, prometheus.MustNewConstMetric(scrapeDuration, prometheus.GaugeValue, time.Since(start).Seconds(), sg.Name))
}

// usage emits tenant, account and, with withBuckets, bucket usage of the tenants
// this cluster manages on sg. Grid tenants without an account here are dropped.
func (g *gridCollector) usage(ctx context.Context, sg *s3v1alpha1.StorageGrid, api grid.API, withBuckets bool, emit emitFunc) error {
	var accounts s3v1alpha1.S3TenantAccountList
	if err := g.reader.List(ctx, &accounts); err != nil {
		return err
	}
	managed := map[string]*s3v1alpha1.S3TenantAccount{}
	for i := range accounts.Items {
		a := &accounts.Items[i]
		if a.Spec.StorageGridRef.Name == sg.Name && a.Status.TenantID != "" {
			managed[a.Status.TenantID] = a
		}
	}

	var usage map[string]grid.TenantUsageStats
	var err error
	if withBuckets {
		usage, err = api.TenantUsageWithBuckets(ctx, slices.Sorted(maps.Keys(managed)), g.opts.BucketConcurrency)
	} else {
		usage, err = api.ListTenantUsage(ctx)
	}
	if err != nil {
		return err
	}

	for id, a := range managed {
		u, ok := usage[id]
		if !ok {
			continue
		}
		emitUsage(emit, u, accountUsageBytes, accountUsageObjects, accountQuotaBytes, a.Name, sg.Name, id)
		if r := a.Status.S3TenantRef; r != nil && r.Name != "" {
			emitUsage(emit, u, tenantUsageBytes, tenantUsageObjects, tenantQuotaBytes, r.Namespace, r.Name, a.Name, sg.Name, id)
		}
	}
	if withBuckets {
		return g.buckets(ctx, managed, usage, emit)
	}
	return nil
}

func emitUsage(emit emitFunc, u grid.TenantUsageStats, bytes, objects, quota *prometheus.Desc, labels ...string) {
	emit(bytes, float64(u.Bytes), labels...)
	emit(objects, float64(u.Objects), labels...)
	if u.QuotaBytes != nil {
		emit(quota, float64(*u.QuotaBytes), labels...)
	}
}

// buckets emits usage for S3Bucket CRs whose tenant is managed, matched by status.bucketName.
func (g *gridCollector) buckets(ctx context.Context, managed map[string]*s3v1alpha1.S3TenantAccount,
	usage map[string]grid.TenantUsageStats, emit emitFunc) error {
	var tenants s3v1alpha1.S3TenantList
	if err := g.reader.List(ctx, &tenants); err != nil {
		return err
	}
	var buckets s3v1alpha1.S3BucketList
	if err := g.reader.List(ctx, &buckets); err != nil {
		return err
	}
	tenantIDs := map[types.NamespacedName]string{}
	for i := range tenants.Items {
		t := &tenants.Items[i]
		if _, ok := managed[t.Status.TenantID]; ok {
			tenantIDs[types.NamespacedName{Namespace: t.Namespace, Name: t.Name}] = t.Status.TenantID
		}
	}
	for i := range buckets.Items {
		b := &buckets.Items[i]
		ref := types.NamespacedName{Namespace: b.Spec.S3TenantRef.Namespace, Name: b.Spec.S3TenantRef.Name}
		if ref.Namespace == "" {
			ref.Namespace = b.Namespace
		}
		id, ok := tenantIDs[ref]
		if !ok || b.Status.BucketName == "" {
			continue
		}
		bu, ok := usage[id].Buckets[b.Status.BucketName]
		if !ok {
			continue
		}
		labels := []string{b.Namespace, b.Name, ref.Name, b.Status.BucketName}
		emit(bucketUsageBytes, float64(bu.Bytes), labels...)
		emit(bucketUsageObjects, float64(bu.Objects), labels...)
	}
	return nil
}

// endpoints emits health for S3TenantClasses of sg whose backingID the grid reports.
func (g *gridCollector) endpoints(ctx context.Context, sg *s3v1alpha1.StorageGrid, api grid.API, emit emitFunc) error {
	health, err := api.EndpointHealth(ctx)
	if err != nil {
		return err
	}
	var classes s3v1alpha1.S3TenantClassList
	if err := g.reader.List(ctx, &classes); err != nil {
		return err
	}
	for i := range classes.Items {
		c := &classes.Items[i]
		if c.Spec.StorageGridRef.Name != sg.Name || c.Spec.BackingID == "" {
			continue
		}
		h, ok := health[c.Spec.BackingID]
		if !ok {
			continue
		}
		bound := 0.0
		if h.Bound {
			bound = 1
		}
		emit(endpointBound, bound, c.Name, sg.Name)
		if h.CertExpiry != nil {
			emit(endpointCertExpiry, float64(h.CertExpiry.Unix()), c.Name, sg.Name)
		}
	}
	return nil
}
