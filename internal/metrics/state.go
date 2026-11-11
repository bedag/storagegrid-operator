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
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

const metricPrefix = "storagegrid_operator_"

// item is the kind-independent view of one custom resource.
type item struct {
	obj   metav1.Object
	conds []metav1.Condition
	phase string
	info  []string // Values for the kind's info labels, in order.
	ready *bool    // StorageGrid only.
	quota *float64 // Tenant kinds only.
}

// kind describes how to turn one custom resource kind into series.
type kind struct {
	name       string
	namespaced bool
	infoLabels []string
	phases     []string
	hasQuota   bool
	list       func(ctx context.Context, c client.Reader) ([]item, error)
}

type stateCollector struct {
	reader  client.Reader
	elected <-chan struct{}
	kinds   []kind
}

// NewStateCollector returns an unchecked collector exporting the state of the
// operator's custom resources. It emits nothing until elected is closed.
func NewStateCollector(c client.Reader, elected <-chan struct{}) prometheus.Collector {
	return &stateCollector{reader: c, elected: elected, kinds: stateKinds()}
}

// Describe sends nothing, which makes this an unchecked collector.
func (s *stateCollector) Describe(chan<- *prometheus.Desc) {}

func (s *stateCollector) Collect(ch chan<- prometheus.Metric) {
	if !isElected(s.elected) {
		return
	}
	ctx := context.Background()
	listErrors := prometheus.NewDesc(metricPrefix+"state_list_errors", "1 when listing a kind failed during the scrape.", []string{"kind"}, nil)
	for _, k := range s.kinds {
		items, err := k.list(ctx, s.reader)
		if err != nil {
			// Skip the kind: an invalid metric would fail the whole shared /metrics scrape.
			ctrl.Log.WithName("state-metrics").Error(err, "listing resources failed", "kind", k.name)
			gauge(ch, listErrors, 1, k.name)
			continue
		}
		k.collect(items, ch)
	}
}

func (k kind) desc(suffix, help string, extra ...string) *prometheus.Desc {
	return prometheus.NewDesc(metricPrefix+k.name+suffix, help, append(k.identity(), extra...), nil)
}

func (k kind) identity() []string {
	if k.namespaced {
		return []string{"namespace", k.name}
	}
	return []string{k.name}
}

func (k kind) idValues(o metav1.Object) []string {
	if k.namespaced {
		return []string{o.GetNamespace(), o.GetName()}
	}
	return []string{o.GetName()}
}

func gauge(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
}

func (k kind) collect(items []item, ch chan<- prometheus.Metric) {
	info := k.desc("_info", "Descriptive labels of the resource.", k.infoLabels...)
	cond := k.desc("_status_condition", "Resource conditions, 1 per condition present.", "type", "status")
	created := k.desc("_created", "Creation time in Unix seconds.")
	phase := k.desc("_status_phase", "Current phase, 1 for the current phase and 0 otherwise.", "phase")
	quota := k.desc("_quota_limit_bytes", "Storage quota limit in bytes from status.")
	ready := prometheus.NewDesc(metricPrefix+"storagegrid_ready", "Whether the StorageGrid is ready.", k.identity(), nil)

	for _, it := range items {
		id := k.idValues(it.obj)
		gauge(ch, info, 1, append(slices.Clone(id), it.info...)...)
		gauge(ch, created, float64(it.obj.GetCreationTimestamp().Unix()), id...)
		for _, c := range it.conds {
			gauge(ch, cond, 1, append(slices.Clone(id), c.Type, string(c.Status))...)
		}
		if it.phase != "" {
			phases := k.phases
			if !slices.Contains(phases, it.phase) {
				phases = append(slices.Clone(phases), it.phase)
			}
			for _, p := range phases {
				v := 0.0
				if p == it.phase {
					v = 1
				}
				gauge(ch, phase, v, append(slices.Clone(id), p)...)
			}
		}
		if it.ready != nil {
			v := 0.0
			if *it.ready {
				v = 1
			}
			gauge(ch, ready, v, id...)
		}
		if k.hasQuota && it.quota != nil {
			gauge(ch, quota, *it.quota, id...)
		}
	}
}

func quotaBytes(q *resource.Quantity) *float64 {
	if q == nil {
		return nil
	}
	v := float64(q.Value())
	return &v
}

func tenantPhases() []string {
	// Union of the CRD enum on CommonTenantStatus.Phase and the Go constants.
	var out []string
	for _, p := range []string{"Pending", "Ready", "Failed", "Deleting", "InProgress", "Retaining", "RetainWait", "Bound"} {
		out = append(out, p)
	}
	for _, p := range []s3v1alpha1.Phase{
		s3v1alpha1.PhasePending, s3v1alpha1.PhaseReady, s3v1alpha1.PhaseFailed, s3v1alpha1.PhaseDeleting,
		s3v1alpha1.PhaseInProgress, s3v1alpha1.PhaseRetainThenDelete, s3v1alpha1.PhaseBound,
	} {
		if !slices.Contains(out, string(p)) {
			out = append(out, string(p))
		}
	}
	return out
}

func stateKinds() []kind {
	return []kind{
		{name: "storagegrid", infoLabels: []string{"management_endpoint"}, list: func(ctx context.Context, c client.Reader) ([]item, error) {
			var l s3v1alpha1.StorageGridList
			if err := c.List(ctx, &l); err != nil {
				return nil, err
			}
			var out []item
			for i := range l.Items {
				o := &l.Items[i]
				out = append(out, item{obj: o, conds: o.Status.Conditions, info: []string{o.Spec.ManagementEndpoint}, ready: &o.Status.Ready})
			}
			return out, nil
		}},
		{name: "s3tenantclass", infoLabels: []string{"storagegrid", "backing_id", "display_name"}, list: func(ctx context.Context, c client.Reader) ([]item, error) {
			var l s3v1alpha1.S3TenantClassList
			if err := c.List(ctx, &l); err != nil {
				return nil, err
			}
			var out []item
			for i := range l.Items {
				o := &l.Items[i]
				out = append(out, item{obj: o, conds: o.Status.Conditions, info: []string{o.Spec.StorageGridRef.Name, o.Spec.BackingID, o.Status.DisplayName}})
			}
			return out, nil
		}},
		{
			name: "s3tenantaccount", phases: tenantPhases(), hasQuota: true,
			infoLabels: []string{"storagegrid", "s3tenantclass", "tenant_id", "backend_name", "deletion_policy", "bound_namespace", "bound_s3tenant"},
			list: func(ctx context.Context, c client.Reader) ([]item, error) {
				var l s3v1alpha1.S3TenantAccountList
				if err := c.List(ctx, &l); err != nil {
					return nil, err
				}
				var out []item
				for i := range l.Items {
					o := &l.Items[i]
					var backend, policy, boundNS, bound string
					if o.Status.ObservedTenantBackendName != nil {
						backend = *o.Status.ObservedTenantBackendName
					}
					if o.Status.TenantDeletionPolicy != nil {
						policy = string(o.Status.TenantDeletionPolicy.Policy)
					}
					if r := o.Status.S3TenantRef; r != nil {
						boundNS, bound = r.Namespace, r.Name
					}
					out = append(out, item{
						obj: o, conds: o.Status.Conditions, phase: string(o.Status.Phase), quota: quotaBytes(o.Status.Quota.Limit),
						info: []string{o.Spec.StorageGridRef.Name, o.Spec.S3TenantClassName, o.Status.TenantID, backend, policy, boundNS, bound},
					})
				}
				return out, nil
			},
		},
		{
			name: "s3tenant", namespaced: true, phases: tenantPhases(), hasQuota: true,
			infoLabels: []string{"storagegrid", "s3tenantclass", "s3tenantaccount", "tenant_id"},
			list: func(ctx context.Context, c client.Reader) ([]item, error) {
				var l s3v1alpha1.S3TenantList
				if err := c.List(ctx, &l); err != nil {
					return nil, err
				}
				var out []item
				for i := range l.Items {
					o := &l.Items[i]
					var acc string
					if o.Status.S3TenantAccountRef != nil {
						acc = o.Status.S3TenantAccountRef.Name
					}
					out = append(out, item{
						obj: o, conds: o.Status.Conditions, phase: string(o.Status.Phase), quota: quotaBytes(o.Status.Quota.Limit),
						info: []string{o.Spec.StorageGridRef.Name, o.Spec.S3TenantClassName, acc, o.Status.TenantID},
					})
				}
				return out, nil
			},
		},
		{
			name: "s3bucket", namespaced: true, infoLabels: []string{"s3tenant", "bucket_name", "region"},
			phases: []string{
				string(s3v1alpha1.BucketPhasePending), string(s3v1alpha1.BucketPhaseReady), string(s3v1alpha1.BucketPhaseFailed),
				string(s3v1alpha1.BucketPhaseDraining), string(s3v1alpha1.BucketPhaseDeleting),
			},
			list: func(ctx context.Context, c client.Reader) ([]item, error) {
				var l s3v1alpha1.S3BucketList
				if err := c.List(ctx, &l); err != nil {
					return nil, err
				}
				var out []item
				for i := range l.Items {
					o := &l.Items[i]
					out = append(out, item{
						obj: o, conds: o.Status.Conditions, phase: string(o.Status.Phase),
						info: []string{o.Spec.S3TenantRef.Name, o.Status.BucketName, o.Status.Region},
					})
				}
				return out, nil
			},
		},
		{
			name: "s3access", namespaced: true, infoLabels: []string{"s3bucket"},
			phases: []string{string(s3v1alpha1.S3AccessPhasePending), string(s3v1alpha1.S3AccessPhaseReady), string(s3v1alpha1.S3AccessPhaseFailed)},
			list: func(ctx context.Context, c client.Reader) ([]item, error) {
				var l s3v1alpha1.S3AccessList
				if err := c.List(ctx, &l); err != nil {
					return nil, err
				}
				var out []item
				for i := range l.Items {
					o := &l.Items[i]
					out = append(out, item{obj: o, conds: o.Status.Conditions, phase: string(o.Status.Phase), info: []string{o.Spec.S3BucketRef.Name}})
				}
				return out, nil
			},
		},
		{name: "globals3policy", list: func(ctx context.Context, c client.Reader) ([]item, error) {
			var l s3v1alpha1.GlobalS3PolicyList
			if err := c.List(ctx, &l); err != nil {
				return nil, err
			}
			var out []item
			for i := range l.Items {
				out = append(out, item{obj: &l.Items[i], conds: l.Items[i].Status.Conditions})
			}
			return out, nil
		}},
		{name: "s3policy", namespaced: true, list: func(ctx context.Context, c client.Reader) ([]item, error) {
			var l s3v1alpha1.S3PolicyList
			if err := c.List(ctx, &l); err != nil {
				return nil, err
			}
			var out []item
			for i := range l.Items {
				out = append(out, item{obj: &l.Items[i], conds: l.Items[i].Status.Conditions})
			}
			return out, nil
		}},
	}
}
