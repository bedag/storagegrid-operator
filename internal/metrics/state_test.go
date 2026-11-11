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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

func testReader(t *testing.T) client.Reader {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := s3v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	limit := resource.MustParse("1Ki")
	tenant := &s3v1alpha1.S3Tenant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "t1"},
		Spec: s3v1alpha1.S3TenantSpec{CommonTenantSpec: s3v1alpha1.CommonTenantSpec{
			StorageGridRef: corev1.LocalObjectReference{Name: "primary-grid"}, S3TenantClassName: "shcofe",
		}},
		Status: s3v1alpha1.S3TenantStatus{
			CommonTenantStatus: s3v1alpha1.CommonTenantStatus{TenantID: "7173", Quota: s3v1alpha1.QuotaStatus{Limit: &limit}},
			S3TenantAccountRef: &corev1.ObjectReference{Name: "acc-1"},
			Conditions:         []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}},
		},
	}
	account := &s3v1alpha1.S3TenantAccount{ObjectMeta: metav1.ObjectMeta{Name: "acc-1"}}
	bucket := &s3v1alpha1.S3Bucket{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "b1"},
		Status:     s3v1alpha1.S3BucketStatus{Phase: s3v1alpha1.BucketPhaseReady},
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(tenant, account, bucket).Build()
}

func collect(t *testing.T, c prometheus.Collector, expected, name string) {
	t.Helper()
	if err := testutil.CollectAndCompare(c, strings.NewReader(expected), name); err != nil {
		t.Fatal(err)
	}
}

func TestStateInfoLabels(t *testing.T) {
	collect(t, NewStateCollector(testReader(t), nil), `
# HELP storagegrid_operator_s3tenant_info Descriptive labels of the resource.
# TYPE storagegrid_operator_s3tenant_info gauge
storagegrid_operator_s3tenant_info{namespace="team-a",s3tenant="t1",s3tenantaccount="acc-1",s3tenantclass="shcofe",storagegrid="primary-grid",tenant_id="7173"} 1
`, "storagegrid_operator_s3tenant_info")
}

func TestStatePhaseOneHot(t *testing.T) {
	collect(t, NewStateCollector(testReader(t), nil), `
# HELP storagegrid_operator_s3bucket_status_phase Current phase, 1 for the current phase and 0 otherwise.
# TYPE storagegrid_operator_s3bucket_status_phase gauge
storagegrid_operator_s3bucket_status_phase{namespace="team-a",phase="Pending",s3bucket="b1"} 0
storagegrid_operator_s3bucket_status_phase{namespace="team-a",phase="Ready",s3bucket="b1"} 1
storagegrid_operator_s3bucket_status_phase{namespace="team-a",phase="Failed",s3bucket="b1"} 0
storagegrid_operator_s3bucket_status_phase{namespace="team-a",phase="Draining",s3bucket="b1"} 0
storagegrid_operator_s3bucket_status_phase{namespace="team-a",phase="Deleting",s3bucket="b1"} 0
`, "storagegrid_operator_s3bucket_status_phase")
}

func TestStateConditionsAndQuota(t *testing.T) {
	c := NewStateCollector(testReader(t), nil)
	collect(t, c, `
# HELP storagegrid_operator_s3tenant_status_condition Resource conditions, 1 per condition present.
# TYPE storagegrid_operator_s3tenant_status_condition gauge
storagegrid_operator_s3tenant_status_condition{namespace="team-a",s3tenant="t1",status="False",type="Ready"} 1
`, "storagegrid_operator_s3tenant_status_condition")
	collect(t, c, `
# HELP storagegrid_operator_s3tenant_quota_limit_bytes Storage quota limit in bytes from status.
# TYPE storagegrid_operator_s3tenant_quota_limit_bytes gauge
storagegrid_operator_s3tenant_quota_limit_bytes{namespace="team-a",s3tenant="t1"} 1024
`, "storagegrid_operator_s3tenant_quota_limit_bytes")
}

func TestClusterScopedHasNoNamespace(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(NewStateCollector(testReader(t), nil))
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, mf := range mfs {
		if !strings.HasPrefix(mf.GetName(), "storagegrid_operator_s3tenantaccount_") {
			continue
		}
		for _, m := range mf.GetMetric() {
			seen++
			for _, l := range m.GetLabel() {
				if l.GetName() == "namespace" {
					t.Errorf("%s has a namespace label", mf.GetName())
				}
			}
		}
	}
	if seen == 0 {
		t.Fatal("no s3tenantaccount series")
	}
}

func TestNotElectedEmitsNothing(t *testing.T) {
	if n := testutil.CollectAndCount(NewStateCollector(testReader(t), make(chan struct{}))); n != 0 {
		t.Fatalf("got %d series, want 0", n)
	}
}
