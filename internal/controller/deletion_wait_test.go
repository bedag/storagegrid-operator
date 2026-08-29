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

package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

// bucketTenantIndexKey mirrors the index registered by S3TenantReconciler.SetupWithManager.
// Tests build the fake client with the same key so that a mismatch between the registered
// index and the queries that use it fails here rather than silently at runtime.
func bucketTenantIndexKey(obj client.Object) []string {
	bucket := obj.(*s3v1alpha1.S3Bucket)
	ns := bucket.Spec.S3TenantRef.Namespace
	if ns == "" {
		ns = bucket.Namespace
	}
	return []string{ns + "/" + bucket.Spec.S3TenantRef.Name}
}

func newTenantReconciler(objs ...client.Object) *S3TenantReconciler {
	c := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithIndex(&s3v1alpha1.S3Bucket{}, "spec.s3TenantRef.namespacedName", bucketTenantIndexKey).
		WithObjects(objs...).
		WithStatusSubresource(&s3v1alpha1.S3Tenant{}, &s3v1alpha1.S3TenantAccount{}, &s3v1alpha1.S3Bucket{}).
		Build()

	return &S3TenantReconciler{
		Client:   c,
		Scheme:   scheme.Scheme,
		Recorder: record.NewFakeRecorder(50),
	}
}

func deletingTenant(name string) *s3v1alpha1.S3Tenant {
	now := metav1.Now()

	return &s3v1alpha1.S3Tenant{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{s3TenantFinalizer},
		},
		Spec: s3v1alpha1.S3TenantSpec{
			CommonTenantSpec: s3v1alpha1.CommonTenantSpec{
				StorageGridRef: corev1.LocalObjectReference{Name: "test-grid"},
			},
		},
	}
}

func bucketFor(name, tenantName string) *s3v1alpha1.S3Bucket {
	return &s3v1alpha1.S3Bucket{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: s3v1alpha1.S3BucketSpec{
			S3TenantRef: corev1.ObjectReference{Name: tenantName},
		},
	}
}

var _ = Describe("Blocked deletion waits instead of failing", func() {
	ctx := context.Background()

	Context("when an S3Tenant still has linked buckets", func() {
		It("returns no error, keeps the finalizer, and schedules a bounded requeue", func() {
			tenant := deletingTenant("blocked-tenant")
			account := &s3v1alpha1.S3TenantAccount{ObjectMeta: metav1.ObjectMeta{Name: "acct"}}
			r := newTenantReconciler(tenant, account, bucketFor("logs", "blocked-tenant"))

			rctx := &tenantReconcileContext{S3Tenant: tenant, Account: account}

			By("finalizing while a bucket still references the tenant")
			// A wait must not surface as an error: controller-runtime discards the Result
			// when the error is non-nil, which is what forced the ~17m backoff.
			Expect(r.finalize(ctx, rctx)).To(Succeed())

			By("scheduling a bounded requeue instead of relying on the workqueue backoff")
			Expect(rctx.RequeAfter.Duration).To(Equal(s3v1alpha1.DefaultDeletionPollInterval))

			By("reporting the wait as progress, not as a reconcile failure")
			deleting := meta.FindStatusCondition(tenant.Status.Conditions, s3v1alpha1.ConditionTypeDeleting)
			Expect(deleting).NotTo(BeNil())
			Expect(deleting.Status).To(Equal(metav1.ConditionTrue))
			Expect(deleting.Reason).To(Equal("WaitingForBuckets"))
			Expect(deleting.Message).To(ContainSubstring("logs"))

			reconciled := meta.FindStatusCondition(tenant.Status.Conditions, s3v1alpha1.ConditionTypeReconcileSucceeded)
			Expect(reconciled).NotTo(BeNil())
			Expect(reconciled.Status).To(Equal(metav1.ConditionTrue))

			By("keeping the finalizer so the tenant is not released while buckets remain")
			Expect(r.reconcileFinalizerAndDlelete(ctx, rctx)).To(Succeed())
			Expect(tenant.Finalizers).To(ContainElement(s3TenantFinalizer))
		})

		It("blocks on the backend bucket count even when no bucket CRs remain", func() {
			tenant := deletingTenant("backend-blocked")
			tenant.Status.TenantUsage.BucketCount = 3
			account := &s3v1alpha1.S3TenantAccount{ObjectMeta: metav1.ObjectMeta{Name: "acct"}}
			r := newTenantReconciler(tenant, account)

			rctx := &tenantReconcileContext{S3Tenant: tenant, Account: account}

			Expect(r.finalize(ctx, rctx)).To(Succeed())
			Expect(rctx.RequeAfter.Duration).To(BeNumerically(">", 0))
			Expect(meta.FindStatusCondition(tenant.Status.Conditions, s3v1alpha1.ConditionTypeDeleting).Message).
				To(ContainSubstring("3 bucket(s)"))
		})
	})

	Context("when nothing blocks the deletion", func() {
		It("completes finalization and releases the finalizer", func() {
			tenant := deletingTenant("free-tenant")
			account := &s3v1alpha1.S3TenantAccount{
				ObjectMeta: metav1.ObjectMeta{Name: "acct"},
				Spec: s3v1alpha1.S3TenantAccountSpec{
					S3TenantRef: &corev1.ObjectReference{Name: "free-tenant", Namespace: "default"},
				},
			}
			r := newTenantReconciler(tenant, account)

			rctx := &tenantReconcileContext{S3Tenant: tenant, Account: account}

			Expect(r.finalize(ctx, rctx)).To(Succeed())

			By("not scheduling a wait")
			Expect(rctx.RequeAfter.Duration).To(BeZero())

			By("unbinding the account")
			Expect(account.Spec.S3TenantRef).To(BeNil())

			By("removing the finalizer")
			Expect(r.reconcileFinalizerAndDlelete(ctx, rctx)).To(Succeed())
			Expect(tenant.Finalizers).NotTo(ContainElement(s3TenantFinalizer))
		})
	})

	Context("emitting events for a wait", func() {
		It("emits once on entry, not on every poll", func() {
			tenant := deletingTenant("chatty-tenant")
			account := &s3v1alpha1.S3TenantAccount{ObjectMeta: metav1.ObjectMeta{Name: "acct"}}
			r := newTenantReconciler(tenant, account, bucketFor("logs", "chatty-tenant"))
			recorder := r.Recorder.(*record.FakeRecorder)

			rctx := &tenantReconcileContext{S3Tenant: tenant, Account: account}

			Expect(r.finalize(ctx, rctx)).To(Succeed())
			Expect(recorder.Events).To(HaveLen(1))

			By("polling again with the same reason")
			Expect(r.finalize(ctx, rctx)).To(Succeed())
			Expect(recorder.Events).To(HaveLen(1), "a repeated wait must not re-emit its event")
		})
	})
})

var _ = Describe("Deletion poll interval resolution", func() {
	ctx := context.Background()

	grid := func(interval *metav1.Duration) *s3v1alpha1.StorageGrid {
		sg := &s3v1alpha1.StorageGrid{ObjectMeta: metav1.ObjectMeta{Name: "test-grid"}}
		if interval != nil {
			sg.Spec.Operations = &s3v1alpha1.OperationsConfig{
				Deletion: &s3v1alpha1.DeletionConfig{PollInterval: interval},
			}
		}
		return sg
	}

	It("prefers the tenant spec over the grid config", func() {
		tenant := deletingTenant("t")
		tenant.Spec.DeletionPollInterval = &metav1.Duration{Duration: 90 * time.Second}
		r := newTenantReconciler(tenant, grid(&metav1.Duration{Duration: 10 * time.Second}))

		Expect(r.computeDeletionPollInterval(ctx, &tenantReconcileContext{S3Tenant: tenant})).
			To(Equal(90 * time.Second))
	})

	It("falls back to the grid config when the tenant does not override it", func() {
		tenant := deletingTenant("t")
		r := newTenantReconciler(tenant, grid(&metav1.Duration{Duration: 10 * time.Second}))

		Expect(r.computeDeletionPollInterval(ctx, &tenantReconcileContext{S3Tenant: tenant})).
			To(Equal(10 * time.Second))
	})

	It("falls back to the default when neither is set", func() {
		tenant := deletingTenant("t")
		r := newTenantReconciler(tenant, grid(nil))

		Expect(r.computeDeletionPollInterval(ctx, &tenantReconcileContext{S3Tenant: tenant})).
			To(Equal(s3v1alpha1.DefaultDeletionPollInterval))
	})

	It("falls back to the default when the grid cannot be fetched", func() {
		tenant := deletingTenant("t")
		r := newTenantReconciler(tenant)

		Expect(r.computeDeletionPollInterval(ctx, &tenantReconcileContext{S3Tenant: tenant})).
			To(Equal(s3v1alpha1.DefaultDeletionPollInterval))
	})
})

var _ = Describe("Waking a tenant when its buckets change", func() {
	ctx := context.Background()

	It("maps a bucket to its tenant, defaulting the namespace to the bucket's own", func() {
		r := newTenantReconciler()

		requests := r.mapBucketToTenant(ctx, bucketFor("logs", "my-tenant"))
		Expect(requests).To(HaveLen(1))
		Expect(requests[0].Name).To(Equal("my-tenant"))
		Expect(requests[0].Namespace).To(Equal("default"))
	})

	It("honors an explicit tenant namespace", func() {
		r := newTenantReconciler()

		bucket := bucketFor("logs", "my-tenant")
		bucket.Spec.S3TenantRef.Namespace = "elsewhere"

		requests := r.mapBucketToTenant(ctx, bucket)
		Expect(requests).To(HaveLen(1))
		Expect(requests[0].Namespace).To(Equal("elsewhere"))
	})

	It("ignores objects that are not S3Buckets", func() {
		r := newTenantReconciler()
		Expect(r.mapBucketToTenant(ctx, &s3v1alpha1.S3Tenant{})).To(BeNil())
	})

	Context("the watch predicate", func() {
		p := bucketDeletionRelevantPredicate()

		It("passes deletes, which are what unblock a terminating tenant", func() {
			Expect(p.Delete(event.DeleteEvent{Object: bucketFor("logs", "t")})).To(BeTrue())
		})

		It("passes creates, which add to LinkedBuckets", func() {
			Expect(p.Create(event.CreateEvent{Object: bucketFor("logs", "t")})).To(BeTrue())
		})

		It("passes the moment a bucket starts terminating", func() {
			now := metav1.Now()
			old := bucketFor("logs", "t")
			updated := bucketFor("logs", "t")
			updated.DeletionTimestamp = &now

			Expect(p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated})).To(BeTrue())
		})

		It("ignores status-only updates so many buckets do not churn the tenant", func() {
			old := bucketFor("logs", "t")
			updated := bucketFor("logs", "t")
			updated.Status.BucketUsage.ObjectCount = 42

			Expect(p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated})).To(BeFalse())
		})
	})
})

var _ = Describe("The S3Bucket-by-tenant field index", func() {
	ctx := context.Background()

	// mapTenantToBuckets previously queried "spec.s3TenantRef.name", which was never
	// registered, so the List always errored and the watch was a silent no-op.
	It("is queryable under the name the controllers actually register", func() {
		c := fake.NewClientBuilder().
			WithScheme(scheme.Scheme).
			WithIndex(&s3v1alpha1.S3Bucket{}, "spec.s3TenantRef.namespacedName", bucketTenantIndexKey).
			WithObjects(bucketFor("logs", "my-tenant"), bucketFor("other", "different-tenant")).
			Build()

		buckets := &s3v1alpha1.S3BucketList{}
		Expect(c.List(ctx, buckets, client.MatchingFields{
			"spec.s3TenantRef.namespacedName": "default/my-tenant",
		})).To(Succeed())

		Expect(buckets.Items).To(HaveLen(1))
		Expect(buckets.Items[0].Name).To(Equal("logs"))
	})
})

var _ = Describe("A terminating tenant reports Deleting, not Bound", func() {
	ctx := context.Background()

	It("sets the Deleting phase and surfaces why", func() {
		tenant := deletingTenant("terminating")
		r := newTenantReconciler(tenant)

		r.setCondition(tenant, s3v1alpha1.ConditionTypeDeleting, metav1.ConditionTrue,
			"WaitingForBuckets", "Waiting for 1 linked S3Bucket(s) to be deleted: [logs]")

		r.deriveReadiness(ctx, tenant)

		Expect(tenant.Status.Phase).To(Equal(s3v1alpha1.PhaseDeleting))

		ready := meta.FindStatusCondition(tenant.Status.Conditions, s3v1alpha1.ConditionTypeReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Message).To(ContainSubstring("logs"))
	})

	It("still reports Bound for a live tenant", func() {
		tenant := &s3v1alpha1.S3Tenant{ObjectMeta: metav1.ObjectMeta{Name: "live", Namespace: "default"}}
		r := newTenantReconciler(tenant)

		r.deriveReadiness(ctx, tenant)

		Expect(tenant.Status.Phase).To(Equal(s3v1alpha1.PhaseBound))
	})
})

var _ = Describe("Poll interval validation", func() {
	ctx := context.Background()

	// A zero interval translates to RequeueAfter: 0, which with a nil error means
	// "do not requeue" - an in-flight drain or a blocked deletion would silently stop
	// polling. The CRD rejects it at admission so it can never reach the controller.
	newGrid := func(name string, deletion *s3v1alpha1.DeletionConfig, drain *s3v1alpha1.DrainConfig) *s3v1alpha1.StorageGrid {
		return &s3v1alpha1.StorageGrid{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: s3v1alpha1.StorageGridSpec{
				SecretRef: corev1.ObjectReference{
					Name:      "creds",
					Namespace: "default",
				},
				ManagementEndpoint: "https://storagegrid.example.com",
				Operations:         &s3v1alpha1.OperationsConfig{Deletion: deletion, Drain: drain},
			},
		}
	}

	It("rejects a zero deletion pollInterval", func() {
		grid := newGrid("zero-poll", &s3v1alpha1.DeletionConfig{
			PollInterval: &metav1.Duration{Duration: 0},
		}, nil)

		err := k8sClient.Create(ctx, grid)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("pollInterval must be at least 5s"))
	})

	It("rejects a zero drain pollInterval", func() {
		grid := newGrid("zero-drain", nil, &s3v1alpha1.DrainConfig{
			InitialPollInterval: &metav1.Duration{Duration: 0},
		})

		err := k8sClient.Create(ctx, grid)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("initialPollInterval must be at least 5s"))
	})

	It("rejects a zero deletionPollInterval on an S3Tenant", func() {
		tenant := &s3v1alpha1.S3Tenant{
			ObjectMeta: metav1.ObjectMeta{Name: "zero-interval", Namespace: "default"},
			Spec: s3v1alpha1.S3TenantSpec{
				CommonTenantSpec: s3v1alpha1.CommonTenantSpec{
					StorageGridRef: corev1.LocalObjectReference{Name: "test-grid"},
					StorageQuota:   ptr.To(resource.MustParse("1Gi")),
				},
				DeletionPollInterval: &metav1.Duration{Duration: 0},
			},
		}

		err := k8sClient.Create(ctx, tenant)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("deletionPollInterval must be at least 5s"))
	})

	It("accepts a sane interval", func() {
		grid := newGrid("sane-poll", &s3v1alpha1.DeletionConfig{
			PollInterval: &metav1.Duration{Duration: 45 * time.Second},
		}, nil)

		Expect(k8sClient.Create(ctx, grid)).To(Succeed())
		Expect(k8sClient.Delete(ctx, grid)).To(Succeed())
	})
})

var _ = Describe("A terminating bucket", func() {
	ctx := context.Background()

	newBucketReconciler := func() *S3BucketReconciler {
		return &S3BucketReconciler{
			Client:   fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(),
			Scheme:   scheme.Scheme,
			Recorder: record.NewFakeRecorder(50),
		}
	}

	terminatingBucket := func() *s3v1alpha1.S3Bucket {
		now := metav1.Now()

		return &s3v1alpha1.S3Bucket{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "logs",
				Namespace:         "default",
				DeletionTimestamp: &now,
				Finalizers:        []string{s3BucketFinalizer},
			},
			Status: s3v1alpha1.S3BucketStatus{BucketName: "logs"},
		}
	}

	Context("when a drain finishes or is canceled", func() {
		// completeDrain and cancelDrain reset the phase to Ready. For a bucket that is being
		// deleted that would misreport a terminating object as healthy, and it also skips the
		// drain requeue branch in Reconcile.
		It("stays in Deleting rather than flipping back to Ready", func() {
			r := newBucketReconciler()
			rctx := &bucketReconcileContext{Bucket: terminatingBucket()}

			Expect(r.phaseAfterDrain(rctx)).To(Equal(s3v1alpha1.BucketPhaseDeleting))
		})

		It("returns a live bucket to Ready", func() {
			r := newBucketReconciler()
			rctx := &bucketReconcileContext{
				Bucket: &s3v1alpha1.S3Bucket{ObjectMeta: metav1.ObjectMeta{Name: "logs", Namespace: "default"}},
			}

			Expect(r.phaseAfterDrain(rctx)).To(Equal(s3v1alpha1.BucketPhaseReady))
		})
	})

	Context("while it waits to become empty", func() {
		It("reports the wait as progress and schedules a bounded requeue", func() {
			r := newBucketReconciler()
			rctx := &bucketReconcileContext{Bucket: terminatingBucket()}

			r.waitForDeletion(ctx, rctx, "BucketNotEmpty",
				"Bucket logs still contains 5 object(s).", 3*time.Minute)

			Expect(rctx.RequeAfter.Duration).To(Equal(3 * time.Minute))

			deleting := meta.FindStatusCondition(rctx.Bucket.Status.Conditions, s3v1alpha1.ConditionTypeDeleting)
			Expect(deleting).NotTo(BeNil())
			Expect(deleting.Reason).To(Equal("BucketNotEmpty"))

			reconciled := meta.FindStatusCondition(rctx.Bucket.Status.Conditions, s3v1alpha1.ConditionTypeReconcileSucceeded)
			Expect(reconciled.Status).To(Equal(metav1.ConditionTrue),
				"waiting for a bucket to empty is not a reconcile failure")
		})

		It("emits once per state, not per poll", func() {
			r := newBucketReconciler()
			recorder := r.Recorder.(*record.FakeRecorder)
			rctx := &bucketReconcileContext{Bucket: terminatingBucket()}

			r.waitForDeletion(ctx, rctx, "BucketNotEmpty", "still 5 objects", time.Minute)
			Expect(recorder.Events).To(HaveLen(1))

			r.waitForDeletion(ctx, rctx, "BucketNotEmpty", "still 5 objects", time.Minute)
			Expect(recorder.Events).To(HaveLen(1))

			By("emitting again when the state actually changes")
			r.waitForDeletion(ctx, rctx, "DrainInProgress", "draining", time.Minute)
			Expect(recorder.Events).To(HaveLen(2))
		})
	})

	Context("deriving readiness", func() {
		It("surfaces why the deletion has not completed", func() {
			r := newBucketReconciler()
			rctx := &bucketReconcileContext{Bucket: terminatingBucket()}

			r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeDeleting, metav1.ConditionTrue,
				"BucketNotEmpty", "Bucket logs still contains 5 object(s).")

			r.deriveReadiness(ctx, rctx)

			Expect(rctx.Bucket.Status.Phase).To(Equal(s3v1alpha1.BucketPhaseDeleting))

			ready := meta.FindStatusCondition(rctx.Bucket.Status.Conditions, s3v1alpha1.ConditionTypeReady)
			Expect(ready.Status).To(Equal(metav1.ConditionFalse))
			Expect(ready.Reason).To(Equal("BucketNotEmpty"))
			Expect(ready.Message).To(ContainSubstring("5 object(s)"))
		})
	})
})

var _ = Describe("Dependents of a terminating tenant", func() {
	ctx := context.Background()

	// Regression: deriveReadiness now reports PhaseDeleting for a terminating tenant, where
	// it previously always reported Bound. Both S3Bucket and S3Access gate on the tenant
	// phase *before* their own finalizer step, so a strict Bound check deadlocked the pair -
	// the bucket could never finalize, and the tenant waits on exactly that bucket.
	boundTenant := &s3v1alpha1.S3Tenant{
		Status: s3v1alpha1.S3TenantStatus{
			CommonTenantStatus: s3v1alpha1.CommonTenantStatus{Phase: s3v1alpha1.PhaseBound},
		},
	}
	deletingTenantPhase := &s3v1alpha1.S3Tenant{
		Status: s3v1alpha1.S3TenantStatus{
			CommonTenantStatus: s3v1alpha1.CommonTenantStatus{Phase: s3v1alpha1.PhaseDeleting},
		},
	}
	pendingTenant := &s3v1alpha1.S3Tenant{
		Status: s3v1alpha1.S3TenantStatus{
			CommonTenantStatus: s3v1alpha1.CommonTenantStatus{Phase: s3v1alpha1.PhasePending},
		},
	}

	It("lets a terminating dependent clean up through a terminating tenant", func() {
		Expect(deletingTenantPhase.CanServeBackendOperations(true)).To(BeTrue())
	})

	It("still refuses a live dependent on a terminating tenant", func() {
		Expect(deletingTenantPhase.CanServeBackendOperations(false)).To(BeFalse())
	})

	It("serves a bound tenant either way", func() {
		Expect(boundTenant.CanServeBackendOperations(false)).To(BeTrue())
		Expect(boundTenant.CanServeBackendOperations(true)).To(BeTrue())
	})

	It("does not open the gate for other non-bound phases", func() {
		Expect(pendingTenant.CanServeBackendOperations(true)).To(BeFalse())
	})

	It("does not block a terminating bucket at the readiness gate", func() {
		now := metav1.Now()
		r := &S3BucketReconciler{
			Client:   fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(),
			Scheme:   scheme.Scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		rctx := &bucketReconcileContext{
			Bucket: &s3v1alpha1.S3Bucket{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "logs",
					Namespace:         "default",
					DeletionTimestamp: &now,
					Finalizers:        []string{s3BucketFinalizer},
				},
			},
			S3Tenant: deletingTenantPhase,
		}

		Expect(r.reconcileTenantReadiness(ctx, rctx)).To(Succeed())
	})
})

var _ = Describe("S3TenantAccount phase reflects reality", func() {
	ctx := context.Background()

	newAccountReconciler := func(objs ...client.Object) *S3TenantAccountReconciler {
		return &S3TenantAccountReconciler{
			Client:   fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).Build(),
			Scheme:   scheme.Scheme,
			Recorder: record.NewFakeRecorder(50),
		}
	}

	// Conditions carry ObservedGeneration, and setting a deletionTimestamp bumps generation,
	// so a deleting account's readiness conditions all read as stale. Generation 2 with
	// conditions from generation 1 reproduces that.
	deletingAccount := func() *s3v1alpha1.S3TenantAccount {
		now := metav1.Now()

		return &s3v1alpha1.S3TenantAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "acct",
				Generation:        2,
				DeletionTimestamp: &now,
				Finalizers:        []string{s3TenantAccountFinalizer},
			},
			Status: s3v1alpha1.S3TenantAccountStatus{
				Conditions: []metav1.Condition{
					{Type: s3v1alpha1.ConditionTypeReconcileSucceeded, Status: metav1.ConditionTrue, Reason: "Ok", ObservedGeneration: 1},
					{Type: s3v1alpha1.ContitionTypeBackingResourceReady, Status: metav1.ConditionTrue, Reason: "Ok", ObservedGeneration: 1},
					{Type: s3v1alpha1.ConditionTypeQuotaSufficient, Status: metav1.ConditionTrue, Reason: "Ok", ObservedGeneration: 1},
					{Type: s3v1alpha1.ConditionTypeBound, Status: metav1.ConditionTrue, Reason: "Ok", ObservedGeneration: 1},
				},
			},
		}
	}

	It("reports Deleting, not Failed, while it is being deleted", func() {
		rctx := &accountReconcileContext{Account: deletingAccount()}
		r := newAccountReconciler(rctx.Account)

		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeDeleting, metav1.ConditionTrue,
			"BackendConfirmationPending", "waiting for backend confirmation")

		r.derivePhase(ctx, rctx)

		Expect(rctx.Account.Status.Phase).To(Equal(s3v1alpha1.PhaseDeleting))

		ready := meta.FindStatusCondition(rctx.Account.Status.Conditions, s3v1alpha1.ConditionTypeReady)
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal("BackendConfirmationPending"))
	})

	It("does not let the retention branch clobber the backend confirmation requeue", func() {
		// A RetainThenDelete account whose deadline has passed is then deleted for real. The
		// retention branch would recompute RequeAfter as (past deadline - now), i.e. negative,
		// which Reconcile reads as "no requeue" - stranding the account until the next resync.
		past := metav1.NewTime(metav1.Now().Add(-time.Hour))
		account := deletingAccount()
		account.Status.DeletionTimestamp = &past
		account.Status.Conditions = append(account.Status.Conditions,
			metav1.Condition{Type: s3v1alpha1.ConditionTypeRetainThenDelete, Status: metav1.ConditionTrue, Reason: "Retaining"})

		rctx := &accountReconcileContext{
			Account:    account,
			RequeAfter: metav1.Duration{Duration: time.Minute},
		}
		r := newAccountReconciler(account)

		r.derivePhase(ctx, rctx)

		Expect(rctx.RequeAfter.Duration).To(Equal(time.Minute))
		Expect(rctx.Account.Status.Phase).To(Equal(s3v1alpha1.PhaseDeleting))
	})

	It("still reports Bound for a live, bound account", func() {
		account := deletingAccount()
		account.DeletionTimestamp = nil
		account.Generation = 1 // conditions are current

		rctx := &accountReconcileContext{Account: account}
		r := newAccountReconciler(account)

		r.derivePhase(ctx, rctx)

		Expect(rctx.Account.Status.Phase).To(Equal(s3v1alpha1.PhaseBound))
	})

	It("reports Ready once a Retain policy unbinds it", func() {
		// Retain clears spec.s3TenantRef and sets Bound=False; the account becomes claimable.
		account := deletingAccount()
		account.DeletionTimestamp = nil
		account.Generation = 1
		meta.SetStatusCondition(&account.Status.Conditions, metav1.Condition{
			Type: s3v1alpha1.ConditionTypeBound, Status: metav1.ConditionFalse,
			Reason: "S3TenantNotBound", Message: "S3Tenant was deleted, account is now unbound",
			ObservedGeneration: 1,
		})
		meta.SetStatusCondition(&account.Status.Conditions, metav1.Condition{
			Type: s3v1alpha1.ConditionTypeRetained, Status: metav1.ConditionTrue,
			Reason: "Retained", ObservedGeneration: 1,
		})

		rctx := &accountReconcileContext{Account: account}
		r := newAccountReconciler(account)

		r.derivePhase(ctx, rctx)

		Expect(rctx.Account.Status.Phase).To(Equal(s3v1alpha1.PhaseReady))
	})

	It("does not panic when RetainThenDelete outlives its status timestamp", func() {
		// ConditionTypeRetainThenDelete is not generation-gated, so it can outlive the
		// *metav1.Time that set it.
		account := deletingAccount()
		account.DeletionTimestamp = nil
		account.Generation = 1
		account.Status.DeletionTimestamp = nil
		account.Status.Conditions = append(account.Status.Conditions,
			metav1.Condition{Type: s3v1alpha1.ConditionTypeRetainThenDelete, Status: metav1.ConditionTrue, Reason: "Retaining"})

		rctx := &accountReconcileContext{Account: account}
		r := newAccountReconciler(account)

		Expect(func() { r.derivePhase(ctx, rctx) }).NotTo(Panic())
		Expect(rctx.Account.Status.Phase).To(Equal(s3v1alpha1.PhaseRetainThenDelete))
	})
})

var _ = Describe("A failed drain must not stall a terminating bucket", func() {
	ctx := context.Background()
	It("keeps a bounded poll and records the failure instead of erroring out", func() {
		now := metav1.Now()
		bucket := &s3v1alpha1.S3Bucket{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "logs",
				Namespace:         "default",
				DeletionTimestamp: &now,
				Finalizers:        []string{s3BucketFinalizer},
				Annotations:       map[string]string{s3v1alpha1.AnnotationDrainBucket: "true"},
			},
			Status: s3v1alpha1.S3BucketStatus{BucketName: "logs"},
		}

		r := &S3BucketReconciler{
			Client:   fake.NewClientBuilder().WithScheme(scheme.Scheme).Build(),
			Scheme:   scheme.Scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		rctx := &bucketReconcileContext{Bucket: bucket}

		// Stand in for the post-drain-failure state: still terminating, still not empty.
		r.setCondition(bucket, s3v1alpha1.ConditionTypeDraining, metav1.ConditionFalse,
			"DrainFailed", "Drain could not be started")
		r.waitForDeletion(ctx, rctx, "BucketNotEmpty",
			"Bucket logs still contains 3 object(s).", 3*time.Minute)

		By("still scheduling a bounded requeue rather than relying on error backoff")
		Expect(rctx.RequeAfter.Duration).To(Equal(3 * time.Minute))

		By("surfacing the drain failure without claiming the reconcile failed")
		draining := meta.FindStatusCondition(bucket.Status.Conditions, s3v1alpha1.ConditionTypeDraining)
		Expect(draining).NotTo(BeNil())
		Expect(draining.Status).To(Equal(metav1.ConditionFalse))
		Expect(draining.Reason).To(Equal("DrainFailed"))
		Expect(meta.FindStatusCondition(bucket.Status.Conditions, s3v1alpha1.ConditionTypeReconcileSucceeded).Status).
			To(Equal(metav1.ConditionTrue))
	})
})

var _ = Describe("Waking the account when its tenant's buckets change", func() {
	ctx := context.Background()

	// Regression: a deleting S3Tenant's second guard reads the backend bucket count, which
	// lives in the account's status and is only refreshed by the account's own reconcile.
	// Nothing woke the account once the last bucket was gone, so the count stayed stale and
	// the tenant waited on a value that could never change until the next full resync -
	// verified against a live grid, where the grid reported zero buckets while the account
	// still reported one.
	accountIndex := func(obj client.Object) []string {
		account := obj.(*s3v1alpha1.S3TenantAccount)
		if account.Spec.S3TenantRef == nil {
			return nil
		}
		return []string{account.Spec.S3TenantRef.Namespace + "/" + account.Spec.S3TenantRef.Name}
	}

	newAccountReconciler := func(objs ...client.Object) *S3TenantAccountReconciler {
		return &S3TenantAccountReconciler{
			Client: fake.NewClientBuilder().
				WithScheme(scheme.Scheme).
				WithIndex(&s3v1alpha1.S3TenantAccount{}, accountByTenantIndex, accountIndex).
				WithObjects(objs...).
				Build(),
			Scheme:   scheme.Scheme,
			Recorder: record.NewFakeRecorder(10),
		}
	}

	boundAccount := func(name, tenantNs, tenantName string) *s3v1alpha1.S3TenantAccount {
		return &s3v1alpha1.S3TenantAccount{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: s3v1alpha1.S3TenantAccountSpec{
				S3TenantRef: &corev1.ObjectReference{Namespace: tenantNs, Name: tenantName},
			},
		}
	}

	It("enqueues the account bound to the bucket's tenant", func() {
		r := newAccountReconciler(
			boundAccount("acct-a", "default", "my-tenant"),
			boundAccount("acct-b", "default", "other-tenant"),
		)

		requests := r.mapBucketToAccount(ctx, bucketFor("logs", "my-tenant"))

		Expect(requests).To(HaveLen(1))
		Expect(requests[0].Name).To(Equal("acct-a"))
		Expect(requests[0].Namespace).To(BeEmpty(), "S3TenantAccount is cluster-scoped")
	})

	It("returns nothing when no account is bound to that tenant", func() {
		r := newAccountReconciler(boundAccount("acct-b", "default", "other-tenant"))
		Expect(r.mapBucketToAccount(ctx, bucketFor("logs", "my-tenant"))).To(BeEmpty())
	})

	It("ignores unbound accounts and non-bucket objects", func() {
		r := newAccountReconciler(&s3v1alpha1.S3TenantAccount{ObjectMeta: metav1.ObjectMeta{Name: "unbound"}})
		Expect(r.mapBucketToAccount(ctx, bucketFor("logs", "my-tenant"))).To(BeEmpty())
		Expect(r.mapBucketToAccount(ctx, &s3v1alpha1.S3Tenant{})).To(BeNil())
	})
})
