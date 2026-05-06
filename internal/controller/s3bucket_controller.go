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
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	"github.com/bedag/storagegrid-operator/pkg/grid"
	"github.com/bedag/storagegrid-operator/pkg/kube"
	"github.com/bedag/storagegrid-operator/pkg/s3"
)

// S3BucketReconciler reconciles a S3Bucket object.
type S3BucketReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

const (
	s3BucketFinalizer     string = "bucket.s3.bedag.ch/finalizer"
	managedByTagKey       string = "s3.bedag.ch/managed-by"
	managedByTagValue     string = "storagegrid-operator"
	bucketNamespaceTagKey string = "s3.bedag.ch/bucket-namespace"
	bucketNameTagKey      string = "s3.bedag.ch/bucket-name"
	bucketUIDTagKey       string = "s3.bedag.ch/bucket-uid"
)

// bucketReconcileContext holds all the context needed for bucket reconciliation.
type bucketReconcileContext struct {
	Bucket        *s3v1alpha1.S3Bucket
	S3Tenant      *s3v1alpha1.S3Tenant
	ObjectUpdated bool
	DoRequeue     bool
	// keep the backend bucketUsage to avoid multiple calls to the backend.
	// while making sure they are consistent within one reconciliation loop.
	BucketUsage *grid.BucketUsage

	// clients need to be unique per reconciliation loop.
	TenantClient *grid.TenantClient
	S3Client     *s3.S3Client
}

// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3buckets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3buckets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3buckets/finalizers,verbs=update
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenants,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop.
func (r *S3BucketReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("s3bucket", req.NamespacedName)
	log.V(1).Info("Starting reconciliation")

	// Initialize reconcile context.
	rctx := &bucketReconcileContext{
		Bucket:        &s3v1alpha1.S3Bucket{},
		S3Tenant:      &s3v1alpha1.S3Tenant{},
		ObjectUpdated: false,
		DoRequeue:     false,
	}

	// Fetch the S3Bucket instance.
	if err := r.Get(ctx, req.NamespacedName, rctx.Bucket); err != nil {
		log.Error(err, "Failed to get S3Bucket")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.V(1).Info("Successfully retrieved S3Bucket", "name", rctx.Bucket.Name)

	// Perform the main reconciliation.
	statusBase := rctx.Bucket.DeepCopy()
	err := r.doReconcile(ctx, rctx)

	// Use conditions to derive the readiness state.
	r.deriveReadiness(ctx, rctx)

	// update the annotations if they were updated.
	// needs to be done before the status is updated because we lose the annotations otherwise.
	if rctx.ObjectUpdated {
		// creating a deep copy of the status to avoid modifying the original object.
		statusCopy := rctx.Bucket.DeepCopy()

		log.V(1).Info("Annotations were updated, updating the object")
		if updateErr := r.Update(ctx, rctx.Bucket); updateErr != nil {
			log.Error(updateErr, "Failed to update annotations")
			if err != nil {
				err = fmt.Errorf("reconciliation failed: %w, failed to update annotations: %w", err, updateErr)
			} else {
				err = fmt.Errorf("failed to update annotations: %w", updateErr)
			}
		} else {
			log.V(1).Info("Annotations updated successfully")
		}

		rctx.Bucket.Status = statusCopy.Status // restore the status from the copy
	}

	// update the status of the account if it was updated during reconciliation.
	if updateErr := r.Status().Patch(ctx, rctx.Bucket, client.MergeFrom(statusBase)); updateErr != nil {
		log.Error(updateErr, "Failed to update status, requeuing")
		if err == nil {
			// no error occurred during reconciliation, but status update failed.
			rctx.DoRequeue = true // we need to requeue to ensure the status is updated
		}
		// if an error already occurred during reconciliation, we just return that error.
	}

	// Return with appropriate requeue interval for draining buckets
	if rctx.Bucket.Status.Phase == s3v1alpha1.BucketPhaseDraining {
		if rctx.Bucket.Status.DrainStatus != nil {
			log.V(1).Info("Bucket is draining, requeuing after interval", "interval", rctx.Bucket.Status.DrainStatus.NextPollInterval.Duration)
			return ctrl.Result{RequeueAfter: rctx.Bucket.Status.DrainStatus.NextPollInterval.Duration}, err
		}
		log.Error(fmt.Errorf("bucket phase is Draining but DrainStatus is nil"), "Inconsistent drain state")
	}

	log.V(1).Info("Reconciliation completed successfully")
	return ctrl.Result{Requeue: rctx.DoRequeue}, err
}

func (r *S3BucketReconciler) doReconcile(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "doReconcile")

	// Set initial phase if not set
	if rctx.Bucket.Status.Phase == "" {
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhasePending
	}

	// Fetch and validate S3Tenant.
	if err := r.reconcileS3TenantReference(ctx, rctx); err != nil {
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseFailed
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantNotFound", err.Error())
		r.setCondition(rctx.Bucket, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "TenantNotFound", err.Error())
		return err
	}

	// basic reconciliations
	r.reconcileSecretRefs(ctx, rctx)
	r.reconcileS3EndpointConfig(ctx, rctx)

	// Validate tenant readiness.
	if err := r.reconcileTenantReadiness(ctx, rctx); err != nil {
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhasePending
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantNotReady", err.Error())
		r.setCondition(rctx.Bucket, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "TenantNotReady", err.Error())
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketTenantNotReady,
			fmt.Sprintf("Waiting for S3Tenant %s to become ready (current phase: %s)", rctx.S3Tenant.Name, rctx.S3Tenant.Status.Phase))
		return err
	}

	// Initialize tenant client.
	if err := r.reconcileTenantClient(ctx, rctx); err != nil {
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseFailed
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantClientInitFailed", err.Error())
		r.setCondition(rctx.Bucket, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "TenantClientInitFailed", err.Error())
		return err
	}
	r.setCondition(rctx.Bucket, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionTrue, "TenantReady", "Tenant is ready for bucket operations")

	// Handle finalizer and deletion.
	if err := r.reconcileFinalizerAndDelete(ctx, rctx); err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "FinalizerError", err.Error())
		return err
	}
	if rctx.DoRequeue {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "ReconcileSucceeded", "Finalizer reconciled, requeuing")
		return nil
	}

	// Check if bucket has been created or imported.
	// Gate on ConditionTrue (not just existence) so that a failed creation attempt (ConditionFalse)
	// re-enters the creation path and allows the user to fix spec.bucketName.
	createdCondition := meta.FindStatusCondition(rctx.Bucket.Status.Conditions, s3v1alpha1.ConditionTypeCreated)

	if createdCondition == nil || createdCondition.Status != metav1.ConditionTrue {
		// make sure proper region is set before trying to create or import the bucket.
		if err := r.reconcileRegion(ctx, rctx); err != nil {
			return err
		}

		if rctx.Bucket.Annotations != nil {
			if bucketToImport := rctx.Bucket.Annotations[s3v1alpha1.AnnotationImportBucket]; bucketToImport != "" {
				// Reconcile import
				if err := r.reconcileImport(ctx, rctx, bucketToImport); err != nil {
					r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketImportFailed", err.Error())
					return err
				}

				// Import handles its own status updates, conditions, and requeue
				return nil
			}
		}

		// Reconcile bucket creation.
		if err := r.reconcileBucketCreation(ctx, rctx); err != nil {
			r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketCreationFailed", err.Error())
			return err
		}

		return nil
	}

	// Create admin user and group for bucket.
	if err := r.reconcileBucketAdmin(ctx, rctx); err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketAdminReconcileFailed", err.Error())
		return err
	}

	// Create S3 credentials for user.
	if err := r.reconcileBucketS3Credentials(ctx, rctx); err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketS3CredentialsReconcileFailed", err.Error())
		return err
	}

	// Reconcile bucket usage.
	if err := r.reconcileBucketUsage(ctx, rctx); err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketUsageReconcileFailed", err.Error())
		// Don't fail reconciliation for usage errors, just log.
	}

	// Reconcile bucket drain operations (requires bucket usage to be up-to-date).
	if err := r.reconcileDrain(ctx, rctx); err != nil {
		log.Error(err, "Failed to reconcile bucket drain")
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketDrainReconcileFailed", err.Error())
		// Return error to ensure drain issues are addressed.
		return err
	}

	if err := r.initS3ClientAsBucketAdmin(ctx, rctx); err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "S3ClientInitFailed", err.Error())
		return err
	}

	// make sure ownership tags are present.
	if err := r.reconcileOwnershipTags(ctx, rctx); err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketOwnershipTaggingFailed", err.Error())
		return err
	}

	// Reconcile bucket object-lock drift. Sends spec verbatim; StorageGRID rejects invalid transitions.
	if err := r.reconcileBucketObjectLock(ctx, rctx); err != nil {
		log.Error(err, "Failed to reconcile bucket object lock")
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketObjectLockReconcileFailed", err.Error())
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeConfigurationSynced, metav1.ConditionFalse, "BucketObjectLockReconcileFailed", err.Error())
		// Don't return error; continue with other reconciliation so policy and credentials can still progress.
	}

	// Reconcile bucket policy.
	if err := r.reconcileBucketPolicy(ctx, rctx); err != nil {
		log.Error(err, "Failed to reconcile bucket policy")
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketPolicyReconcileFailed", err.Error())
		// Don't return error, continue with other reconciliation.
	}

	// Reconcile bucket lifecycle management.
	if err := r.reconcileBucketLifecycle(ctx, rctx); err != nil {
		log.Error(err, "Failed to reconcile bucket lifecycle")
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketLifecycleReconcileFailed", err.Error())
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeLifecycleSynced, metav1.ConditionFalse, "BucketLifecycleReconcileFailed", err.Error())
		// Don't return error; continue so other reconcilers can still progress.
	}

	// Reconcile the operator-synthesized connection-details Secret. Non-fatal: any failure is
	// surfaced via condition + event but does not block the rest of reconciliation.
	if err := r.reconcileBucketConnectionDetails(ctx, rctx); err != nil {
		log.Error(err, "Failed to reconcile bucket connection details")
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "ConnectionDetailsReconcileFailed", err.Error())
	}

	// Reconciliation completed successfully.
	r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "ReconcileSucceeded", "Reconciliation completed successfully")

	return nil
}

func (r *S3BucketReconciler) reconcileSecretRefs(ctx context.Context, rctx *bucketReconcileContext) {
	log := log.FromContext(ctx).WithValues("function", "reconcileSecretRefs")
	log.V(1).Info("Starting secret references reconciliation")

	// if already specified we can just return as this is only set once.
	if rctx.Bucket.Status.S3AdminKeysSecretRef != nil && rctx.Bucket.Status.S3AdminKeysSecretRef.Name != "" {
		return
	}

	// secret name is generated as -<Bucket>-s3-admin-keypair.
	rctx.Bucket.Status.S3AdminKeysSecretRef = &corev1.LocalObjectReference{
		Name: fmt.Sprintf("s3bucket-%s-s3-admin-keypair", rctx.Bucket.Name),
	}

	log.V(1).Info("Secret references reconciliation completed")
}

func (r *S3BucketReconciler) reconcileFinalizerAndDelete(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileFinalizerAndDelete")

	if rctx.Bucket.DeletionTimestamp.IsZero() {
		// Object is not being deleted, ensure finalizer is present.
		if !controllerutil.ContainsFinalizer(rctx.Bucket, s3BucketFinalizer) {
			log.V(1).Info("Adding finalizer to S3Bucket")
			controllerutil.AddFinalizer(rctx.Bucket, s3BucketFinalizer)
			rctx.DoRequeue = true
			rctx.ObjectUpdated = true
			return nil
		}
	} else {
		// Object is being deleted.
		log.V(1).Info("S3Bucket is being deleted")
		if controllerutil.ContainsFinalizer(rctx.Bucket, s3BucketFinalizer) {
			log.V(1).Info("Finalizing S3Bucket")
			if err := r.finalize(ctx, rctx); err != nil {
				log.Error(err, "Failed to finalize S3Bucket")
				return err
			}

			log.V(1).Info("Removing finalizer from S3Bucket")
			controllerutil.RemoveFinalizer(rctx.Bucket, s3BucketFinalizer)
			rctx.DoRequeue = true // we need to requeue to ensure the finalizer is removed on the next reconciliation
			rctx.ObjectUpdated = true
			return nil
		}

		log.V(1).Info("Finalizer not present, deletion can proceed without further action")
	}

	return nil
}

func (r *S3BucketReconciler) reconcileS3EndpointConfig(ctx context.Context, rctx *bucketReconcileContext) {
	log := log.FromContext(ctx).WithValues("function", "reconcileS3EndpointConfig")

	if rctx.Bucket.Status.S3EndpointConfig != rctx.S3Tenant.Status.S3EndpointConfig {
		log.V(1).Info("Updating S3 endpoint config from tenant")
		rctx.Bucket.Status.S3EndpointConfig = rctx.S3Tenant.Status.S3EndpointConfig
	}
}

// resolveBucketTenantKey returns the ObjectKey of the S3Tenant referenced by
// a bucket. An empty Namespace in the ref means "the bucket's own namespace".
func resolveBucketTenantKey(bucket *s3v1alpha1.S3Bucket) client.ObjectKey {
	ns := bucket.Spec.S3TenantRef.Namespace
	if ns == "" {
		ns = bucket.Namespace
	}
	return client.ObjectKey{Name: bucket.Spec.S3TenantRef.Name, Namespace: ns}
}

func (r *S3BucketReconciler) reconcileS3TenantReference(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileS3TenantReference")

	rctx.S3Tenant = &s3v1alpha1.S3Tenant{}
	tenantKey := resolveBucketTenantKey(rctx.Bucket)
	log.V(1).Info("Resolved tenant lookup key", "namespace", tenantKey.Namespace, "name", tenantKey.Name)

	if err := r.Get(ctx, tenantKey, rctx.S3Tenant); err != nil {
		return fmt.Errorf("failed to get S3Tenant %s: %w", tenantKey, err)
	}

	log.V(1).Info("Successfully retrieved S3Tenant",
		"tenantName", rctx.S3Tenant.Name,
		"tenantPhase", rctx.S3Tenant.Status.Phase)

	return nil
}

func (r *S3BucketReconciler) reconcileTenantReadiness(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileTenantReadiness")

	if rctx.S3Tenant.Status.Phase != s3v1alpha1.PhaseBound {
		log.V(1).Info("Tenant is not ready",
			"currentPhase", rctx.S3Tenant.Status.Phase,
			"requiredPhase", s3v1alpha1.PhaseBound)
		return fmt.Errorf("tenant %s is not ready, current phase: %s", rctx.S3Tenant.Name, rctx.S3Tenant.Status.Phase)
	}

	log.V(1).Info("Tenant is ready for operations")
	return nil
}

func (r *S3BucketReconciler) reconcileTenantClient(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileTenantClient")

	// Fetch tenant admin credentials.
	username, password, err := kube.FetchCredentialsFromSecret(
		ctx,
		r.Client,
		rctx.S3Tenant.Status.AdminSecretRef.Namespace,
		rctx.S3Tenant.Status.AdminSecretRef.Name,
	)
	if err != nil {
		return fmt.Errorf("failed to fetch tenant credentials: %w", err)
	}

	log.V(1).Info("Successfully fetched tenant credentials")

	// Initialize tenant client.
	client, err := grid.InitTenantClient(username, password, rctx.S3Tenant.Status.GridEndpoint, rctx.S3Tenant.Status.TenantID)
	if err != nil {
		return fmt.Errorf("failed to initialize tenant client: %w", err)
	}

	rctx.TenantClient = client

	log.V(1).Info("Successfully initialized tenant client")
	return nil
}

func (r *S3BucketReconciler) reconcileBucketCreation(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketCreation")

	// Generate unique bucket name if not set.
	if err := r.reconcileBucketName(ctx, rctx.Bucket); err != nil {
		return err
	}

	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketCreating,
		fmt.Sprintf("Creating bucket %s in region %s", rctx.Bucket.Status.BucketName, rctx.Bucket.Status.Region))

	// Try to create bucket.
	err := grid.CreateBucket(ctx, rctx.Bucket.Status.BucketName, rctx.Bucket.Spec.Region, rctx.Bucket.Spec.S3ObjectLock, rctx.TenantClient)
	if err != nil {
		if strings.Contains(err.Error(), "BucketAlreadyExists") {
			r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionFalse, "BucketNameConflict", fmt.Sprintf("Bucket name %s is already taken, please choose a different name", rctx.Bucket.Status.BucketName))
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketCreateFailed,
				fmt.Sprintf("Failed to create bucket: Bucket name %s is already taken, please either change spec.bucketName or leave it empty for an automatic generated one", rctx.Bucket.Status.BucketName))

			// Clear the attempted name so reconcileBucketName re-evaluates from spec on next reconcile.
			// This allows the user to update spec.bucketName and retry.
			rctx.Bucket.Status.BucketName = ""

			// Mark reconciliation as succeeded so deriveReadiness keeps phase as Pending (not Failed).
			// The ConditionTypeCreated=False with reason BucketNameConflict provides the actionable info.
			r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "ReconcileSucceeded", "Bucket name conflict, waiting for user action")
			return nil
		}

		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionFalse, "BucketCreateReconcileFailed", err.Error())
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketCreateFailed,
			fmt.Sprintf("Failed to create bucket: %v", err))
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "BucketCreated", "Bucket created successfully")
	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketCreated,
		fmt.Sprintf("Successfully created bucket %s", rctx.Bucket.Status.BucketName))

	log.V(1).Info("Bucket created successfully", "bucketName", rctx.Bucket.Status.BucketName)

	log.V(1).Info("Bucket creation completed successfully")
	rctx.DoRequeue = true // Requeue for further processing
	return nil
}

// reconcileBucketObjectLock fetches the current object-lock configuration of the bucket and
// issues an UpdateObjectLock when it diverges from the spec. Sends spec verbatim; the StorageGRID
// backend rejects invalid transitions (e.g. disabling once enabled) — we surface those errors.
// Note: StorageGRID applies retention changes to NEW objects only; an event is emitted to make
// this explicit to operators.
func (r *S3BucketReconciler) reconcileBucketObjectLock(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketObjectLock")

	current, err := grid.GetBucketObjectLock(ctx, rctx.Bucket.Status.BucketName, rctx.TenantClient)
	if err != nil {
		return fmt.Errorf("failed to fetch current object lock settings: %w", err)
	}

	desired := grid.DesiredBucketObjectLock(rctx.Bucket.Spec.S3ObjectLock, current)

	if grid.ObjectLockSettingsEqual(current, desired) {
		log.V(1).Info("Bucket object lock already in sync")
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeConfigurationSynced, metav1.ConditionTrue, "ObjectLockInSync", "Bucket S3 Object Lock configuration matches spec")
		return nil
	}

	log.V(1).Info("Bucket object lock drift detected, updating", "bucketName", rctx.Bucket.Status.BucketName)

	if err := grid.UpdateBucketObjectLock(ctx, rctx.Bucket.Status.BucketName, desired, rctx.TenantClient); err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketObjectLockUpdateFailed,
			fmt.Sprintf("Failed to update S3 Object Lock for bucket %s: %v", rctx.Bucket.Status.BucketName, err))
		return fmt.Errorf("failed to update bucket object lock: %w", err)
	}

	r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketObjectLockUpdated,
		fmt.Sprintf("Updated S3 Object Lock configuration for bucket %s; the new default retention applies to NEW objects only — existing objects keep their prior retention", rctx.Bucket.Status.BucketName))
	return nil
}

func (r *S3BucketReconciler) reconcileImport(ctx context.Context, rctx *bucketReconcileContext, bucketToImport string) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileImport")

	log.V(1).Info("Starting import of bucket", "bucketToImport", bucketToImport)

	// Check if bucket already exists.
	exists, err := grid.BucketExists(ctx, bucketToImport, rctx.TenantClient)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		log.V(1).Info("Bucket does not exist, skipping import", "bucketName", bucketToImport)
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketImportFailed,
			fmt.Sprintf("Bucket %s does not exist in tenant %s, or is not available with given credentials, import aborted", rctx.Bucket.Status.BucketName, rctx.S3Tenant.Name))
		return nil
	}

	_, forceOwnership := rctx.Bucket.Annotations[s3v1alpha1.AnnotationForceBucketOwnership]
	if forceOwnership {
		log.V(1).Info("Force import ownership annotation found, ignoring current ownership tags during import")
	} else {
		// if ownership is to be checked, we need to use the tenant s3 client to check the tags.
		err := r.initS3ClientAsTenantAdmin(ctx, rctx)
		if err != nil {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketImportFailed,
				"Failed to initialize S3 client for tenant admin during import of bucket")
			return fmt.Errorf("failed to initialize S3 client for tenant admin during import of bucket %s: %w", bucketToImport, err)
		}

		// as reconcileOwnershipTags uses the name in status, we need to set it temporarily.
		rctx.Bucket.Status.BucketName = bucketToImport
		err = r.reconcileOwnershipTags(ctx, rctx)
		rctx.Bucket.Status.BucketName = "" // reset it back after the check.
		if err != nil {
			if strings.Contains(err.Error(), "Bucket is managed by another entity") {
				r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketImportFailed,
					fmt.Sprintf("Cannot import bucket %s as it is managed by another entity", bucketToImport))

				return fmt.Errorf("cannot import bucket %s as it is managed by another entity: %w", bucketToImport, err)
			}
			if strings.Contains(err.Error(), "NoSuchBucket") {
				r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketImportFailed,
					fmt.Sprintf("Cannot import bucket %s as it does not exist in tenant %s", bucketToImport, rctx.S3Tenant.Name))
				return fmt.Errorf("cannot import bucket %s as it does not exist: %w", bucketToImport, err)
			}
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketImportFailed,
				fmt.Sprintf("Failed to import bucket %s: %v", bucketToImport, err))
			return fmt.Errorf("failed to reconcile ownership tags during import of bucket %s: %w", bucketToImport, err)
		}

		// set s3client back to nil to ensure it's reinitialized as bucket admin later.
		rctx.S3Client = nil
	}

	// if we reach here, the bucket is not managed, we can proceed with the import.
	log.V(1).Info("Importing bucket", "bucketToImport", bucketToImport)

	// The import is as easy as setting the bucket name in status.
	// this will be used in the bucket creation reconciliation to check for existence.
	// Seems weird to do it here again after reconcileOwnership, but it keeps the logic clean.
	rctx.Bucket.Status.BucketName = bucketToImport

	// remove the annotations as it's no longer needed.
	delete(rctx.Bucket.Annotations, s3v1alpha1.AnnotationImportBucket)
	rctx.ObjectUpdated = true

	r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "BucketImported", fmt.Sprintf("Imported Bucket with name %s into state", bucketToImport))
	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketImported,
		fmt.Sprintf("Successfully imported bucket %s", rctx.Bucket.Status.BucketName))

	// we need to requeue to proceed with the bucket creation reconciliation.
	rctx.DoRequeue = true
	return nil
}

// reoncileOwnershipTags checks if the bucket is already managed by another entity.
// If it is managed by another entity, it returns an error.
// If it is not managed, it applies the ownership tags to the bucket.
func (r *S3BucketReconciler) reconcileOwnershipTags(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileOwnershipTags")

	if forceOwnership := rctx.Bucket.Annotations[s3v1alpha1.AnnotationForceBucketOwnership]; forceOwnership == "true" {
		log.V(1).Info("Force ownership annotation found, overriding existing ownership tags if present")
	} else {
		// check if bucket is not managed already.
		isManaged, err := r.isBucketManaged(ctx, rctx)
		if err != nil {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketOwnershipCheckFailed,
				"Failed to check bucket ownership")
			return fmt.Errorf("failed to check if bucket is managed: %w", err)
		}

		// if the bucket is managed, we check if it's managed by us.
		if isManaged {
			isOwned, err := r.isBucketOwnedByOperator(ctx, rctx)
			if err != nil {
				r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketOwnershipCheckFailed,
					"Failed to check bucket ownership")
				return fmt.Errorf("failed to check if bucket is owned by operator: %w", err)
			}

			// if the bucket is owned by us, we return nil.
			if isOwned {
				log.V(1).Info("Bucket is already managed and owned by operator, skipping further checks")
				return nil
			} else {
				log.V(1).Info("Bucket is managed by another entity, cannot import")

				// get current owner for proper error and event message.
				currentOwner, err := r.getCurrentBucketOwnershipTags(ctx, rctx)
				if err != nil {
					r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketImportFailed,
						"Failed to check bucket ownership")
					return fmt.Errorf("failed to get current bucket ownership tags: %w", err)
				}

				// convert to readable owner string.
				currentOwnerString := []string{}
				for key, value := range currentOwner {
					currentOwnerString = append(currentOwnerString, fmt.Sprintf("%s=%s", key, value))
				}

				r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketImportFailed,
					fmt.Sprintf("Bucket is managed by another entity (%s), cannot be owned", (strings.Join(currentOwnerString, ", "))))
				return fmt.Errorf("Bucket is managed by another entity (%s), cannot be owned", (strings.Join(currentOwnerString, ", ")))
			}
		}
	}

	// apply ownership tags to bucket.
	if err := r.applyBucketOwnershipTags(ctx, rctx); err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketOwnershipTaggingFailed,
			fmt.Sprintf("Failed to apply ownership tags to bucket %s", rctx.Bucket.Status.BucketName))
		return fmt.Errorf("failed to apply ownership tags to bucket %s: %w", rctx.Bucket.Status.BucketName, err)
	}

	delete(rctx.Bucket.Annotations, s3v1alpha1.AnnotationForceBucketOwnership)
	rctx.ObjectUpdated = true

	log.V(1).Info("Successfully applied ownership tags to bucket", "bucketName", rctx.Bucket.Status.BucketName)
	return nil
}

func (r *S3BucketReconciler) generateOwnershipTags(bucket *s3v1alpha1.S3Bucket) map[string]string {
	return map[string]string{
		managedByTagKey:       managedByTagValue,
		bucketNamespaceTagKey: bucket.Namespace,
		bucketNameTagKey:      bucket.Name,
		bucketUIDTagKey:       string(bucket.UID),
	}
}

func (r *S3BucketReconciler) applyBucketOwnershipTags(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "generateBucketOwnershipTags")

	// apply tags to bucket.
	if err := s3.AppendTagMap(ctx, rctx.Bucket.Status.BucketName, r.generateOwnershipTags(rctx.Bucket), rctx.S3Client); err != nil {
		log.Error(err, "Failed to apply ownership tags to bucket")
		return fmt.Errorf("failed to apply ownership tags to bucket: %w", err)
	}

	log.V(1).Info("Successfully applied ownership tags to bucket")
	return nil
}

func (r *S3BucketReconciler) getCurrentBucketOwnershipTags(ctx context.Context, rctx *bucketReconcileContext) (map[string]string, error) {
	log := log.FromContext(ctx).WithValues("function", "getCurrentBucketOwnershipTags")

	// fetch tags from bucket.
	tags, err := s3.GetBucketTagMap(ctx, rctx.Bucket.Status.BucketName, rctx.S3Client)
	if err != nil {
		log.Error(err, "Failed to fetch bucket tags")
		return nil, fmt.Errorf("failed to fetch bucket tags: %w", err)
	}

	log.V(1).Info("Successfully fetched bucket tags")
	return tags, nil
}

// Returns true if this bucket is owned by the operator, false otherwise.
// Only returns true if all ownership tags are present and correct.
func (r *S3BucketReconciler) isBucketOwnedByOperator(ctx context.Context, rctx *bucketReconcileContext) (bool, error) {
	log := log.FromContext(ctx).WithValues("function", "isBucketOwnedByOperator")

	// fetch tags from bucket.
	tags, err := r.getCurrentBucketOwnershipTags(ctx, rctx)
	if err != nil {
		log.Error(err, "Failed to fetch bucket tags")
		return false, fmt.Errorf("failed to fetch bucket tags: %w", err)
	}

	// check for ownership tags.
	ownershipTags := r.generateOwnershipTags(rctx.Bucket)
	for key, value := range ownershipTags {
		if tagValue, exists := tags[key]; !exists || tagValue != value {
			log.V(1).Info("Bucket is not owned by operator", "missingOrIncorrectTag", key)
			return false, nil
		}
	}

	log.V(1).Info("Bucket is owned by operator")
	return true, nil
}

// Returns true if this bucket has the managed-by tag set to storagegrid-operator.
// Returns false if the tag is missing
// Returns false with error if the tag is present but has an unexpected value.
func (r *S3BucketReconciler) isBucketManaged(ctx context.Context, rctx *bucketReconcileContext) (bool, error) {
	log := log.FromContext(ctx).WithValues("function", "isBucketManaged")

	// fetch tags from bucket.
	tags, err := s3.GetBucketTagMap(ctx, rctx.Bucket.Status.BucketName, rctx.S3Client)
	if err != nil {
		log.Error(err, "Failed to fetch bucket tags")
		return false, fmt.Errorf("failed to fetch bucket tags: %w", err)
	}

	// check for managed-by tag.
	if tagValue, exists := tags[managedByTagKey]; !exists {
		log.V(1).Info("Bucket is not managed by operator, managed-by tag missing")
		return false, nil
	} else if tagValue != managedByTagValue {
		log.V(1).Info("Bucket is not managed by operator, managed-by tag has an unexpected value", "tagValue", tagValue)
		return false, fmt.Errorf("bucket is not managed by operator, managed-by tag has an unexpected value: %s", tagValue)
	}

	log.V(1).Info("Bucket is managed by operator")
	return true, nil
}

func (r *S3BucketReconciler) reconcileRegion(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileRegion")

	region, err := r.getDesiredReqgion(ctx, rctx)
	if err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketRegionValidationFailed,
			err.Error())
		return err
	}

	// Validate specified region exists in tenant.
	if !slices.Contains(rctx.S3Tenant.Status.Regions, region) {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketRegionValidationFailed,
			fmt.Sprintf("Specified region %s does not exist in tenant (available: %v)", rctx.Bucket.Spec.Region, rctx.S3Tenant.Status.Regions))
		return fmt.Errorf("specified region %s does not exist in tenant", rctx.Bucket.Spec.Region)
	}

	log.V(1).Info("Using desired region", "region", region)
	rctx.Bucket.Status.Region = region

	return nil
}

func (r *S3BucketReconciler) getDesiredReqgion(ctx context.Context, rctx *bucketReconcileContext) (string, error) {
	log := log.FromContext(ctx).WithValues("function", "getDesiredRegion")

	if rctx.Bucket.Spec.Region != "" {
		log.V(1).Info("Using specified region from spec", "region", rctx.Bucket.Spec.Region)
		return rctx.Bucket.Spec.Region, nil
	}

	if rctx.S3Tenant.Status.DefaultBucketRegion != "" {
		log.V(1).Info("Using default region from tenant", "region", rctx.S3Tenant.Status.DefaultBucketRegion)
		return rctx.S3Tenant.Status.DefaultBucketRegion, nil
	}

	return "", fmt.Errorf("no region specified in spec and no default region set in tenant")
}

func (r *S3BucketReconciler) reconcileBucketAdmin(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketAdminUser")

	// we're using the bucket UID as unique identifier for the admin user.
	err := grid.CreateBucketAdminIfNotExists(ctx, rctx.Bucket.Status.BucketName, r.getBucketIdentifier(rctx.Bucket), rctx.TenantClient)
	if err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketAdminUserCreateFailed,
			fmt.Sprintf("Failed to create admin user: %v", err))
		return fmt.Errorf("failed to create admin user: %w", err)
	}

	log.V(1).Info("Admin user and group reconciled successfully")
	return nil
}

func (r *S3BucketReconciler) reconcileBucketS3Credentials(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketS3Credentials")

	// Annotation-driven recreation takes priority over all other paths.
	if rctx.Bucket.Annotations != nil && rctx.Bucket.Annotations[s3v1alpha1.AnnotationRecreateBucketKeypairs] == "true" {
		if err := r.createS3AdminKeypair(ctx, rctx, true); err != nil {
			return fmt.Errorf("failed to recreate S3 admin keypair: %w", err)
		}
		delete(rctx.Bucket.Annotations, s3v1alpha1.AnnotationRecreateBucketKeypairs)
		rctx.ObjectUpdated = true
		return nil
	}

	// Fresh bucket (no AccessKeyId tracked in status): always create a new
	// keypair and overwrite whatever secret may exist.
	if rctx.Bucket.Status.AccessKeyId == "" {
		log.V(1).Info("Fresh bucket, creating S3 admin keypair (overwrites any stale secret pending GC)")
		return r.createS3AdminKeypair(ctx, rctx, false)
	}

	// If the secret is missing for an existing bucket, recover by recreating
	// credentials on the grid and rewriting the secret.
	if _, _, err := kube.FetchKeyPairFromSecret(ctx, r.Client, rctx.Bucket.Namespace, rctx.Bucket.Status.S3AdminKeysSecretRef.Name); err != nil {
		log.Info("S3 credentials secret missing for existing bucket, recreating",
			"secret", rctx.Bucket.Status.S3AdminKeysSecretRef.Name, "error", err.Error())
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketCredentialsSecretMissing,
			fmt.Sprintf("Secret %s missing for bucket %s, recreating credentials",
				rctx.Bucket.Status.S3AdminKeysSecretRef.Name, rctx.Bucket.Status.BucketName))
		return r.createS3AdminKeypair(ctx, rctx, true)
	}

	return nil
}

func (r *S3BucketReconciler) createS3AdminKeypair(ctx context.Context, rctx *bucketReconcileContext, recreate bool) (err error) {
	log := log.FromContext(ctx)

	// initialize variables for access key ID, access key, and secret key.
	accessKeyId, accessKey, secretKey := "", "", ""

	if recreate {
		log.V(1).Info("Recreating S3 keypair")
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketCredentialsRotated,
			fmt.Sprintf("Rotating S3 credentials for bucket %s", rctx.Bucket.Status.BucketName))

		// recreate the S3 admin keypair with existing access key ID.
		accessKeyId, accessKey, secretKey, err = grid.RecreateS3Credentials(ctx, rctx.Bucket.Status.BucketName, r.getBucketIdentifier(rctx.Bucket), rctx.Bucket.Status.AccessKeyId, rctx.TenantClient)
		if err != nil {
			log.Error(err, "Failed to recreate S3 admin keypair")
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketCredentialsRotationFailed,
				fmt.Sprintf("Failed to rotate credentials: %v", err))
			return err
		}
	} else {
		log.V(1).Info("Creating initial S3 keypair")
		// create s3 keys.
		accessKeyId, accessKey, secretKey, err = grid.CreateS3Credentials(ctx, rctx.Bucket.Status.BucketName, r.getBucketIdentifier(rctx.Bucket), rctx.TenantClient)
		if err != nil {
			log.Error(err, "Failed to create s3 keys")
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketCredentialsRotationFailed,
				fmt.Sprintf("Failed to create S3 credentials: %v", err))
			return err
		}
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketCredentialsCreated,
			fmt.Sprintf("Created S3 credentials for bucket %s", rctx.Bucket.Status.BucketName))
	}

	// store s3 keys in a secret.
	// Update secret with new credentials.
	err = kube.CreateKeyPairSecret(ctx, r.Client, rctx.Bucket.Namespace, rctx.Bucket.Status.S3AdminKeysSecretRef.Name, accessKey, secretKey, rctx.Bucket, rctx.Bucket.Kind)
	if err != nil {
		return fmt.Errorf("failed to update credential secret: %w", err)
	}

	rctx.Bucket.Status.AccessKeyId = accessKeyId

	return nil
}

func (r *S3BucketReconciler) reconcileBucketUsage(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketUsage")

	// Fetch bucket usage from API.
	usage, err := grid.FetchBucketUsage(ctx, rctx.Bucket.Status.BucketName, rctx.TenantClient)
	if err != nil {
		log.Error(err, "Failed to fetch bucket usage")
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketUsageFetchFailed,
			fmt.Sprintf("Failed to fetch usage metrics: %v", err))
		return fmt.Errorf("failed to fetch bucket usage: %w", err)
	}

	rctx.BucketUsage = usage

	// Update status with usage information.
	rctx.Bucket.Status.BucketUsage.ObjectCount = grid.GetBucketObjectCount(rctx.BucketUsage)
	rctx.Bucket.Status.BucketUsage.Bytes = kube.ParseBytes(grid.GetBucketUsedBytes(rctx.BucketUsage))

	log.V(1).Info("Bucket usage updated",
		"objectCount", rctx.Bucket.Status.BucketUsage.ObjectCount,
		"bytes", rctx.Bucket.Status.BucketUsage.Bytes)

	return nil
}

func (r *S3BucketReconciler) initS3Client(ctx context.Context, rctx *bucketReconcileContext, accessKey string, secretKey string) error {
	log := log.FromContext(ctx).WithValues("function", "initS3Client")

	// Resolve which S3 endpoint to use
	endpointConfig, tenantClassName, err := r.resolveS3Endpoint(ctx, rctx)
	if err != nil {
		return err
	}

	endpointURL := endpointConfig.URL
	s3client, err := s3.InitS3Client(ctx, endpointURL, accessKey, secretKey, rctx.Bucket.Status.Region, *endpointConfig.PathStyleAccess)
	if err != nil {
		log.Error(err, "Failed to initialize S3 client")
		r.emitEvent(rctx, corev1.EventTypeWarning, EventS3EndpointConnectionFailed,
			fmt.Sprintf("Failed to connect to S3 endpoint %s (%s): %v (check network access)", endpointURL, tenantClassName, err))
		return fmt.Errorf("failed to initialize S3 client: %w", err)
	}

	rctx.S3Client = s3client

	log.V(1).Info("S3 client initialized successfully", "endpoint", endpointURL, "source", tenantClassName)
	return nil
}

func (r *S3BucketReconciler) initS3ClientAsBucketAdmin(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "initS3ClientAsBucketAdmin")

	log.V(1).Info("Fetching S3 credentials for bucket admin")

	// Get S3 credentials for client initialization.
	accessKey, secretKey, err := kube.FetchKeyPairFromSecret(ctx, r.Client, rctx.Bucket.Namespace, rctx.Bucket.Status.S3AdminKeysSecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch S3 credentials")
		return fmt.Errorf("failed to fetch S3 credentials: %w", err)
	}

	return r.initS3Client(ctx, rctx, accessKey, secretKey)
}

func (r *S3BucketReconciler) initS3ClientAsTenantAdmin(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "initS3ClientAsTenantAdmin")

	log.V(1).Info("Fetching S3 credentials for tenant admin")

	// Get S3 credentials for client initialization.
	accessKey, secretKey, err := kube.FetchKeyPairFromSecret(ctx, r.Client, rctx.S3Tenant.Status.S3AdminKeysSecretRef.Namespace, rctx.S3Tenant.Status.S3AdminKeysSecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch S3 credentials")
		return fmt.Errorf("failed to fetch S3 credentials: %w", err)
	}

	return r.initS3Client(ctx, rctx, accessKey, secretKey)
}

// resolveS3Endpoint determines which S3 endpoint configuration to use for bucket operations.
// Returns the endpoint config and the name of the tenantclass used as source.
// Priority: StorageGrid.S3OperationsTenantClass > Bucket's tenant endpoint.
func (r *S3BucketReconciler) resolveS3Endpoint(ctx context.Context, rctx *bucketReconcileContext) (*s3v1alpha1.S3EndpointConfig, string, error) {
	log := log.FromContext(ctx).WithValues("function", "resolveS3Endpoint")

	// Fetch StorageGrid to check for S3OperationsTenantClass.
	storageGrid := &s3v1alpha1.StorageGrid{}
	sgName := rctx.S3Tenant.Spec.StorageGridRef.Name
	if err := r.Get(ctx, client.ObjectKey{Name: sgName}, storageGrid); err != nil {
		log.Error(err, "Failed to fetch StorageGrid for S3 endpoint resolution")
		return nil, "", fmt.Errorf("failed to fetch StorageGrid: %w", err)
	}

	// Option 1: Use grid-wide S3OperationsTenantClass if configured.
	if storageGrid.Spec.S3OperationsTenantClass != "" {
		tenantClass := &s3v1alpha1.S3TenantClass{}
		if err := r.Get(ctx, client.ObjectKey{Name: storageGrid.Spec.S3OperationsTenantClass}, tenantClass); err != nil {
			log.Error(err, "Failed to fetch S3OperationsTenantClass")
			return nil, "", fmt.Errorf("failed to fetch S3OperationsTenantClass %s: %w", storageGrid.Spec.S3OperationsTenantClass, err)
		}

		tenantClassName := storageGrid.Spec.S3OperationsTenantClass
		log.V(1).Info("Using S3OperationsTenantClass endpoint for S3 operations", "tenantClass", storageGrid.Spec.S3OperationsTenantClass)

		if len(tenantClass.Status.S3EndpointConfig.Addresses) == 0 {
			return nil, "", fmt.Errorf("S3OperationsTenantClass %s has no endpoint addresses configured", storageGrid.Spec.S3OperationsTenantClass)
		}

		return tenantClass.Status.S3EndpointConfig, tenantClassName, nil
	}

	// Option 2: Use bucket's tenant-specific endpoint (default).
	if rctx.S3Tenant.Status.S3EndpointConfig == nil {
		return nil, "", fmt.Errorf("tenant %s has no S3EndpointConfig in status", rctx.S3Tenant.Name)
	}
	tenantClassName := rctx.S3Tenant.Status.S3EndpointConfig.S3TenantClassName
	log.V(1).Info("Using tenantclass from bucket's tenant for S3 operations", "tenantClass", tenantClassName)

	if rctx.Bucket.Status.S3EndpointConfig == nil || len(rctx.Bucket.Status.S3EndpointConfig.Addresses) == 0 {
		return nil, "", fmt.Errorf("no S3 endpoint configuration available from %s", tenantClassName)
	}

	return rctx.Bucket.Status.S3EndpointConfig, tenantClassName, nil
}

func (r *S3BucketReconciler) reconcileBucketPolicy(ctx context.Context, rctx *bucketReconcileContext) error {
	// Handle policy reconciliation.
	if rctx.Bucket.Spec.BucketPolicyJson != "" {
		return r.reconcileBucketPolicyApply(ctx, rctx)
	} else {
		return r.reconcileBucketPolicyRemove(ctx, rctx)
	}
}

func (r *S3BucketReconciler) reconcileBucketPolicyApply(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketPolicyApply")

	// Get current policy from S3 API.
	currentPolicy, err := s3.GetPolicy(ctx, rctx.Bucket.Status.BucketName, rctx.S3Client)
	if err != nil {
		return fmt.Errorf("failed to get current policy: %w", err)
	}

	// Apply policy if it differs.
	if rctx.Bucket.Spec.BucketPolicyJson != currentPolicy {
		log.V(1).Info("Applying bucket policy")
		err = s3.ApplyPolicy(ctx, rctx.Bucket.Status.BucketName, rctx.Bucket.Spec.BucketPolicyJson, rctx.S3Client)
		if err != nil {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketPolicyApplyFailed,
				fmt.Sprintf("Failed to apply bucket policy: %v", err))
			return fmt.Errorf("failed to apply policy: %w", err)
		}

		rctx.Bucket.Status.LastAppliedPolicy = rctx.Bucket.Spec.BucketPolicyJson
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "PolicyApplied", "Bucket policy applied successfully")
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketPolicyApplied,
			"Successfully applied bucket policy")
		log.V(1).Info("Bucket policy applied successfully")
	} else {
		log.V(1).Info("Bucket policy already applied, no changes needed")
	}

	return nil
}

func (r *S3BucketReconciler) reconcileBucketPolicyRemove(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketPolicyRemove")

	// Remove policy if one was previously applied.
	if rctx.Bucket.Status.LastAppliedPolicy != "" {
		log.V(1).Info("Removing bucket policy")
		err := s3.DeletePolicy(ctx, rctx.Bucket.Status.BucketName, rctx.S3Client)
		if err != nil {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketPolicyRemoveFailed,
				fmt.Sprintf("Failed to remove bucket policy: %v", err))
			return fmt.Errorf("failed to delete policy: %w", err)
		}

		rctx.Bucket.Status.LastAppliedPolicy = ""
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "PolicyDeleted", "Bucket policy removed successfully")
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketPolicyRemoved,
			"Successfully removed bucket policy")
		log.V(1).Info("Bucket policy removed successfully")
	}

	return nil
}

// reconcileBucketLifecycle drives the operator-managed S3 lifecycle configuration on the bucket.
// It mirrors reconcileBucketPolicy: dispatches to apply or remove based on whether the spec
// describes any rules, and tracks the last successfully applied configuration via a JSON
// fingerprint stored in status.LastAppliedLifecycle for drift detection.
func (r *S3BucketReconciler) reconcileBucketLifecycle(ctx context.Context, rctx *bucketReconcileContext) error {
	desired := lifecycleSpecFromBucket(rctx.Bucket)
	if desired.IsEmpty() {
		return r.reconcileBucketLifecycleRemove(ctx, rctx)
	}
	return r.reconcileBucketLifecycleApply(ctx, rctx, desired)
}

func (r *S3BucketReconciler) reconcileBucketLifecycleApply(ctx context.Context, rctx *bucketReconcileContext, desired s3.LifecycleSpec) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketLifecycleApply")

	// Warn the user when lifecycle expiration is shorter than the bucket's configured object-lock retention.
	// We do not block the apply: the grid will reject deletes of locked objects when the lifecycle fires.
	r.warnLifecycleObjectLockConflict(rctx, desired)

	desiredCfg := s3.BuildLifecycleConfiguration(desired)
	desiredFingerprint := s3.LifecycleFingerprint(desiredCfg)

	// Always observe the backend before acting: status.LastAppliedLifecycle is diagnostic only,
	// it must not gate work, otherwise out-of-band drift (manual aws CLI changes, restores,
	// other tooling) would be silently masked.
	currentCfg, err := s3.GetLifecycle(ctx, rctx.Bucket.Status.BucketName, rctx.S3Client)
	if err != nil {
		return fmt.Errorf("failed to get current lifecycle configuration: %w", err)
	}

	if s3.LifecycleFingerprint(currentCfg) == desiredFingerprint {
		log.V(1).Info("Lifecycle configuration already in sync, no changes needed")
		rctx.Bucket.Status.LastAppliedLifecycle = desiredFingerprint
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeLifecycleSynced, metav1.ConditionTrue, "LifecycleApplied", "Bucket lifecycle configuration in sync")
		return nil
	}

	log.V(1).Info("Applying bucket lifecycle configuration", "expirationInDays", desired.ExpirationInDays)
	if err := s3.PutLifecycle(ctx, rctx.Bucket.Status.BucketName, desiredCfg, rctx.S3Client); err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketLifecycleApplyFailed,
			fmt.Sprintf("Failed to apply bucket lifecycle configuration: %v", err))
		return fmt.Errorf("failed to apply lifecycle: %w", err)
	}

	rctx.Bucket.Status.LastAppliedLifecycle = desiredFingerprint
	r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeLifecycleSynced, metav1.ConditionTrue, "LifecycleApplied", "Bucket lifecycle configuration applied successfully")
	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketLifecycleApplied,
		fmt.Sprintf("Successfully applied bucket lifecycle configuration (expirationInDays=%d)", desired.ExpirationInDays))
	log.V(1).Info("Bucket lifecycle configuration applied successfully")
	return nil
}

func (r *S3BucketReconciler) reconcileBucketLifecycleRemove(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketLifecycleRemove")

	// Fast path: if we never applied a lifecycle configuration, there's nothing to remove.
	// This trades strict drift-correction (out-of-band rules added directly to the backend won't
	// be reconciled away) for a cheaper steady state on buckets that don't use lifecycle at all.
	if rctx.Bucket.Status.LastAppliedLifecycle == "" {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeLifecycleSynced, metav1.ConditionTrue, "LifecycleDisabled", "Bucket lifecycle management disabled")
		return nil
	}

	log.V(1).Info("Removing bucket lifecycle configuration")
	if err := s3.DeleteLifecycle(ctx, rctx.Bucket.Status.BucketName, rctx.S3Client); err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketLifecycleRemoveFailed,
			fmt.Sprintf("Failed to remove bucket lifecycle configuration: %v", err))
		return fmt.Errorf("failed to delete lifecycle: %w", err)
	}

	rctx.Bucket.Status.LastAppliedLifecycle = ""
	r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeLifecycleSynced, metav1.ConditionTrue, "LifecycleDisabled", "Bucket lifecycle management disabled")
	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketLifecycleRemoved,
		"Successfully removed bucket lifecycle configuration")
	return nil
}

// lifecycleSpecFromBucket extracts the operator-level lifecycle spec from the bucket CR.
// Treats a nil pointer the same as an explicit {expirationInDays: 0} (disabled).
func lifecycleSpecFromBucket(bucket *s3v1alpha1.S3Bucket) s3.LifecycleSpec {
	if bucket.Spec.LifecycleManagement == nil {
		return s3.LifecycleSpec{}
	}
	return s3.LifecycleSpec{
		ExpirationInDays: bucket.Spec.LifecycleManagement.ExpirationInDays,
	}
}

// warnLifecycleObjectLockConflict emits a warning event when the configured lifecycle
// expiration is shorter than the bucket's object-lock retention. The grid will refuse to
// delete locked objects when the lifecycle fires; we surface this so users notice early.
func (r *S3BucketReconciler) warnLifecycleObjectLockConflict(rctx *bucketReconcileContext, desired s3.LifecycleSpec) {
	lock := rctx.Bucket.Spec.S3ObjectLock
	if lock == nil || lock.Mode == s3v1alpha1.S3ObjectLockModeDisabled {
		return
	}
	if desired.ExpirationInDays > 0 && desired.ExpirationInDays < lock.RetentionInDays {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketLifecycleObjectLockConflict,
			fmt.Sprintf("Lifecycle expirationInDays=%d is shorter than s3ObjectLock.retentionInDays=%d; StorageGRID will refuse to delete locked objects when the lifecycle fires",
				desired.ExpirationInDays, lock.RetentionInDays))
	}
}

// reconcileBucketConnectionDetails synthesizes (or removes) a Kubernetes Secret containing
// the AWS-style connection details for this bucket, owned by the S3Bucket CR.
func (r *S3BucketReconciler) reconcileBucketConnectionDetails(ctx context.Context, rctx *bucketReconcileContext) error {
	mode := s3v1alpha1.ConnectionDetailsModeAll
	if rctx.Bucket.Spec.ConnectionDetails != nil && rctx.Bucket.Spec.ConnectionDetails.Mode != "" {
		mode = rctx.Bucket.Spec.ConnectionDetails.Mode
	}
	if mode == s3v1alpha1.ConnectionDetailsModeDisabled {
		return r.reconcileBucketConnectionDetailsRemove(ctx, rctx)
	}
	return r.reconcileBucketConnectionDetailsApply(ctx, rctx)
}

// connectionDetailsSecretName returns the user-supplied destination secret name when set,
// otherwise the default `s3bucket-<bucket>-connection-details`. The kind prefix avoids
// collisions with S3Access default names that would otherwise share the namespace.
func bucketConnectionDetailsSecretName(bucket *s3v1alpha1.S3Bucket) string {
	if bucket.Spec.ConnectionDetails != nil && bucket.Spec.ConnectionDetails.DestinationSecret != "" {
		return bucket.Spec.ConnectionDetails.DestinationSecret
	}
	return fmt.Sprintf("s3bucket-%s-connection-details", bucket.Name)
}

func (r *S3BucketReconciler) reconcileBucketConnectionDetailsApply(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketConnectionDetailsApply")

	// Need the admin keypair, the resolved endpoint URL, region and bucket name. Bail out (without
	// flipping the condition to False) when prerequisites aren't ready yet — earlier reconcile steps
	// will fill them in and we'll be requeued.
	if rctx.Bucket.Status.S3AdminKeysSecretRef == nil || rctx.Bucket.Status.S3AdminKeysSecretRef.Name == "" {
		log.V(1).Info("Skipping connection details: admin keypair secret not yet known")
		return nil
	}
	if rctx.Bucket.Status.BucketName == "" {
		log.V(1).Info("Skipping connection details: bucket name not yet known")
		return nil
	}
	if rctx.Bucket.Status.S3EndpointConfig == nil || rctx.Bucket.Status.S3EndpointConfig.URL == "" {
		log.V(1).Info("Skipping connection details: endpoint URL not yet known")
		return nil
	}

	accessKey, secretKey, err := kube.FetchKeyPairFromSecret(ctx, r.Client, rctx.Bucket.Namespace, rctx.Bucket.Status.S3AdminKeysSecretRef.Name)
	if err != nil {
		return fmt.Errorf("failed to read bucket admin keypair: %w", err)
	}

	inputs := kube.ConnectionDetailsInputs{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		EndpointURL:     rctx.Bucket.Status.S3EndpointConfig.URL,
		Region:          rctx.Bucket.Status.Region,
		BucketName:      rctx.Bucket.Status.BucketName,
	}

	data := kube.BuildConnectionDetailsData(inputs)
	desiredSecretName := bucketConnectionDetailsSecretName(rctx.Bucket)

	// Detect a destinationSecret rename
	previousSecretName := ""
	if rctx.Bucket.Status.ConnectionDetailsSecretRef != nil {
		previousSecretName = rctx.Bucket.Status.ConnectionDetailsSecretRef.Name
	}
	renamed := previousSecretName != "" && previousSecretName != desiredSecretName

	changed, err := kube.ReconcileOwnedSecret(ctx, r.Client, rctx.Bucket.Namespace, desiredSecretName, data, rctx.Bucket)
	if err != nil {
		if errors.Is(err, kube.ErrSecretNotOwned) {
			msg := fmt.Sprintf("Secret %s/%s exists and is not owned by this S3Bucket; refusing to overwrite. Set spec.connectionDetails.destinationSecret to a different name or remove the foreign secret.",
				rctx.Bucket.Namespace, desiredSecretName)
			r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeOwnershipConflict, metav1.ConditionTrue, "ConnectionDetailsConflict", msg)
			r.emitEvent(rctx, corev1.EventTypeWarning, EventOwnershipConflict, msg)
			// Non-fatal: user must resolve. Don't propagate as error.
			return nil
		}
		r.emitEvent(rctx, corev1.EventTypeWarning, EventConnectionDetailsApplyFailed,
			fmt.Sprintf("Failed to apply connection details secret %s: %v", desiredSecretName, err))
		return fmt.Errorf("failed to reconcile connection details secret: %w", err)
	}

	// Rename: drop the old owned Secret only after the new one is durably written. We
	// only ever delete Secrets we own, so foreign Secrets are left alone.
	if renamed {
		if _, derr := kube.DeleteOwnedSecret(ctx, r.Client, rctx.Bucket.Namespace, previousSecretName, rctx.Bucket); derr != nil {
			log.Error(derr, "Failed to delete previous connection-details secret after rename; it can be cleaned up manually", "secret", previousSecretName)
		} else {
			r.emitEvent(rctx, corev1.EventTypeWarning, "ConnectionDetailsSecretRenamed",
				fmt.Sprintf("Connection-details secret renamed from %s to %s \u2014 workloads referencing the old name must be updated", previousSecretName, desiredSecretName))
		}
	}

	rctx.Bucket.Status.ConnectionDetailsSecretRef = &corev1.LocalObjectReference{Name: desiredSecretName}
	if changed {
		r.emitEvent(rctx, corev1.EventTypeNormal, EventConnectionDetailsApplied,
			fmt.Sprintf("Successfully applied connection details secret %s", desiredSecretName))
	}
	return nil
}

func (r *S3BucketReconciler) reconcileBucketConnectionDetailsRemove(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketConnectionDetailsRemove")

	// Fast path: nothing to remove if we never wrote one.
	if rctx.Bucket.Status.ConnectionDetailsSecretRef == nil {
		return nil
	}

	secretName := rctx.Bucket.Status.ConnectionDetailsSecretRef.Name
	deleted, err := kube.DeleteOwnedSecret(ctx, r.Client, rctx.Bucket.Namespace, secretName, rctx.Bucket)
	if err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventConnectionDetailsRemoveFailed,
			fmt.Sprintf("Failed to remove connection details secret %s: %v", secretName, err))
		return fmt.Errorf("failed to delete connection details secret: %w", err)
	}

	rctx.Bucket.Status.ConnectionDetailsSecretRef = nil
	if deleted {
		r.emitEvent(rctx, corev1.EventTypeNormal, EventConnectionDetailsRemoved,
			fmt.Sprintf("Successfully removed connection details secret %s", secretName))
	}
	log.V(1).Info("Connection details remove reconciliation completed", "deleted", deleted)
	return nil
}

func (r *S3BucketReconciler) finalize(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "finalize")
	log.V(1).Info("Finalizing S3Bucket")

	// Set phase to Deleting
	rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseDeleting

	if err := r.reconcileBucketUsage(ctx, rctx); err != nil {
		log.Error(err, "Failed to reconcile bucket usage during finalization")
		// Continue with finalization even if usage reconciliation fails.
	}

	// if bucket still has objects, we cannot delete it.
	if rctx.Bucket.Status.BucketUsage.ObjectCount > 0 {
		err := fmt.Errorf("bucket %s still has %d objects, cannot delete. Delete objects first or drain bucket using the drain-bucket-force annotation", rctx.Bucket.Status.BucketName, rctx.Bucket.Status.BucketUsage.ObjectCount)
		log.Error(err, "Bucket not empty, cannot finalize")
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketNotEmpty,
			fmt.Sprintf("Cannot delete bucket %s: still contains %d objects", rctx.Bucket.Status.BucketName, rctx.Bucket.Status.BucketUsage.ObjectCount))
		return err
	}

	// If no AccessKeyId, the bucket was never fully created.
	if rctx.Bucket.Status.AccessKeyId == "" {
		log.V(1).Info("Bucket was never fully created, nothing to clean up")
		return nil
	}

	// Delete group and user from tenant.
	if err := grid.DeleteBucketAdmin(ctx, r.getBucketIdentifier(rctx.Bucket), rctx.TenantClient); err != nil {
		log.Error(err, "Failed to delete bucket admin user and group")
		return fmt.Errorf("failed to delete bucket admin user and group %w", err)
	}

	// Delete bucket from tenant.
	// TODO: test if we need to drain the bucket first.
	if rctx.Bucket.Status.BucketName != "" {
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketDeleting,
			fmt.Sprintf("Deleting bucket %s", rctx.Bucket.Status.BucketName))

		if err := grid.DeleteBucket(ctx, rctx.Bucket.Status.BucketName, rctx.TenantClient); err != nil {
			log.Error(err, "Failed to delete bucket", "bucketName", rctx.Bucket.Status.BucketName)
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketDeleteFailed,
				fmt.Sprintf("Failed to delete bucket: %v", err))
			return fmt.Errorf("failed to delete bucket %s: %w", rctx.Bucket.Status.BucketName, err)
		}
		log.V(1).Info("Bucket deleted successfully", "bucketName", rctx.Bucket.Status.BucketName)
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketDeleted,
			fmt.Sprintf("Successfully deleted bucket %s", rctx.Bucket.Status.BucketName))
	}

	log.V(1).Info("Finalization completed successfully")
	return nil
}

// reconcileDrain is the main drain state machine that handles bucket draining operations.
// It checks for the drain annotation and manages transitions between drain states.
func (r *S3BucketReconciler) reconcileDrain(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileDrain")

	// Get current backend drain status
	backendStatus, err := grid.GetBucketDrainStatus(ctx, rctx.Bucket.Status.BucketName, rctx.TenantClient)
	if err != nil {
		log.Error(err, "Failed to check backend drain status")
		return fmt.Errorf("failed to check backend drain status: %w", err)
	}

	// Check for orphaned drain (backend draining but no StartedAt in our status)
	if backendStatus.IsDeletingObjects &&
		(rctx.Bucket.Status.DrainStatus == nil || rctx.Bucket.Status.DrainStatus.StartedAt == nil) {
		return r.cancelOrphanedDrain(ctx, rctx)
	}

	// Check if user wants to drain (annotation present)
	_, wantsDrain := rctx.Bucket.Annotations[s3v1alpha1.AnnotationDrainBucket]

	// Check if bucket is currently draining
	isDraining := rctx.Bucket.Status.DrainStatus != nil

	// State machine transitions
	switch {
	// we want to drain and it isn't draining yet, and there are objects to delete.
	case wantsDrain && !isDraining && rctx.Bucket.Status.BucketUsage.ObjectCount > 0:
		// START: User added annotation, bucket has objects, not draining yet
		return r.initiateDrain(ctx, rctx)

		// we are draining, we want to drain, and there are still objects to delete.
	case isDraining && wantsDrain && rctx.Bucket.Status.BucketUsage.ObjectCount > 0:
		// POLLING: Active drain with objects remaining
		return r.pollDrainProgress(ctx, rctx, backendStatus)

		// we are draining, but user removed the annotation.
	case isDraining && !wantsDrain:
		// CANCEL: User removed annotation while draining
		return r.cancelDrain(ctx, rctx)

		// we are draining, no objects remain.
	case isDraining && rctx.Bucket.Status.BucketUsage.ObjectCount == 0:
		// COMPLETE: Drain finished successfully
		return r.completeDrain(ctx, rctx)
	}

	return nil
}

// initiateDrain starts a new drain operation on the bucket.
func (r *S3BucketReconciler) initiateDrain(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "initiateDrain")
	log.Info("Initiating bucket drain", "bucket", rctx.Bucket.Status.BucketName)

	// Call backend to start drain
	if err := grid.DrainBucket(ctx, rctx.Bucket.Status.BucketName, rctx.TenantClient); err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketDrainFailed,
			fmt.Sprintf("Failed to start drain: %v", err))
		return fmt.Errorf("failed to initiate drain: %w", err)
	}

	// Get status after initiation
	status, err := grid.GetBucketDrainStatus(ctx, rctx.Bucket.Status.BucketName, rctx.TenantClient)
	if err != nil {
		return fmt.Errorf("failed to get drain status after initiation: %w", err)
	}

	now := metav1.Now()
	rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseDraining

	// Compute initial poll interval (respects current spec + grid config)
	nextPollInterval := r.computeNextPollInterval(ctx, rctx, time.Duration(0))

	rctx.Bucket.Status.DrainStatus = &s3v1alpha1.BucketDrainStatus{
		StartedAt:           &now,
		IsDeletingObjects:   status.IsDeletingObjects,
		InitialObjectCount:  status.InitialObjectCount,
		InitialObjectBytes:  status.InitialObjectBytes,
		LastCheckedAt:       &now,
		LastProgressAt:      &now,
		PreviousObjectCount: int64(rctx.Bucket.Status.BucketUsage.ObjectCount),
		Message:             fmt.Sprintf("Drain started: %d objects to delete", status.InitialObjectCount),
		NextPollInterval:    metav1.Duration{Duration: nextPollInterval},
	}

	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketDrainingStarted,
		fmt.Sprintf("Started draining %d objects (%s)",
			status.InitialObjectCount, humanizeBytes(status.InitialObjectBytes)))

	return nil
}

// pollDrainProgress checks the progress of an ongoing drain operation.
func (r *S3BucketReconciler) pollDrainProgress(ctx context.Context, rctx *bucketReconcileContext, backendStatus *grid.DrainStatus) error {
	log := log.FromContext(ctx).WithValues("function", "pollDrainProgress")
	now := metav1.Now()

	// Update poll timestamp
	rctx.Bucket.Status.DrainStatus.LastCheckedAt = &now
	rctx.Bucket.Status.DrainStatus.IsDeletingObjects = backendStatus.IsDeletingObjects

	// Recompute next poll interval (picks up config changes, handles two-tier polling)
	elapsed := time.Since(rctx.Bucket.Status.DrainStatus.StartedAt.Time)
	nextPollInterval := r.computeNextPollInterval(ctx, rctx, elapsed)
	rctx.Bucket.Status.DrainStatus.NextPollInterval = metav1.Duration{Duration: nextPollInterval}

	currentCount := int64(rctx.Bucket.Status.BucketUsage.ObjectCount)
	previousCount := rctx.Bucket.Status.DrainStatus.PreviousObjectCount

	// Check for progress
	if currentCount < previousCount {
		deleted := previousCount - currentCount
		log.V(1).Info("Drain making progress",
			"deleted", deleted,
			"remaining", currentCount)

		rctx.Bucket.Status.DrainStatus.LastProgressAt = &now
		rctx.Bucket.Status.DrainStatus.PreviousObjectCount = currentCount
		rctx.Bucket.Status.DrainStatus.Message = fmt.Sprintf("Draining: %d objects remaining", currentCount)

		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketDrainingProgress,
			fmt.Sprintf("Drain progress: deleted %d objects, %d remaining", deleted, currentCount))
	} else {
		// No progress - check if stuck
		// check how long since last progress
		stuckElapsed := now.Time.Sub(rctx.Bucket.Status.DrainStatus.LastProgressAt.Time)
		stuckThreshold := r.getStuckThreshold(ctx, rctx)

		if stuckElapsed > stuckThreshold {
			log.Info("Drain appears stuck", "noProgressFor", stuckElapsed)
			rctx.Bucket.Status.DrainStatus.Message = fmt.Sprintf("Warning: No progress for %v", stuckElapsed.Round(time.Minute))
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketDrainingStuck,
				fmt.Sprintf("No progress for %v, %d objects still remain", stuckElapsed.Round(time.Minute), currentCount))
		}
	}

	return nil
}

// completeDrain cleans up after a successful drain operation.
func (r *S3BucketReconciler) completeDrain(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "completeDrain")
	log.Info("Drain completed successfully", "bucket", rctx.Bucket.Status.BucketName)

	elapsed := time.Since(rctx.Bucket.Status.DrainStatus.StartedAt.Time)

	// Clean up drain status completely
	rctx.Bucket.Status.DrainStatus = nil
	rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseReady

	// Remove annotation
	delete(rctx.Bucket.Annotations, s3v1alpha1.AnnotationDrainBucket)
	rctx.ObjectUpdated = true

	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketDrainingComplete,
		fmt.Sprintf("Drain completed in %v", elapsed.Round(time.Minute)))

	return nil
}

// cancelDrain cancels an ongoing drain operation.
func (r *S3BucketReconciler) cancelDrain(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "cancelDrain")
	log.Info("Canceling drain operation", "bucket", rctx.Bucket.Status.BucketName)

	if err := grid.CancelBucketDrain(ctx, rctx.Bucket.Status.BucketName, rctx.TenantClient); err != nil {
		log.Error(err, "Failed to cancel drain on backend")
		return fmt.Errorf("failed to cancel drain: %w", err)
	}

	// Clean up drain status
	rctx.Bucket.Status.DrainStatus = nil
	rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseReady

	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketDrainingCanceled,
		"Drain operation canceled by user")

	return nil
}

// cancelOrphanedDrain cancels drain operations not initiated by the operator.
func (r *S3BucketReconciler) cancelOrphanedDrain(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "detectOrphanedDrain")
	log.Info("Detected orphaned drain operation (backend draining without operator initiation)")

	// Cancel the orphaned drain
	if err := grid.CancelBucketDrain(ctx, rctx.Bucket.Status.BucketName, rctx.TenantClient); err != nil {
		log.Error(err, "Failed to cancel orphaned drain")
		return fmt.Errorf("failed to cancel orphaned drain: %w", err)
	}

	r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketOrphanedDrain,
		"Detected and canceled drain operation not initiated by operator")

	return nil
}

// computeNextPollInterval calculates the next poll interval based on current config and elapsed time.
// Called on every drain reconciliation to pick up config changes and handle two-tier polling.
func (r *S3BucketReconciler) computeNextPollInterval(ctx context.Context, rctx *bucketReconcileContext, elapsed time.Duration) time.Duration {
	// Bucket-level override takes precedence (single interval, no two-tier)
	if rctx.Bucket.Spec.DrainPollInterval != nil {
		return rctx.Bucket.Spec.DrainPollInterval.Duration
	}

	// Fetch StorageGrid config for grid-level defaults
	sg := &s3v1alpha1.StorageGrid{}
	sgName := rctx.S3Tenant.Spec.StorageGridRef.Name
	if err := r.Get(ctx, client.ObjectKey{Name: sgName}, sg); err != nil {
		// Fallback to hardcoded defaults
		if elapsed < 1*time.Hour {
			return s3v1alpha1.DefaultDrainInitialPollInterval
		}
		return s3v1alpha1.DefaultDrainLongRunningPollInterval
	}

	// Extract grid-level intervals with defaults
	initialInterval := s3v1alpha1.DefaultDrainInitialPollInterval
	longRunningInterval := s3v1alpha1.DefaultDrainLongRunningPollInterval

	if sg.Spec.Operations != nil && sg.Spec.Operations.Drain != nil {
		if sg.Spec.Operations.Drain.InitialPollInterval != nil {
			initialInterval = sg.Spec.Operations.Drain.InitialPollInterval.Duration
		}
		if sg.Spec.Operations.Drain.LongRunningPollInterval != nil {
			longRunningInterval = sg.Spec.Operations.Drain.LongRunningPollInterval.Duration
		}
	}

	// Two-tier polling: switch after 1 hour
	if elapsed < 1*time.Hour {
		return initialInterval
	}
	return longRunningInterval
}

// getStuckThreshold returns the threshold for detecting stuck drains.
// Fetches from bucket spec override or StorageGrid config.
func (r *S3BucketReconciler) getStuckThreshold(ctx context.Context, rctx *bucketReconcileContext) time.Duration {
	// Bucket-level override takes precedence
	if rctx.Bucket.Spec.DrainStuckThreshold != nil {
		return rctx.Bucket.Spec.DrainStuckThreshold.Duration
	}

	// Fetch from StorageGrid config
	sg := &s3v1alpha1.StorageGrid{}
	sgName := rctx.S3Tenant.Spec.StorageGridRef.Name
	if err := r.Get(ctx, client.ObjectKey{Name: sgName}, sg); err != nil {
		return s3v1alpha1.DefaultDrainStuckThreshold
	}

	if sg.Spec.Operations != nil && sg.Spec.Operations.Drain != nil &&
		sg.Spec.Operations.Drain.StuckThreshold != nil {
		return sg.Spec.Operations.Drain.StuckThreshold.Duration
	}

	return s3v1alpha1.DefaultDrainStuckThreshold
}

// humanizeBytes converts bytes to human-readable format.
func humanizeBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func (r *S3BucketReconciler) setCondition(bucket *s3v1alpha1.S3Bucket, condType string, status metav1.ConditionStatus, reason string, message string) {
	condition := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: bucket.GetGeneration(),
	}

	meta.SetStatusCondition(&bucket.Status.Conditions, condition)
}

// deriveReadiness uses conditions to derive the overall readiness state and phase of the bucket.
// This method should be called after doReconcile to properly set the Ready condition and phase.
func (r *S3BucketReconciler) deriveReadiness(ctx context.Context, rctx *bucketReconcileContext) {
	log := log.FromContext(ctx).WithValues("function", "deriveReadiness")

	// Track condition states for readiness derivation.
	reconciliation := false
	backendReady := false
	created := false

	// Message that will show on the ready condition.
	message := ""

	// Check reconciliation condition.
	reconcileCondition := meta.FindStatusCondition(rctx.Bucket.Status.Conditions, s3v1alpha1.ConditionTypeReconcileSucceeded)
	if reconcileCondition != nil {
		if reconcileCondition.ObservedGeneration == rctx.Bucket.GetGeneration() {
			if reconcileCondition.Status == metav1.ConditionTrue {
				log.V(1).Info("Reconciliation succeeded")
				message += "Reconciliation succeeded"
				reconciliation = true
			} else {
				log.V(1).Info("Reconciliation failed", "reason", reconcileCondition.Reason)
				message += fmt.Sprintf("Reconciliation failed: %s", reconcileCondition.Reason)
			}
		} else {
			log.V(1).Info("Reconciliation condition is not from the current generation")
			message += "Reconciliation condition is not from the current generation"
		}
	} else {
		log.V(1).Info("Reconciliation condition not found")
		message += "Reconciliation condition not found"
	}

	// Check backing resource (tenant) ready condition.
	backendCondition := meta.FindStatusCondition(rctx.Bucket.Status.Conditions, s3v1alpha1.ContitionTypeBackingResourceReady)
	if backendCondition != nil {
		if backendCondition.ObservedGeneration == rctx.Bucket.GetGeneration() {
			if backendCondition.Status == metav1.ConditionTrue {
				log.V(1).Info("Backing resource (tenant) is ready")
				message += ", Tenant is ready"
				backendReady = true
			} else {
				log.V(1).Info("Backing resource (tenant) is not ready")
				message += ", Tenant is not ready"
			}
		} else {
			log.V(1).Info("Backing resource condition is not from the current generation")
			message += ", Tenant condition is not from the current generation"
		}
	} else {
		log.V(1).Info("Backing resource condition not found")
		message += ", Tenant condition not found"
	}

	// Check created condition.
	createdCondition := meta.FindStatusCondition(rctx.Bucket.Status.Conditions, s3v1alpha1.ConditionTypeCreated)
	if createdCondition != nil {
		if createdCondition.Status == metav1.ConditionTrue {
			log.V(1).Info("Bucket has been created")
			message += ", Bucket created"
			created = true
		} else {
			log.V(1).Info("Bucket has not been created yet")
			message += ", Bucket not yet created"
		}
	} else {
		log.V(1).Info("Created condition not found")
		message += ", Bucket creation pending"
	}

	// Determine phase and ready condition based on condition states.
	// Special case: Draining and Deleting phases take precedence.
	if rctx.Bucket.Status.Phase == s3v1alpha1.BucketPhaseDraining {
		log.V(1).Info("Bucket is draining, maintaining Draining phase")
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "BucketDraining", message+", Bucket is draining")
		return
	}

	if !rctx.Bucket.DeletionTimestamp.IsZero() {
		log.V(1).Info("Bucket is being deleted")
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseDeleting
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "BucketDeleting", message+", Bucket is being deleted")
		return
	}

	// Derive readiness based on conditions.
	if reconciliation && backendReady && created {
		log.V(1).Info("Bucket is ready")
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseReady
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, "BucketReady", message)
	} else if !reconciliation {
		log.V(1).Info("Bucket reconciliation failed")
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseFailed
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "BucketReconcileFailed", message)
	} else if !backendReady {
		log.V(1).Info("Bucket tenant not ready, bucket is pending")
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhasePending
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "TenantNotReady", message)
	} else if !created {
		log.V(1).Info("Bucket not yet created, bucket is pending")
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhasePending
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "BucketNotCreated", message)
	}
}

// bucket name needs to be unique across the entire storagegrid.
// we're adding the first 8 digits of the bucket uid as an identifier.
// format: bucketname-bucketuid.
func (r *S3BucketReconciler) reconcileBucketName(ctx context.Context, s3Bucket *s3v1alpha1.S3Bucket) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketName")
	log.V(1).Info("Reconciling bucket name for uniqueness")

	if s3Bucket.Status.BucketName != "" {
		log.V(1).Info("Bucket name already set, skipping generation", "bucketName", s3Bucket.Status.BucketName)
		// Bucket name already set, nothing to do.
		return nil
	}

	// if user provided a unique bucket name, use it.
	if s3Bucket.Spec.BucketName != nil {
		s3Bucket.Status.BucketName = *s3Bucket.Spec.BucketName
		log.V(1).Info("Using user-defined unique bucket name", "bucketName", s3Bucket.Status.BucketName)
		return nil
	}

	// Generate unique bucket name: bucketname-namespace-clusterid as default.
	s3Bucket.Status.BucketName = fmt.Sprintf("%s-%s", s3Bucket.Name, r.getBucketIdentifier(s3Bucket))
	log.V(1).Info("Generated unique bucket name", "bucketName", s3Bucket.Status.BucketName)

	return nil
}

func (r *S3BucketReconciler) getBucketIdentifier(s3Bucket *s3v1alpha1.S3Bucket) string {
	if s3Bucket.Spec.BucketName != nil {
		return *s3Bucket.Spec.BucketName
	}
	return string(s3Bucket.UID)[0:8]
}

// emitEvent emits a Kubernetes event immediately.
func (r *S3BucketReconciler) emitEvent(
	rctx *bucketReconcileContext,
	eventType, reason, message string) {
	r.Recorder.Event(rctx.Bucket, eventType, reason, message)
}

// SetupWithManager sets up the controller with the Manager.
func (r *S3BucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&s3v1alpha1.S3Bucket{}, builder.WithPredicates(
			PredicateWithoutStatusChange(),
		)).
		Owns(&corev1.Secret{}).
		Watches(
			&s3v1alpha1.S3Tenant{},
			handler.EnqueueRequestsFromMapFunc(r.mapTenantToBuckets),
			builder.WithPredicates(r.tenantEndpointChangePredicate()),
		).
		Complete(r)
}

// tenantEndpointChangePredicate triggers reconciliation only when the tenant's
// S3EndpointConfig changes (e.g. due to an S3TenantClassName change). All other
// tenant status updates are ignored for now to avoid unnecessary bucket reconciliations.
func (r *S3BucketReconciler) tenantEndpointChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			// Buckets handle their own initial endpoint resolution.
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldT, okOld := e.ObjectOld.(*s3v1alpha1.S3Tenant)
			newT, okNew := e.ObjectNew.(*s3v1alpha1.S3Tenant)
			if !okOld || !okNew {
				return false
			}
			return !reflect.DeepEqual(oldT.Status.S3EndpointConfig, newT.Status.S3EndpointConfig)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// Bucket reconciliation handles tenant disappearance on its next loop.
			return false
		},
	}
}

// mapTenantToBuckets returns reconcile requests for all S3Buckets that
// reference the given S3Tenant. Uses the "spec.s3TenantRef.name" field index
// (registered by S3TenantReconciler) to scope the list, then filters with the
// shared resolveBucketTenantKey helper to honor namespace-defaulting semantics.
func (r *S3BucketReconciler) mapTenantToBuckets(ctx context.Context, obj client.Object) []ctrl.Request {
	log := log.FromContext(ctx)
	tenant, ok := obj.(*s3v1alpha1.S3Tenant)
	if !ok {
		return nil
	}

	buckets := &s3v1alpha1.S3BucketList{}
	if err := r.List(ctx, buckets, client.MatchingFields{"spec.s3TenantRef.name": tenant.Name}); err != nil {
		log.Error(err, "Failed to list buckets for tenant", "tenant", tenant.Name)
		return nil
	}

	tenantKey := client.ObjectKey{Name: tenant.Name, Namespace: tenant.Namespace}
	requests := make([]ctrl.Request, 0, len(buckets.Items))
	for i := range buckets.Items {
		b := &buckets.Items[i]
		if resolveBucketTenantKey(b) != tenantKey {
			continue
		}
		requests = append(requests, ctrl.Request{
			NamespacedName: types.NamespacedName{Name: b.Name, Namespace: b.Namespace},
		})
	}

	if len(requests) > 0 {
		log.V(1).Info("Enqueueing buckets due to tenant endpoint change",
			"tenant", tenant.Name, "namespace", tenant.Namespace, "count", len(requests))
	}
	return requests
}
