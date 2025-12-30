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
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

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
	s3BucketFinalizer = "bucket.s3.bedag.ch/finalizer"
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
	err := r.doReconcile(ctx, rctx)

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
	if updateErr := r.Status().Update(ctx, rctx.Bucket); updateErr != nil {
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

	r.reconcileSecretRefs(ctx, rctx)

	// Validate tenant readiness.
	if err := r.reconcileTenantReadiness(ctx, rctx); err != nil {
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhasePending
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantNotReady", err.Error())
		r.setCondition(rctx.Bucket, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "TenantNotReady", err.Error())
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketTenantNotReady,
			fmt.Sprintf("Waiting for S3Tenant %s to become ready (current phase: %s)", rctx.S3Tenant.Name, rctx.S3Tenant.Status.Phase))
		return err
	}
	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketTenantReady,
		fmt.Sprintf("S3Tenant %s is ready for bucket operations", rctx.S3Tenant.Name))

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
		return nil
	}

	// Reconcile bucket creation.
	if err := r.reconcileBucketCreation(ctx, rctx); err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketCreationFailed", err.Error())
		return err
	}
	if rctx.DoRequeue {
		// Bucket was just created or updated, requeue for further processing.
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

	if err := r.initS3Client(ctx, rctx); err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "S3ClientInitFailed", err.Error())
		return err
	}

	// Reconcile bucket policy.
	if err := r.reconcileBucketPolicy(ctx, rctx); err != nil {
		log.Error(err, "Failed to reconcile bucket policy")
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketPolicyReconcileFailed", err.Error())
		// Don't return error, continue with other reconciliation.
	}

	// Set final ready condition and phase (unless draining).
	if rctx.Bucket.Status.Phase != s3v1alpha1.BucketPhaseDraining {
		rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseReady
	}
	r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, "BucketReady", "Bucket is created and ready to use")

	return nil
}

func (r *S3BucketReconciler) reconcileSecretRefs(ctx context.Context, rctx *bucketReconcileContext) {
	log := log.FromContext(ctx).WithValues("function", "reconcileSecretRefs")
	log.V(1).Info("Starting secret references reconciliation")

	// if already specified we can just return as this is only set once.
	if rctx.Bucket.Status.S3AdminKeysSecretRef != nil && rctx.Bucket.Status.S3AdminKeysSecretRef.Name != "" {
		return
	}

	// secret name is generated as <S3 Tenant>-<Bucket>-s3-admin-keypair.
	rctx.Bucket.Status.S3AdminKeysSecretRef = &corev1.LocalObjectReference{
		Name: fmt.Sprintf("s3bucket-%s-%s-s3-admin-keypair", rctx.S3Tenant.Name, rctx.Bucket.Name),
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

func (r *S3BucketReconciler) reconcileS3TenantReference(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileS3TenantReference")

	rctx.S3Tenant = &s3v1alpha1.S3Tenant{}

	// Determine namespace to look up tenant.
	tenantNamespace := rctx.Bucket.Spec.S3TenantRef.Namespace
	if tenantNamespace == "" {
		tenantNamespace = rctx.Bucket.Namespace
		log.V(1).Info("Using bucket namespace for tenant lookup", "namespace", tenantNamespace)
	} else {
		log.V(1).Info("Using specified tenant namespace", "namespace", tenantNamespace)
	}

	tenantKey := client.ObjectKey{
		Name:      rctx.Bucket.Spec.S3TenantRef.Name,
		Namespace: tenantNamespace,
	}

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

	// Check if bucket already exists.
	exists, err := grid.BucketExists(ctx, rctx.Bucket.Status.BucketName, rctx.TenantClient)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if exists {
		log.V(1).Info("Bucket already exists", "bucketName", rctx.Bucket.Status.BucketName)

		// Update S3 endpoint config from tenant.
		rctx.Bucket.Status.S3EndpointConfig = rctx.S3Tenant.Status.S3EndpointConfig

		return nil
	}

	log.V(1).Info("Bucket does not exist, creating it")

	// Validate and set region.
	if err := r.reconcileRegion(ctx, rctx); err != nil {
		return err
	}

	// Generate unique bucket name if not set.
	if err := r.reconcileBucketName(ctx, rctx.Bucket); err != nil {
		return err
	}

	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketCreating,
		fmt.Sprintf("Creating bucket %s in region %s", rctx.Bucket.Status.BucketName, rctx.Bucket.Status.Region))

	// Create bucket.
	err = grid.CreateBucket(ctx, rctx.Bucket.Status.BucketName, rctx.Bucket.Spec.Region, *rctx.Bucket.Spec.RetentionInDays, rctx.TenantClient)
	if err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionFalse, "BucketCreateReconcileFailed", err.Error())
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketCreateFailed,
			fmt.Sprintf("Failed to create bucket: %v", err))
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "BucketCreated", "Bucket created successfully")
	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketCreated,
		fmt.Sprintf("Successfully created bucket %s", rctx.Bucket.Status.BucketName))

	log.V(1).Info("Bucket created successfully", "bucketName", rctx.Bucket.Status.BucketName)

	// Set phase to Ready after successful creation
	rctx.Bucket.Status.Phase = s3v1alpha1.BucketPhaseReady

	log.V(1).Info("Bucket creation completed successfully")
	rctx.DoRequeue = true // Requeue for further processing
	return nil
}

func (r *S3BucketReconciler) reconcileRegion(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileRegion")

	if rctx.Bucket.Spec.Region == "" {
		if rctx.S3Tenant.Status.DefaultBucketRegion == "" {
			return fmt.Errorf("no region specified and no default region set in tenant")
		}

		log.V(1).Info("Setting default region from tenant",
			"defaultBucketRegion", rctx.S3Tenant.Status.DefaultBucketRegion)
		rctx.Bucket.Status.Region = rctx.S3Tenant.Status.DefaultBucketRegion
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketRegionSet,
			fmt.Sprintf("Using default region %s from tenant", rctx.S3Tenant.Status.DefaultBucketRegion))
	} else {
		rctx.Bucket.Status.Region = rctx.Bucket.Spec.Region

		// Validate specified region exists in tenant.
		if !slices.Contains(rctx.S3Tenant.Status.Regions, rctx.Bucket.Status.Region) {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventBucketRegionValidationFailed,
				fmt.Sprintf("Specified region %s does not exist in tenant (available: %v)", rctx.Bucket.Spec.Region, rctx.S3Tenant.Status.Regions))
			return fmt.Errorf("specified region %s does not exist in tenant", rctx.Bucket.Spec.Region)
		}
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketRegionSet,
			fmt.Sprintf("Using specified region %s", rctx.Bucket.Spec.Region))
	}

	log.V(1).Info("Region validation successful", "region", rctx.Bucket.Status.Region)
	return nil
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

	log.V(1).Info("Admin user created successfully")
	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketAdminUserCreated,
		fmt.Sprintf("Created admin user and group for bucket %s", rctx.Bucket.Status.BucketName))
	return nil
}

func (r *S3BucketReconciler) reconcileBucketS3Credentials(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketS3Credentials")

	_, _, err := kube.FetchKeyPairFromSecret(ctx, r.Client, rctx.Bucket.Namespace, rctx.Bucket.Status.S3AdminKeysSecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch s3 keypair from secret, creating new s3 admin keypair")
		err := r.createS3AdminKeypair(ctx, rctx, false)
		if err != nil {
			log.Error(err, "Failed to create S3 admin keypair")
			return err
		}

		return nil
	}

	// Check if credential recreation annotation is present.
	// check if annotations exist and exit if unset.
	if rctx.Bucket.Annotations != nil {
		// if annotation is not set to "true", we start recreation.
		if rctx.Bucket.Annotations[s3v1alpha1.AnnotationRecreateBucketKeypairs] == "true" {
			err := r.createS3AdminKeypair(ctx, rctx, true)
			if err != nil {
				return fmt.Errorf("failed to recreate S3 admin keypair: %w", err)
			}
			// Remove annotation.
			delete(rctx.Bucket.Annotations, s3v1alpha1.AnnotationRecreateBucketKeypairs)
			rctx.ObjectUpdated = true
			return nil
		}
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
	err = kube.CreateKeyPairSecret(ctx, r.Client, rctx.Bucket.Namespace, rctx.Bucket.Status.S3AdminKeysSecretRef.Name, accessKey, secretKey, rctx.Bucket)
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

	r.emitEvent(rctx, corev1.EventTypeNormal, EventBucketUsageUpdated,
		fmt.Sprintf("Updated usage: %d objects, %s", rctx.Bucket.Status.BucketUsage.ObjectCount, rctx.Bucket.Status.BucketUsage.Bytes))

	log.V(1).Info("Bucket usage updated",
		"objectCount", rctx.Bucket.Status.BucketUsage.ObjectCount,
		"bytes", rctx.Bucket.Status.BucketUsage.Bytes)

	return nil
}

func (r *S3BucketReconciler) initS3Client(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "initS3Client")

	// Get S3 credentials for client initialization.
	accessKey, secretKey, err := kube.FetchCredentialsFromSecret(ctx, r.Client, rctx.Bucket.Namespace, rctx.Bucket.Status.S3AdminKeysSecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch S3 credentials")
		return fmt.Errorf("failed to fetch S3 credentials: %w", err)
	}

	// Resolve which S3 endpoint to use
	endpointConfig, tenantClassName, err := r.resolveS3Endpoint(ctx, rctx)
	if err != nil {
		return err
	}

	endpointURL := fmt.Sprintf("https://%s:%d", endpointConfig.DefaultAddress, endpointConfig.Port)
	s3client, err := s3.InitS3Client(ctx, endpointURL, accessKey, secretKey, rctx.Bucket.Status.Region, *endpointConfig.PathStyleAccess)
	if err != nil {
		log.Error(err, "Failed to initialize S3 client")
		r.emitEvent(rctx, corev1.EventTypeWarning, EventS3EndpointConnectionFailed,
			fmt.Sprintf("Failed to connect to S3 endpoint %s (%s): %v (check network access)", endpointURL, tenantClassName, err))
		return fmt.Errorf("failed to initialize S3 client: %w", err)
	}

	rctx.S3Client = s3client
	r.emitEvent(rctx, corev1.EventTypeNormal, EventS3EndpointConnectionEstablished,
		fmt.Sprintf("Successfully connected to S3 endpoint %s (%s)", endpointURL, tenantClassName))

	log.V(1).Info("S3 client initialized successfully", "endpoint", endpointURL, "source", tenantClassName)
	return nil
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

	// Generate unique bucket name: bucketname-namespace-clusterid.
	s3Bucket.Status.BucketName = fmt.Sprintf("%s-%s", s3Bucket.Name, r.getBucketIdentifier(s3Bucket))
	log.V(1).Info("Generated unique bucket name", "bucketName", s3Bucket.Status.BucketName)

	return nil
}

func (r *S3BucketReconciler) getBucketIdentifier(s3Bucket *s3v1alpha1.S3Bucket) string {
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
		Complete(r)
}
