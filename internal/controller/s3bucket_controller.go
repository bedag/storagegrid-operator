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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	s3v1alpha1 "git.mgmtbi.ch/cloud/storagegrid-operator/api/v1alpha1"
	"git.mgmtbi.ch/cloud/storagegrid-operator/pkg/grid"
	"git.mgmtbi.ch/cloud/storagegrid-operator/pkg/kube"
	"git.mgmtbi.ch/cloud/storagegrid-operator/pkg/s3"
)

// S3BucketReconciler reconciles a S3Bucket object.
type S3BucketReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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

	log.V(1).Info("Reconciliation completed successfully")
	return ctrl.Result{Requeue: rctx.DoRequeue}, err
}

func (r *S3BucketReconciler) doReconcile(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "doReconcile")

	// Fetch and validate S3Tenant.
	if err := r.reconcileS3TenantReference(ctx, rctx); err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantNotFound", err.Error())
		r.setCondition(rctx.Bucket, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "TenantNotFound", err.Error())
		return err
	}

	r.reconcileSecretRefs(ctx, rctx)

	// Validate tenant readiness.
	if err := r.reconcileTenantReadiness(ctx, rctx); err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantNotReady", err.Error())
		r.setCondition(rctx.Bucket, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "TenantNotReady", err.Error())
		return err
	}

	// Initialize tenant client.
	if err := r.reconcileTenantClient(ctx, rctx); err != nil {
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

	// Set final ready condition.
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

		// Update S3 API endpoint from tenant.
		rctx.Bucket.Status.S3ApiEndpoint = rctx.S3Tenant.Status.S3ApiEndpoint

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

	// Create bucket.
	err = grid.CreateBucket(ctx, rctx.Bucket.Status.BucketName, rctx.Bucket.Spec.Region, *rctx.Bucket.Spec.RetentionInDays, rctx.TenantClient)
	if err != nil {
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionFalse, "BucketCreateReconcileFailed", err.Error())
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "BucketCreated", "Bucket created successfully")

	log.V(1).Info("Bucket created successfully", "bucketName", rctx.Bucket.Status.BucketName)

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
	} else {
		rctx.Bucket.Status.Region = rctx.Bucket.Spec.Region

		// Validate specified region exists in tenant.
		if !slices.Contains(rctx.S3Tenant.Status.Regions, rctx.Bucket.Status.Region) {
			return fmt.Errorf("specified region %s does not exist in tenant", rctx.Bucket.Spec.Region)
		}
	}

	log.V(1).Info("Region validation successful", "region", rctx.Bucket.Status.Region)
	return nil
}

func (r *S3BucketReconciler) reconcileBucketAdmin(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketAdminUser")

	// we're using the bucket UID as unique identifier for the admin user.
	err := grid.CreateBucketAdminIfNotExists(ctx, rctx.Bucket.Status.BucketName, r.getBucketIdentifier(rctx.Bucket), rctx.TenantClient)
	if err != nil {
		return fmt.Errorf("failed to create admin user: %w", err)
	}

	log.V(1).Info("Admin user created successfully")
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
		if rctx.Bucket.Annotations[AnnotationRecreateBucketKeypairs] == "true" {
			err := r.createS3AdminKeypair(ctx, rctx, true)
			if err != nil {
				return fmt.Errorf("failed to recreate S3 admin keypair: %w", err)
			}
			// Remove annotation.
			delete(rctx.Bucket.Annotations, AnnotationRecreateBucketKeypairs)
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
		// recreate the S3 admin keypair with existing access key ID.
		accessKeyId, accessKey, secretKey, err = grid.RecreateS3Credentials(ctx, rctx.Bucket.Status.BucketName, r.getBucketIdentifier(rctx.Bucket), rctx.Bucket.Status.AccessKeyId, rctx.TenantClient)
		if err != nil {
			log.Error(err, "Failed to recreate S3 admin keypair")
			return err
		}
	} else {
		log.V(1).Info("Creating initial S3 keypair")
		// create s3 keys.
		accessKeyId, accessKey, secretKey, err = grid.CreateS3Credentials(ctx, rctx.Bucket.Status.BucketName, r.getBucketIdentifier(rctx.Bucket), rctx.TenantClient)
		if err != nil {
			log.Error(err, "Failed to create s3 keys")
			return err
		}
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

func (r *S3BucketReconciler) initS3Client(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "initS3Client")

	// Get S3 credentials for client initialization.
	accessKey, secretKey, err := kube.FetchCredentialsFromSecret(ctx, r.Client, rctx.Bucket.Namespace, rctx.Bucket.Status.S3AdminKeysSecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch S3 credentials")
		return fmt.Errorf("failed to fetch S3 credentials: %w", err)
	}

	// Initialize S3 client.
	endpoint := fmt.Sprintf("https://%s:%d", rctx.Bucket.Status.S3ApiEndpoint.S3Urls[0], rctx.Bucket.Status.S3ApiEndpoint.Port)
	s3client, err := s3.InitS3Client(ctx, endpoint, accessKey, secretKey, rctx.Bucket.Status.Region, *rctx.Bucket.Status.S3ApiEndpoint.PathStyleAccess)
	if err != nil {
		log.Error(err, "Failed to initialize S3 client")
		return fmt.Errorf("failed to initialize S3 client: %w", err)
	}

	rctx.S3Client = s3client

	log.V(1).Info("S3 client initialized successfully")
	return nil
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
			return fmt.Errorf("failed to apply policy: %w", err)
		}

		rctx.Bucket.Status.LastAppliedPolicy = rctx.Bucket.Spec.BucketPolicyJson
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "PolicyApplied", "Bucket policy applied successfully")
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
			return fmt.Errorf("failed to delete policy: %w", err)
		}

		rctx.Bucket.Status.LastAppliedPolicy = ""
		r.setCondition(rctx.Bucket, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "PolicyDeleted", "Bucket policy removed successfully")
		log.V(1).Info("Bucket policy removed successfully")
	}

	return nil
}

func (r *S3BucketReconciler) finalize(ctx context.Context, rctx *bucketReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "finalize")
	log.V(1).Info("Finalizing S3Bucket")

	if err := r.reconcileBucketUsage(ctx, rctx); err != nil {
		log.Error(err, "Failed to reconcile bucket usage during finalization")
		// Continue with finalization even if usage reconciliation fails.
	}

	// if bucket still has objects, we cannot delete it.
	if rctx.Bucket.Status.BucketUsage.ObjectCount > 0 {
		err := fmt.Errorf("bucket %s still has %d objects, cannot delete. Delete objects first or drain bucket using the drain-bucket-force annotation", rctx.Bucket.Status.BucketName, rctx.Bucket.Status.BucketUsage.ObjectCount)
		log.Error(err, "Bucket not empty, cannot finalize")
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
		if err := grid.DeleteBucket(ctx, rctx.Bucket.Status.BucketName, rctx.TenantClient); err != nil {
			log.Error(err, "Failed to delete bucket", "bucketName", rctx.Bucket.Status.BucketName)
			return fmt.Errorf("failed to delete bucket %s: %w", rctx.Bucket.Status.BucketName, err)
		}
		log.V(1).Info("Bucket deleted successfully", "bucketName", rctx.Bucket.Status.BucketName)
	}

	log.V(1).Info("Finalization completed successfully")
	return nil
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

// SetupWithManager sets up the controller with the Manager.
func (r *S3BucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&s3v1alpha1.S3Bucket{}, builder.WithPredicates(
			PredicateWithoutStatusChange(),
		)).
		Owns(&corev1.Secret{}).
		Complete(r)
}
