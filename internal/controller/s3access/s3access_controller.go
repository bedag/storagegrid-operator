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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	log "sigs.k8s.io/controller-runtime/pkg/log"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	parentctrl "github.com/bedag/storagegrid-operator/internal/controller"
	"github.com/bedag/storagegrid-operator/pkg/grid"
	"github.com/bedag/storagegrid-operator/pkg/kube"
)

const (
	S3AccessFinalizer = "s3access.s3.bedag.ch/finalizer"
)

// S3AccessReconciler reconciles a S3Access object.
type S3AccessReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

type s3AccessReconcileContext struct {
	ObjectUpdated            bool
	DoRequeue                bool
	S3Access                 *s3v1alpha1.S3Access
	S3Bucket                 *s3v1alpha1.S3Bucket
	Policies                 []*s3v1alpha1.S3Policy
	GlobalPolicies           []*s3v1alpha1.GlobalS3Policy
	TenantClient             *grid.TenantClient
	PolicyStatements         []grid.PolicyStatement
	DesiredPolicyDocument    string
	DesiredAppliedPolicyRefs []s3v1alpha1.AppliedPolicyRef
}

// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3accesses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3accesses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3accesses/finalizers,verbs=update
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3buckets,verbs=get;list;watch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3policies,verbs=get;list;watch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=globals3policies,verbs=get;list;watch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenants,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *S3AccessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("s3access", req.NamespacedName)
	log.V(1).Info("Starting reconciliation")

	// initialize reconcile context
	rctx := &s3AccessReconcileContext{
		S3Access:       &s3v1alpha1.S3Access{},
		S3Bucket:       &s3v1alpha1.S3Bucket{},
		Policies:       []*s3v1alpha1.S3Policy{},
		GlobalPolicies: []*s3v1alpha1.GlobalS3Policy{},
		ObjectUpdated:  false,
		DoRequeue:      false,
	}

	// fetch the S3Access instance
	if err := r.Get(ctx, req.NamespacedName, rctx.S3Access); err != nil {
		log.Error(err, "Failed to get S3Access")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.V(1).Info("Successfully fetched S3Access", "name", rctx.S3Access.Name)

	statusBase := rctx.S3Access.DeepCopy()
	err := r.doReconcile(ctx, rctx)

	// use conditions to derive the readiness state.
	r.deriveReadiness(ctx, rctx)

	// update the annotations if they were updated.
	// needs to be done before the status is updated because we lose the annotations otherwise.
	if rctx.ObjectUpdated {
		// creating a deep copy of the status to avoid modifying the original object.
		statusCopy := rctx.S3Access.DeepCopy()

		log.V(1).Info("Object was updated")
		if updateErr := r.Update(ctx, rctx.S3Access); updateErr != nil {
			log.Error(updateErr, "Failed to update annotations")
			if err != nil {
				err = fmt.Errorf("reconciliation failed: %w, failed to update annotations: %w", err, updateErr)
			} else {
				err = fmt.Errorf("failed to update annotations: %w", updateErr)
			}
		} else {
			log.V(1).Info("Annotations updated successfully")
		}

		rctx.S3Access.Status = statusCopy.Status // restore the status from the copy
	}

	// update the status of the s3access if it was updated during reconciliation.
	if updateErr := r.Status().Patch(ctx, rctx.S3Access, client.MergeFrom(statusBase)); updateErr != nil {
		log.Error(updateErr, "Failed to update status, requeuing")
		if err == nil {
			// no error occurred during reconciliation, but status update failed.
			rctx.DoRequeue = true // we need to requeue to ensure the status is updated
		}
		// if an error already occurred during reconciliation, we just return that error.
	}

	return ctrl.Result{Requeue: rctx.DoRequeue}, err
}

func (r *S3AccessReconciler) doReconcile(ctx context.Context, rctx *s3AccessReconcileContext) error {
	// Resolve bucket and tenant references.
	if err := r.reconcileBucketReference(ctx, rctx); err != nil {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BucketReferenceError", fmt.Sprintf("Failed to reconcile bucket reference: %v", err))
		return err
	}

	// Reconcile secret references early (set default name in status, handle renames).
	if err := r.reconcileSecretRefs(ctx, rctx); err != nil {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "SecretReferenceError", fmt.Sprintf("Failed to reconcile secret reference: %v", err))
		return err
	}

	// Initialize tenant client (needed before finalize for potential backend cleanup).
	if err := r.reconcileTenantClient(ctx, rctx); err != nil {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "BackendConnectionFailed", err.Error())
		r.setCondition(rctx.S3Access, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "BackendConnectionFailed", fmt.Sprintf("Failed to establish connection to the backend: %v", err))
		return err
	}
	r.setCondition(rctx.S3Access, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionTrue, "BackendConnectionReady", "Connection to the backend established")

	// Handle finalizer and deletion
	if err := r.reconcileFinalizerAndDelete(ctx, rctx); err != nil {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "FinalizerError", fmt.Sprintf("Failed to reconcile delete or finalizer: %v", err))
		return err
	}
	if rctx.DoRequeue {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "FinalizerReconcileSucceeded", "Finalizer and deletion timestamp reconciled successfully, requeuing")
		return nil
	}

	// Reconcile policy references.
	if err := r.reconcilePolicies(ctx, rctx); err != nil {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "PolicyReferenceError", fmt.Sprintf("Failed to reconcile policy references: %v", err))
		return err
	}

	// Render combined policy document from referenced policies.
	if err := r.renderPolicyDocument(ctx, rctx); err != nil {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "PolicyDocumentRenderError", fmt.Sprintf("Failed to render policy document: %v", err))
		r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeWarning, "PolicyRenderFailed", "Failed to render policy document: %v", err)
		return err
	}

	// Apply subpath restrictions to object-scoped policy statements.
	if err := r.reconcileSubPaths(ctx, rctx); err != nil {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "SubPathError", fmt.Sprintf("Failed to reconcile subpaths: %v", err))
		return err
	}

	// Create user and group if they don't exist.
	// We run this everytime to ensure that the user get recreated, if it was deleted manually from the backend.
	if err := r.reconcileUserAndAccessKeys(ctx, rctx); err != nil {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "UserAndAccessKeyError", fmt.Sprintf("Failed to reconcile user and access keys: %v", err))
		return err
	}

	// Only apply policy to backend if the desired document changed.
	if rctx.DesiredPolicyDocument != rctx.S3Access.Status.PolicyDocument {
		if err := r.applyPolicyToUser(ctx, rctx); err != nil {
			r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "ApplyPolicyError", fmt.Sprintf("Failed to apply policy to user: %v", err))
			r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeWarning, "PolicyApplyFailed", "Failed to apply policy to user: %v", err)
			return err
		}
		// Write to status only after successful apply.
		rctx.S3Access.Status.PolicyDocument = rctx.DesiredPolicyDocument
		rctx.S3Access.Status.AppliedPolicyRefs = rctx.DesiredAppliedPolicyRefs
		r.Recorder.Event(rctx.S3Access, corev1.EventTypeNormal, "PolicyApplied", "Policy rendered and applied to access user")
	}

	// Reconcile credentials secret (raw keypair, the operator's source of truth).
	if err := r.reconcileSecret(ctx, rctx); err != nil {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "SecretReconcileError", fmt.Sprintf("Failed to reconcile secret: %v", err))
		return err
	}

	// Reconcile the operator-synthesized connection-details Secret. This is a user-facing
	// projection of the credentials secret + bucket info; non-fatal on failure.
	if err := r.reconcileConnectionDetails(ctx, rctx); err != nil {
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "ConnectionDetailsReconcileFailed", err.Error())
	}

	// Reconciliation completed successfully.
	r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "ReconcileSucceeded", "Reconciliation completed successfully")

	return nil
}

func (r *S3AccessReconciler) finalize(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx)
	log.Info("Finalizing S3Access", "name", rctx.S3Access.Name)

	identifier := r.getAccessIdentifier(rctx)
	if err := grid.DeleteAccessUser(ctx, identifier, rctx.TenantClient); err != nil {
		if strings.Contains(err.Error(), "API error: 404 Not Found (code: 404)") {
			log.Info("Access user not found during finalization, assuming already deleted")
			return nil
		}
		log.Error(err, "Failed to delete access user during finalization")
		return err
	}

	log.Info("S3Access finalized successfully")
	return nil
}

func (r *S3AccessReconciler) reconcileFinalizerAndDelete(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx)

	// examine DeletionTimestamp to determine if object is under deletion.
	if rctx.S3Access.DeletionTimestamp.IsZero() {
		// if the object is not being deleted, add our finalizer if it is not already present.
		if !controllerutil.ContainsFinalizer(rctx.S3Access, S3AccessFinalizer) {
			controllerutil.AddFinalizer(rctx.S3Access, S3AccessFinalizer)
			log.V(1).Info("Adding finalizer to S3Access")
			rctx.DoRequeue = true // requeue to ensure finalizer is added
			rctx.ObjectUpdated = true
			return nil
		}
	} else {
		// the object is being deleted.
		log.V(1).Info("S3Access is being deleted, checking for finalizer")
		if controllerutil.ContainsFinalizer(rctx.S3Access, S3AccessFinalizer) {
			// our finalizer is present, so lets handle any external dependency.
			if err := r.finalize(ctx, rctx); err != nil {
				log.Error(err, "Failed to finalize S3Access")
				return err
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(rctx.S3Access, S3AccessFinalizer)
			log.V(1).Info("Removing finalizer from S3Access")
			rctx.DoRequeue = true // requeue to ensure finalizer is removed
			rctx.ObjectUpdated = true
			return nil
		}
		// Stop reconciliation as the item is being deleted.

		log.V(1).Info("S3Access is being deleted, no finalizer present")
		return nil
	}

	return nil
}

func (r *S3AccessReconciler) setCondition(s3access *s3v1alpha1.S3Access, condType string, status metav1.ConditionStatus, reason string, message string) {
	// add or update with given condition.
	condition := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: s3access.GetGeneration(),
	}

	meta.SetStatusCondition(&s3access.Status.Conditions, condition)
}

func (r *S3AccessReconciler) getAccessIdentifier(rctx *s3AccessReconcileContext) string {
	// bucket and access name combinations are unique as they are always within the same namespace
	return fmt.Sprintf("%s-%s", rctx.S3Bucket.Status.BucketName, rctx.S3Access.Name)
}

func (r *S3AccessReconciler) reconcileBucketReference(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileBucketReference")

	// Fetch S3Bucket from same namespace.
	bucketKey := client.ObjectKey{
		Name:      rctx.S3Access.Spec.S3BucketRef.Name,
		Namespace: rctx.S3Access.Namespace,
	}
	if err := r.Get(ctx, bucketKey, rctx.S3Bucket); err != nil {
		return fmt.Errorf("failed to get S3Bucket %s: %w", bucketKey, err)
	}

	log.V(1).Info("Successfully retrieved S3Bucket", "name", rctx.S3Bucket.Name)

	return nil
}

func (r *S3AccessReconciler) reconcilePolicies(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcilePolicies")

	for _, ref := range rctx.S3Access.Spec.PolicyRefs {
		kind := ref.Kind
		if kind == "" {
			kind = "GlobalS3Policy"
		}

		switch kind {
		case "S3Policy":
			policy := &s3v1alpha1.S3Policy{}
			policyKey := types.NamespacedName{
				Name:      ref.Name,
				Namespace: rctx.S3Access.Namespace,
			}
			if err := r.Get(ctx, policyKey, policy); err != nil {
				return fmt.Errorf("failed to get S3Policy %s: %w", policyKey, err)
			}
			rctx.Policies = append(rctx.Policies, policy)
			log.V(1).Info("Fetched S3Policy", "name", policy.Name)

		case "GlobalS3Policy":
			policy := &s3v1alpha1.GlobalS3Policy{}
			policyKey := types.NamespacedName{Name: ref.Name}
			if err := r.Get(ctx, policyKey, policy); err != nil {
				return fmt.Errorf("failed to get GlobalS3Policy %s: %w", ref.Name, err)
			}
			rctx.GlobalPolicies = append(rctx.GlobalPolicies, policy)
			log.V(1).Info("Fetched GlobalS3Policy", "name", policy.Name)

		default:
			return fmt.Errorf("unsupported policy kind: %s", kind)
		}
	}

	return nil
}

func (r *S3AccessReconciler) renderPolicyDocument(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "renderPolicyDocument")

	bucketName := rctx.S3Bucket.Status.BucketName
	if bucketName == "" {
		return fmt.Errorf("bucket %s has no BucketName in status", rctx.S3Bucket.Name)
	}

	allStatements := []grid.PolicyStatement{}
	appliedRefs := []s3v1alpha1.AppliedPolicyRef{}

	// Collect statements from namespaced policies.
	for _, policy := range rctx.Policies {
		statements, err := r.parsePolicyStatements(policy.Status.CommonPolicyStatus.RenderedPolicy, bucketName)
		if err != nil {
			return fmt.Errorf("failed to parse rendered policy from S3Policy %s: %w", policy.Name, err)
		}
		allStatements = append(allStatements, statements...)
		version := ""
		if policy.Spec.CommonPolicySpec.Version != nil {
			version = *policy.Spec.CommonPolicySpec.Version
		}
		appliedRefs = append(appliedRefs, s3v1alpha1.AppliedPolicyRef{
			Kind:      policy.Kind,
			Name:      policy.Name,
			Namespace: policy.Namespace,
			Version:   version,
		})
	}

	// Collect statements from global policies.
	for _, policy := range rctx.GlobalPolicies {
		statements, err := r.parsePolicyStatements(policy.Status.CommonPolicyStatus.RenderedPolicy, bucketName)
		if err != nil {
			return fmt.Errorf("failed to parse rendered policy from GlobalS3Policy %s: %w", policy.Name, err)
		}
		allStatements = append(allStatements, statements...)
		version := ""
		if policy.Spec.CommonPolicySpec.Version != nil {
			version = *policy.Spec.CommonPolicySpec.Version
		}
		appliedRefs = append(appliedRefs, s3v1alpha1.AppliedPolicyRef{
			Kind:    policy.Kind,
			Name:    policy.Name,
			Version: version,
		})
	}

	rctx.PolicyStatements = allStatements
	rctx.DesiredAppliedPolicyRefs = appliedRefs

	// Render desired policy document (not yet written to status).
	combinedPolicy := grid.Policy{
		Statement: allStatements,
	}
	rctx.DesiredPolicyDocument = grid.GeneratePolicyDocument(combinedPolicy)

	log.V(1).Info("Rendered combined policy document", "statementCount", len(allStatements))
	return nil
}

// parsePolicyStatements parses a rendered policy JSON and returns grid.PolicyStatement slices,
// replacing the BUCKET_NAME placeholder with the actual bucket name.
func (r *S3AccessReconciler) parsePolicyStatements(renderedPolicy string, bucketName string) ([]grid.PolicyStatement, error) {
	if renderedPolicy == "" {
		return nil, fmt.Errorf("rendered policy is empty")
	}

	// Replace BUCKET_NAME placeholder with actual bucket name.
	policyJSON := strings.ReplaceAll(renderedPolicy, "BUCKET_NAME", bucketName)

	var policy grid.Policy
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		return nil, fmt.Errorf("failed to unmarshal policy JSON: %w", err)
	}

	return policy.Statement, nil
}

// reconcileSubPaths restricts object-scoped policy statements to the specified subpaths.
// If no subpaths are specified, all statements remain unchanged (full bucket access).
// Object-scoped resources (ending in /*) are expanded into one resource per subpath.
func (r *S3AccessReconciler) reconcileSubPaths(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileSubPaths")

	subPaths := rctx.S3Access.Spec.SubPaths
	if len(subPaths) == 0 {
		log.V(1).Info("No subpaths specified, skipping")
		return nil
	}

	bucketName := rctx.S3Bucket.Status.BucketName
	objectSuffix := fmt.Sprintf("arn:aws:s3:::%s/*", bucketName)

	updated := make([]grid.PolicyStatement, 0, len(rctx.PolicyStatements))
	for _, stmt := range rctx.PolicyStatements {
		var newResources []string
		for _, res := range stmt.Resource {
			if res == objectSuffix {
				// Expand the wildcard object resource into per-subpath resources.
				for _, sp := range subPaths {
					newResources = append(newResources, fmt.Sprintf("arn:aws:s3:::%s/%s", bucketName, sp))
				}
			} else {
				// Keep bucket-level and other resources unchanged.
				newResources = append(newResources, res)
			}
		}
		updated = append(updated, grid.PolicyStatement{
			Effect:   stmt.Effect,
			Action:   stmt.Action,
			Resource: newResources,
		})
	}

	rctx.PolicyStatements = updated

	// Re-render the desired policy document with subpath-restricted statements.
	combinedPolicy := grid.Policy{
		Statement: updated,
	}
	rctx.DesiredPolicyDocument = grid.GeneratePolicyDocument(combinedPolicy)

	log.V(1).Info("Subpaths applied to policy statements", "subPaths", subPaths)
	return nil
}

func (r *S3AccessReconciler) reconcileTenantClient(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileTenantClient")

	// Fetch S3Tenant from the bucket's tenant reference.
	tenantNamespace := rctx.S3Bucket.Spec.S3TenantRef.Namespace
	if tenantNamespace == "" {
		tenantNamespace = rctx.S3Bucket.Namespace
	}
	tenantKey := client.ObjectKey{
		Name:      rctx.S3Bucket.Spec.S3TenantRef.Name,
		Namespace: tenantNamespace,
	}

	// use temp var for tenant
	s3Tenant := &s3v1alpha1.S3Tenant{}

	if err := r.Get(ctx, tenantKey, s3Tenant); err != nil {
		return fmt.Errorf("failed to get S3Tenant %s: %w", tenantKey, err)
	}

	log.V(1).Info("Successfully retrieved S3Tenant", "name", s3Tenant.Name)

	// Validate tenant readiness. A terminating tenant still serves cleanup for an S3Access
	// that is itself being deleted - this runs before the finalizer step, so refusing here
	// would stop the S3Access from ever removing its backend user.
	if !s3Tenant.CanServeBackendOperations(!rctx.S3Access.DeletionTimestamp.IsZero()) {
		return fmt.Errorf("tenant %s is not ready, current phase: %s", s3Tenant.Name, s3Tenant.Status.Phase)
	}

	// Fetch tenant admin credentials.
	username, password, err := kube.FetchCredentialsFromSecret(
		ctx,
		r.Client,
		s3Tenant.Status.AdminSecretRef.Namespace,
		s3Tenant.Status.AdminSecretRef.Name,
	)
	if err != nil {
		return fmt.Errorf("failed to fetch tenant credentials: %w", err)
	}

	log.V(1).Info("Successfully fetched tenant credentials")

	// Initialize tenant client.
	tenantClient, err := grid.InitTenantClient(username, password, s3Tenant.Status.GridEndpoint, s3Tenant.Status.TenantID)
	if err != nil {
		return fmt.Errorf("failed to initialize tenant client: %w", err)
	}

	rctx.TenantClient = tenantClient

	log.V(1).Info("Successfully initialized tenant client")
	return nil
}

func (r *S3AccessReconciler) reconcileUserAndAccessKeys(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileUserAndAccessKeys")

	identifier := r.getAccessIdentifier(rctx)

	if err := grid.CreateAccessUserIfNotExists(ctx, identifier, rctx.PolicyStatements, rctx.TenantClient); err != nil {
		return fmt.Errorf("failed to create access user: %w", err)
	}

	log.V(1).Info("Access user and group reconciled")
	return nil
}

func (r *S3AccessReconciler) applyPolicyToUser(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "applyPolicyToUser")

	identifier := r.getAccessIdentifier(rctx)

	if err := grid.UpdateAccessGroupPolicy(ctx, identifier, rctx.PolicyStatements, rctx.TenantClient); err != nil {
		return fmt.Errorf("failed to update access group policy: %w", err)
	}

	log.V(1).Info("Policy applied to access group")
	return nil
}

func (r *S3AccessReconciler) reconcileSecret(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileSecret")

	identifier := r.getAccessIdentifier(rctx)
	secretName := rctx.S3Access.Status.SecretRef.Name

	// Check if credentials already exist.
	_, _, err := kube.FetchKeyPairFromSecret(ctx, r.Client, rctx.S3Access.Namespace, secretName)
	if err != nil {
		log.V(1).Info("No existing credentials secret, creating new S3 credentials")
		return r.createAccessKeypair(ctx, rctx, identifier, secretName, false)
	}

	// Check if credential rotation is requested via annotation.
	if rctx.S3Access.Annotations != nil {
		if rctx.S3Access.Annotations[s3v1alpha1.AnnotationRecreateAccessKeypairs] == "true" {
			log.Info("Recreating S3 access keypair due to annotation")
			if err := r.createAccessKeypair(ctx, rctx, identifier, secretName, true); err != nil {
				return err
			}
			// Remove annotation after successful rotation.
			delete(rctx.S3Access.Annotations, s3v1alpha1.AnnotationRecreateAccessKeypairs)
			rctx.ObjectUpdated = true
			return nil
		}
	}

	log.V(1).Info("Credentials secret already exists")
	return nil
}

func (r *S3AccessReconciler) createAccessKeypair(ctx context.Context, rctx *s3AccessReconcileContext, identifier string, secretName string, recreate bool) error {
	log := log.FromContext(ctx).WithValues("function", "createAccessKeypair")

	var accessKeyId, accessKey, secretKey string
	var err error

	if recreate {
		r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeNormal, "CredentialsRotating", "Rotating S3 credentials for access %s", rctx.S3Access.Name)

		accessKeyId, accessKey, secretKey, err = grid.RecreateAccessS3Credentials(ctx, identifier, rctx.S3Access.Status.AccessKeyId, rctx.TenantClient)
		if err != nil {
			r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeWarning, "CredentialsRotationFailed", "Failed to rotate S3 credentials: %v", err)
			return fmt.Errorf("failed to recreate S3 credentials: %w", err)
		}

		r.Recorder.Event(rctx.S3Access, corev1.EventTypeNormal, "CredentialsRotated", "S3 credentials rotated successfully")
	} else {
		accessKeyId, accessKey, secretKey, err = grid.CreateAccessS3Credentials(ctx, identifier, rctx.TenantClient)
		if err != nil {
			return fmt.Errorf("failed to create S3 credentials: %w", err)
		}
	}

	if err := kube.CreateKeyPairSecret(ctx, r.Client, rctx.S3Access.Namespace, secretName, accessKey, secretKey, rctx.S3Access, rctx.S3Access.Kind); err != nil {
		return fmt.Errorf("failed to create credentials secret: %w", err)
	}

	rctx.S3Access.Status.AccessKeyId = accessKeyId
	log.V(1).Info("S3 credentials stored in secret", "recreate", recreate)
	return nil
}

func (r *S3AccessReconciler) reconcileSecretRefs(ctx context.Context, rctx *s3AccessReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileSecretRefs")

	// Determine desired secret name: spec override or default.
	desiredName := fmt.Sprintf("s3access-%s-s3-keypair", rctx.S3Access.Name)
	if rctx.S3Access.Spec.SecretRef != nil && rctx.S3Access.Spec.SecretRef.Name != "" {
		desiredName = rctx.S3Access.Spec.SecretRef.Name
	}

	// First reconcile: just set status and return.
	if rctx.S3Access.Status.SecretRef == nil || rctx.S3Access.Status.SecretRef.Name == "" {
		rctx.S3Access.Status.SecretRef = &corev1.LocalObjectReference{Name: desiredName}
		log.V(1).Info("Secret reference reconciled", "secretName", desiredName)
		return nil
	}

	// No change.
	oldName := rctx.S3Access.Status.SecretRef.Name
	if oldName == desiredName {
		return nil
	}

	// Secret rename requested: move data from old secret to new secret.
	log.Info("Secret rename detected", "oldSecret", oldName, "newSecret", desiredName)

	if err := r.moveSecret(ctx, rctx, oldName, desiredName); err != nil {
		r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeWarning, "SecretRenameFailed", "Failed to rename secret from %s to %s: %v", oldName, desiredName, err)
		return fmt.Errorf("failed to rename secret from %s to %s: %w", oldName, desiredName, err)
	}

	r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeWarning, "SecretRenamed", "Secret renamed from %s to %s — workloads referencing the old secret name must be updated", oldName, desiredName)
	rctx.S3Access.Status.SecretRef = &corev1.LocalObjectReference{Name: desiredName}
	log.Info("Secret renamed successfully", "oldSecret", oldName, "newSecret", desiredName)
	return nil
}

// moveSecret copies credential data from the old secret to a new secret, then deletes the old one.
func (r *S3AccessReconciler) moveSecret(ctx context.Context, rctx *s3AccessReconcileContext, oldName, newName string) error {
	log := log.FromContext(ctx).WithValues("function", "moveSecret")

	// Read credentials from old secret.
	accessKey, secretKey, err := kube.FetchKeyPairFromSecret(ctx, r.Client, rctx.S3Access.Namespace, oldName)
	if err != nil {
		return fmt.Errorf("failed to read old secret %s: %w", oldName, err)
	}

	// Create the new secret with the same data.
	if err := kube.CreateKeyPairSecret(ctx, r.Client, rctx.S3Access.Namespace, newName, accessKey, secretKey, rctx.S3Access, rctx.S3Access.Kind); err != nil {
		return fmt.Errorf("failed to create new secret %s: %w", newName, err)
	}
	log.V(1).Info("New secret created", "secretName", newName)

	// Delete old secret only after new one was created successfully.
	if err := kube.DeleteSecret(ctx, r.Client, rctx.S3Access.Namespace, oldName); err != nil {
		log.Error(err, "Failed to delete old secret after rename, it can be cleaned up manually", "secretName", oldName)
	}

	return nil
}

// deriveReadiness uses conditions to derive the overall readiness state and phase of the S3Access.
// This method should be called after doReconcile to properly set the Ready condition and phase.
func (r *S3AccessReconciler) deriveReadiness(ctx context.Context, rctx *s3AccessReconcileContext) {
	log := log.FromContext(ctx).WithValues("function", "deriveReadiness")

	// Track condition states for readiness derivation.
	reconciliation := false
	backendReady := false

	// Message that will show on the ready condition.
	message := ""

	// Check reconciliation condition.
	reconcileCondition := meta.FindStatusCondition(rctx.S3Access.Status.Conditions, s3v1alpha1.ConditionTypeReconcileSucceeded)
	if reconcileCondition != nil {
		if reconcileCondition.ObservedGeneration == rctx.S3Access.GetGeneration() {
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

	// Check backing resource (backend connection) ready condition.
	backendCondition := meta.FindStatusCondition(rctx.S3Access.Status.Conditions, s3v1alpha1.ContitionTypeBackingResourceReady)
	if backendCondition != nil {
		if backendCondition.ObservedGeneration == rctx.S3Access.GetGeneration() {
			if backendCondition.Status == metav1.ConditionTrue {
				log.V(1).Info("Backend connection is ready")
				message += ", Backend connection established"
				backendReady = true
			} else {
				log.V(1).Info("Backend connection is not ready")
				message += ", Backend connection not established"
			}
		} else {
			log.V(1).Info("Backend condition is not from the current generation")
			message += ", Backend condition is not from the current generation"
		}
	} else {
		log.V(1).Info("Backend condition not found")
		message += ", Backend connection pending"
	}

	// Derive readiness based on conditions.
	if reconciliation && backendReady {
		log.V(1).Info("S3Access is ready")
		rctx.S3Access.Status.Phase = s3v1alpha1.S3AccessPhaseReady
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, "S3AccessReady", message)
	} else if !reconciliation {
		log.V(1).Info("S3Access reconciliation failed")
		rctx.S3Access.Status.Phase = s3v1alpha1.S3AccessPhaseFailed
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "ReconcileFailed", message)
	} else if !backendReady {
		log.V(1).Info("S3Access backend not ready, access is pending")
		rctx.S3Access.Status.Phase = s3v1alpha1.S3AccessPhasePending
		r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "BackendNotReady", message)
	}
}

// findS3AccessesForS3Policy returns reconcile requests for all S3Accesses
// that reference the given S3Policy.
func (r *S3AccessReconciler) findS3AccessesForS3Policy(ctx context.Context, obj client.Object) []ctrl.Request {
	log := log.FromContext(ctx)

	accessList := &s3v1alpha1.S3AccessList{}
	if err := r.List(ctx, accessList, client.InNamespace(obj.GetNamespace())); err != nil {
		log.Error(err, "Failed to list S3Accesses for policy change")
		return nil
	}

	var requests []ctrl.Request
	for _, access := range accessList.Items {
		for _, ref := range access.Status.AppliedPolicyRefs {
			kind := ref.Kind
			if kind == "" {
				kind = "S3Policy"
			}
			if kind == "S3Policy" && ref.Name == obj.GetName() {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{
						Name:      access.Name,
						Namespace: access.Namespace,
					},
				})
				break
			}
		}
	}

	return requests
}

// findS3AccessesForGlobalS3Policy returns reconcile requests for all S3Accesses
// that reference the given GlobalS3Policy.
func (r *S3AccessReconciler) findS3AccessesForGlobalS3Policy(ctx context.Context, obj client.Object) []ctrl.Request {
	log := log.FromContext(ctx)

	accessList := &s3v1alpha1.S3AccessList{}
	if err := r.List(ctx, accessList); err != nil {
		log.Error(err, "Failed to list S3Accesses for global policy change")
		return nil
	}

	var requests []ctrl.Request
	for _, access := range accessList.Items {
		for _, ref := range access.Status.AppliedPolicyRefs {
			if ref.Kind == "GlobalS3Policy" && ref.Name == obj.GetName() {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{
						Name:      access.Name,
						Namespace: access.Namespace,
					},
				})
				break
			}
		}
	}

	return requests
}

// accessConnectionDetailsSecretName returns the user-supplied destination secret name
// when set, otherwise the default `s3access-<access>-connection-details`. The kind prefix
// avoids collisions with S3Bucket default names that would otherwise share the namespace.
// This is a separate Secret from the operator-managed credentials Secret (status.SecretRef)
// — it is a user-facing projection of the credentials plus the bucket's endpoint info.
func accessConnectionDetailsSecretName(access *s3v1alpha1.S3Access) string {
	if access.Spec.ConnectionDetails != nil && access.Spec.ConnectionDetails.DestinationSecret != "" {
		return access.Spec.ConnectionDetails.DestinationSecret
	}
	return fmt.Sprintf("s3access-%s-connection-details", access.Name)
}

// reconcileConnectionDetails dispatches to apply or remove based on
// spec.connectionDetails.mode. The synthesized Secret is a projection only — the raw
// keypair is owned by reconcileSecret.
func (r *S3AccessReconciler) reconcileConnectionDetails(ctx context.Context, rctx *s3AccessReconcileContext) error {
	mode := s3v1alpha1.ConnectionDetailsModeAll
	if rctx.S3Access.Spec.ConnectionDetails != nil && rctx.S3Access.Spec.ConnectionDetails.Mode != "" {
		mode = rctx.S3Access.Spec.ConnectionDetails.Mode
	}
	if mode == s3v1alpha1.ConnectionDetailsModeDisabled {
		return r.reconcileConnectionDetailsRemove(ctx, rctx)
	}
	return r.reconcileConnectionDetailsApply(ctx, rctx)
}

func (r *S3AccessReconciler) reconcileConnectionDetailsApply(ctx context.Context, rctx *s3AccessReconcileContext) error {
	logger := log.FromContext(ctx).WithValues("function", "reconcileConnectionDetailsApply")

	// Bail out without flipping conditions when prerequisites aren't ready: earlier
	// reconcile steps (or the bucket reconciler) will fill them in and we'll be requeued.
	if rctx.S3Access.Status.SecretRef == nil || rctx.S3Access.Status.SecretRef.Name == "" {
		logger.V(1).Info("Skipping connection details: credentials secret not yet known")
		return nil
	}
	if rctx.S3Bucket == nil || rctx.S3Bucket.Status.BucketName == "" {
		logger.V(1).Info("Skipping connection details: bucket not yet ready")
		return nil
	}
	if rctx.S3Bucket.Status.S3EndpointConfig == nil || rctx.S3Bucket.Status.S3EndpointConfig.URL == "" {
		logger.V(1).Info("Skipping connection details: endpoint URL not yet known")
		return nil
	}

	accessKey, secretKey, err := kube.FetchKeyPairFromSecret(ctx, r.Client, rctx.S3Access.Namespace, rctx.S3Access.Status.SecretRef.Name)
	if err != nil {
		return fmt.Errorf("failed to read access keypair: %w", err)
	}

	inputs := kube.ConnectionDetailsInputs{
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		EndpointURL:     rctx.S3Bucket.Status.S3EndpointConfig.URL,
		Region:          rctx.S3Bucket.Status.Region,
		BucketName:      rctx.S3Bucket.Status.BucketName,
	}

	data := kube.BuildConnectionDetailsData(inputs)
	desiredSecretName := accessConnectionDetailsSecretName(rctx.S3Access)

	// Detect a destinationSecret rename so we can clean up the old projection after the
	// new one is durably written. status.ConnectionDetailsSecretRef tracks the actually-
	// deployed name; spec.connectionDetails.destinationSecret tracks the desired name.
	previousSecretName := ""
	if rctx.S3Access.Status.ConnectionDetailsSecretRef != nil {
		previousSecretName = rctx.S3Access.Status.ConnectionDetailsSecretRef.Name
	}
	renamed := previousSecretName != "" && previousSecretName != desiredSecretName

	changed, err := kube.ReconcileOwnedSecret(ctx, r.Client, rctx.S3Access.Namespace, desiredSecretName, data, rctx.S3Access)
	if err != nil {
		if errors.Is(err, kube.ErrSecretNotOwned) {
			msg := fmt.Sprintf("Secret %s/%s exists and is not owned by this S3Access; refusing to overwrite. Set spec.connectionDetails.destinationSecret to a different name or remove the foreign secret.",
				rctx.S3Access.Namespace, desiredSecretName)
			r.setCondition(rctx.S3Access, s3v1alpha1.ConditionTypeOwnershipConflict, metav1.ConditionTrue, "ConnectionDetailsConflict", msg)
			r.Recorder.Event(rctx.S3Access, corev1.EventTypeWarning, parentctrl.EventOwnershipConflict, msg)
			return nil
		}
		r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeWarning, parentctrl.EventConnectionDetailsApplyFailed,
			"Failed to apply connection details secret %s: %v", desiredSecretName, err)
		return fmt.Errorf("failed to reconcile connection details secret: %w", err)
	}

	// Rename: drop the old owned Secret only after the new one is durably written. We
	// only ever delete Secrets we own, so foreign Secrets are left alone.
	if renamed {
		if _, derr := kube.DeleteOwnedSecret(ctx, r.Client, rctx.S3Access.Namespace, previousSecretName, rctx.S3Access); derr != nil {
			logger.Error(derr, "Failed to delete previous connection-details secret after rename; it can be cleaned up manually", "secret", previousSecretName)
		} else {
			r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeWarning, "ConnectionDetailsSecretRenamed",
				"Connection-details secret renamed from %s to %s — workloads referencing the old name must be updated", previousSecretName, desiredSecretName)
		}
	}

	rctx.S3Access.Status.ConnectionDetailsSecretRef = &corev1.LocalObjectReference{Name: desiredSecretName}
	if changed {
		r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeNormal, parentctrl.EventConnectionDetailsApplied,
			"Successfully applied connection details secret %s", desiredSecretName)
	}
	return nil
}

func (r *S3AccessReconciler) reconcileConnectionDetailsRemove(ctx context.Context, rctx *s3AccessReconcileContext) error {
	logger := log.FromContext(ctx).WithValues("function", "reconcileConnectionDetailsRemove")

	if rctx.S3Access.Status.ConnectionDetailsSecretRef == nil {
		return nil
	}

	secretName := rctx.S3Access.Status.ConnectionDetailsSecretRef.Name
	deleted, err := kube.DeleteOwnedSecret(ctx, r.Client, rctx.S3Access.Namespace, secretName, rctx.S3Access)
	if err != nil {
		r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeWarning, parentctrl.EventConnectionDetailsRemoveFailed,
			"Failed to remove connection details secret %s: %v", secretName, err)
		return fmt.Errorf("failed to delete connection details secret: %w", err)
	}

	rctx.S3Access.Status.ConnectionDetailsSecretRef = nil
	if deleted {
		r.Recorder.Eventf(rctx.S3Access, corev1.EventTypeNormal, parentctrl.EventConnectionDetailsRemoved,
			"Successfully removed connection details secret %s", secretName)
	}
	logger.V(1).Info("Connection details remove reconciliation completed", "deleted", deleted)
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *S3AccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&s3v1alpha1.S3Access{}).
		Owns(&corev1.Secret{}).
		Watches(&s3v1alpha1.S3Policy{}, handler.EnqueueRequestsFromMapFunc(r.findS3AccessesForS3Policy)).
		Watches(&s3v1alpha1.GlobalS3Policy{}, handler.EnqueueRequestsFromMapFunc(r.findS3AccessesForGlobalS3Policy)).
		Named("s3access").
		Complete(r)
}
