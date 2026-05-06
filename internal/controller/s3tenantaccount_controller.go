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
)

// S3TenantAccountReconciler reconciles a S3TenantAccount object.
type S3TenantAccountReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Recorder          record.EventRecorder
	OperatorNamespace string // Namespace where the operator is running, used for creating secrets
}

type accountReconcileContext struct {
	// currently reconciled S3TenantAccount.
	Account *s3v1alpha1.S3TenantAccount
	// s3tenant binding the account.
	S3Tenant *s3v1alpha1.S3Tenant
	// Storagegrid reference.
	SG *s3v1alpha1.StorageGrid
	// tracking if annotations were update to send the update request after the status was updated.
	// this is used to avoid sending multiple updates in the same reconciliation loop.
	ObjectUpdated bool
	// track if we need to requeue the reconciliation loop.
	DoRequeue bool
	// requeAfter is used to requeue the reconciliation loop after a certain time.
	RequeAfter metav1.Duration
	// keep the backend tenant and usage to avoid multiple calls to the backend.
	// while making sure they are consistent within one reconciliation loop.
	BackendTenant      *grid.Tenant
	BackendTenantUsage *grid.TenantUsage
	// clients need to be unique per reconciliation loop.
	TenantClient *grid.TenantClient
	GridClient   *grid.GridClient
}

// conditionCheck is a struct to hold the condition checks for the account.
// will be used when deriving the phase of the account.
type conditionCheck struct {
	conditionType string
	successMsg    string
	failMsg       string
	resultVar     *bool
}

// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenantaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenantaccounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenantaccounts/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenants,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to.
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:.
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile

func (r *S3TenantAccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	account := &s3v1alpha1.S3TenantAccount{}

	log.V(1).Info(fmt.Sprintf("Reconciliation loop started for S3TenantAccount %s", req.Name))

	// Get the object to reconcile on.
	if err := r.Get(ctx, req.NamespacedName, account); err != nil {
		log.Error(err, "Failed to retrieve resource")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.V(1).Info("Successfully retrieved resource in question")

	rctx := &accountReconcileContext{
		Account:       account,
		S3Tenant:      &s3v1alpha1.S3Tenant{},
		SG:            &s3v1alpha1.StorageGrid{},
		ObjectUpdated: false,
		DoRequeue:     false,
		RequeAfter:    metav1.Duration{Duration: 0},
	}

	statusBase := account.DeepCopy()
	err := r.doReconcile(ctx, rctx)

	// use conditions to derive the readiness state.
	r.derivePhase(ctx, rctx)

	// update the annotations if they were updated.
	// needs to be done before the status is updated because we lose the annotations otherwise.
	if rctx.ObjectUpdated {
		// creating a deep copy of the status to avoid modifying the original object.
		statusCopy := account.DeepCopy()

		log.V(1).Info("Annotations were updated, updating the object")
		if updateErr := r.Update(ctx, account); updateErr != nil {
			log.Error(updateErr, "Failed to update annotations")
			if err != nil {
				err = fmt.Errorf("reconciliation failed: %w, failed to update annotations: %w", err, updateErr)
			} else {
				err = fmt.Errorf("failed to update annotations: %w", updateErr)
			}
		} else {
			log.V(1).Info("Annotations updated successfully")
		}

		account.Status = statusCopy.Status // restore the status from the copy
	}

	// update the status of the account if it was updated during reconciliation.
	if updateErr := r.Status().Patch(ctx, rctx.Account, client.MergeFrom(statusBase)); updateErr != nil {
		log.Error(updateErr, "Failed to update status, requeuing")
		if err == nil {
			// no error occurred during reconciliation, but status update failed.
			rctx.DoRequeue = true // we need to requeue to ensure the status is updated
		}
		// if an error already occurred during reconciliation, we just return that error.
	}

	if rctx.RequeAfter.Duration > 0 {
		log.V(1).Info("Requeuing reconciliation after duration", "duration", rctx.RequeAfter.Duration)
		return ctrl.Result{RequeueAfter: rctx.RequeAfter.Duration}, err
	}

	return ctrl.Result{Requeue: rctx.DoRequeue}, err
}

func (r *S3TenantAccountReconciler) doReconcile(ctx context.Context, rctx *accountReconcileContext) (err error) {
	// Get the StorageGrid reference from the S3Tenant.
	err = r.reconcileGridReference(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "GridReferenceReconcileFailed", fmt.Sprintf("Failed to reconcile StorageGrid reference: %s", err.Error()))
		return err
	}

	// make sure the most recent deletionpolicy is set.
	err = r.reconcileDeletionPolicy(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "DeletionPolicyReconcileFailed", fmt.Sprintf("Failed to reconcile deletion policy: %s", err.Error()))
		// we should return on error here because the deletion policy is crucial for the deletion process.
		return err
	}

	// get the tenant binding for this account.
	err = r.reconcileS3TenantReference(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "S3TenantReferenceReconcileFailed", fmt.Sprintf("Failed to reconcile S3Tenant reference: %s", err.Error()))
	}

	// do some trivial reconciliation tasks.
	r.reconcileGridEndpoint(ctx, rctx)
	r.reconcileSecretRefs(ctx, rctx)
	r.reconcileTenantName(ctx, rctx)

	// make sure the backing grid is ready before proceeding.
	err = r.reconcileGridReadiness(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "GridReadinessCheckFailed", fmt.Sprintf("StorageGrid %s not ready", rctx.SG.Name))
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "GridReadinessCheckFailed", fmt.Sprintf("Failed to reconcile StorageGrid %s: %s", rctx.SG.Name, err.Error()))
		return err
	}

	// initialize the grid client.
	err = r.initGridClient(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "GridClientInitFailed", fmt.Sprintf("Failed to initialize grid client: %s", err.Error()))
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "GridClientInitFailed", fmt.Sprintf("Failed to initialize grid client due to %s even though it reports ready, do you have the correct secret?", err.Error()))
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBackendConnectionFailed,
			fmt.Sprintf("Failed to connect to StorageGrid backend: %v", err))
		return err
	}
	r.setCondition(rctx.Account, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionTrue, "StorageGridReadyAndAccessible", fmt.Sprintf("StorageGrid %s is ready", rctx.SG.Name))

	// Check if we're recovering from a connection failure
	backendCondition := meta.FindStatusCondition(rctx.Account.Status.Conditions, s3v1alpha1.ContitionTypeBackingResourceReady)
	if backendCondition != nil && backendCondition.Status == metav1.ConditionFalse && backendCondition.Reason == "GridClientInitFailed" {
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBackendConnectionRestored,
			fmt.Sprintf("Successfully reconnected to StorageGrid %s", rctx.SG.Name))
	}

	// examine DeletionTimestamp to determine if object is under deletion.
	err = r.reconcileFinalizerAndDlelete(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "FinalizingFailed", fmt.Sprintf("Failed to reconcile delete or finalizer %s", err.Error()))
		return err
	}
	if rctx.DoRequeue {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "FinalizerReconcileSucceeded", "Finalizer and deletion timestamp reconciled successfully, requeuing")
		return nil
	}

	err = r.reconcilePolicyDeletionTimestamp(ctx, rctx.Account)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "DeletionTimestampReconcileFailed", fmt.Sprintf("Failed to reconcile deletion timestamp: %s", err.Error()))
		return err
	}

	// make sure proper tenantclass is set before creation.
	// update the tenantclass for network access.
	err = r.reconcileS3TenantClass(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantClassReconcileFailed", fmt.Sprintf("Failed to reconcile tenant class: %s", err.Error()))
		return err
	}

	// check for recreate annotation
	err = r.reconcileRecreateAnnotation(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "RecreateAnnotationReconcileFailed", fmt.Sprintf("Failed on reconciling recreate annotation: %s", err.Error()))
		return err
	}

	// Check if tenant has been created or imported
	createdCondition := meta.FindStatusCondition(rctx.Account.Status.Conditions, s3v1alpha1.ConditionTypeCreated)

	if createdCondition == nil {
		// Tenant not yet created - check for import annotation first
		if importTenantID := rctx.Account.Annotations[s3v1alpha1.AnnotationImportTenant]; importTenantID != "" {
			err := r.reconcileImport(ctx, rctx, importTenantID)
			if err != nil {
				r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "ImportFailed", fmt.Sprintf("Failed to import tenant: %s", err.Error()))
				return err
			}
			// Import handles its own status updates, conditions, and requeue
			return nil
		}

		// Create new tenant
		err := r.reconcileCreate(ctx, rctx)
		if err != nil {
			r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "CreateFailed", fmt.Sprintf("Failed to create tenant: %s", err.Error()))
			return err
		}

		return nil
	}

	// initialize tenant client and fetch the tenant from the backend.
	err = r.initTenantClient(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantClientInitFailed", fmt.Sprintf("Failed to initialize tenant client due to %s", err.Error()))
		return err
	}

	err = r.fetchTenant(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantFetchFailed", fmt.Sprintf("Failed to fetch tenant from backend due to %s", err.Error()))
		return err
	}

	// Always check for ownership changes
	err = r.reconcileOwnership(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "OwnershipReconcileFailed", fmt.Sprintf("Unsure we are owner of the resource, please check: %s", err.Error()))
		return err
	}

	r.reconcileRegions(ctx, rctx)

	err = r.reconcileTenantUsage(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantUsageFailed", fmt.Sprintf("Failed to fetch current tenant usage due to %s", err.Error()))
	}

	err = r.reconcileTenantDescription(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "StorageQuotaReconcileFailed", fmt.Sprintf("Failed to reconcile storage quota: %s", err.Error()))
	}

	err = r.reconcileTenantNameUpdate(ctx, rctx)
	if err != nil {
		// no need to stop reconciliation here, just set the condition.
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantUsageReconcileFailed", fmt.Sprintf("Failed to reconcile tenant usage: %s", err.Error()))
	}

	err = r.reconcileStorageQuota(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "StorageQuotaReconcileFailed", fmt.Sprintf("Failed to reconcile storage quota: %s", err.Error()))
	}

	err = r.reconcileObjectLockPolicy(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "ObjectLockPolicyReconcileFailed", fmt.Sprintf("Failed to reconcile object lock policy: %s", err.Error()))
	}

	r.evaluateQuotaConditions(ctx, rctx)

	// reconcile tenant admin credentials.
	err = r.reconcileTenantAdminCredentials(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantAdminCredentialsReconcileFailed", fmt.Sprintf("Failed to reconcile tenant admin credentials: %s", err.Error()))
	}

	// reconcile s3 credentials and recreate if necessary.
	if err := r.reconcileS3Credentials(ctx, rctx); err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "S3CredentialUpdateFailed", fmt.Sprintf("Failed to reconcile S3 credentials: %s", err.Error()))
	}

	// reconciliation ran successfully.
	r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "ReconcileSucceeded", "Reconciliation completed successfully")

	return nil
}

func (r *S3TenantAccountReconciler) setCondition(account *s3v1alpha1.S3TenantAccount, condType string, status metav1.ConditionStatus, reason string, message string) {
	// add or update with given condition.
	condition := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: account.GetGeneration(),
	}

	meta.SetStatusCondition(&account.Status.Conditions, condition)
}

func (r *S3TenantAccountReconciler) reconcileDeletionPolicy(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileDeletionPolicy")

	// if no deletion policy is set, we set it to the default from the storagegrid.
	var policy *s3v1alpha1.TenantDeletionPolicy
	if rctx.Account.Spec.TenantDeletionPolicy != nil {
		policy = rctx.Account.Spec.TenantDeletionPolicy
		log.V(1).Info("Using deletion policy from spec", "policy", policy.Policy)
	} else {
		policy = rctx.SG.Status.DefaultTenantDeletionPolicy
		log.V(1).Info("Using default deletion policy from StorageGrid", "policy", policy.Policy)
	}

	// Only update status if the policy is different or not set.
	if rctx.Account.Status.TenantDeletionPolicy == nil {
		log.V(1).Info("Setting initial tenant deletion policy in status", "policy", policy.Policy)
		rctx.Account.Status.TenantDeletionPolicy = policy.DeepCopy()
	} else {
		// Check if policies are different using deep comparison.
		if !reflect.DeepEqual(rctx.Account.Status.TenantDeletionPolicy, policy) {
			log.V(1).Info("Updating tenant deletion policy in status",
				"oldPolicy", rctx.Account.Status.TenantDeletionPolicy.Policy,
				"newPolicy", policy.Policy)
			rctx.Account.Status.TenantDeletionPolicy = policy.DeepCopy()
		} else {
			log.V(1).Info("Tenant deletion policy unchanged, skipping update")
		}
	}

	return nil
}

func (r *S3TenantAccountReconciler) reconcileS3TenantReference(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// get the S3Tenant reference from the account.
	if rctx.Account.Spec.S3TenantRef == nil {
		log.V(1).Info("No S3Tenant reference found in account spec")

		// if the spec is missing it can be either:.
		// 1. the S3Tenant was never created and the account is used in isolation
		// 2. the referenced S3Tenant was deleted
		if rctx.Account.Status.S3TenantRef == nil {
			// -> 1. Both spec and status are nil - account is available for binding
			r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeBound, metav1.ConditionFalse, "S3TenantNotBound", "No S3 Tenant is binding this account")
			log.V(1).Info("Account is available for binding (no spec or status reference)")
			return nil
		}

		// -> 2. Spec is nil but status has reference - the S3Tenant was deleted
		log.V(1).Info(fmt.Sprintf("Found reference to S3Tenant %s in account status but not in spec, tenant was deleted", rctx.Account.Status.S3TenantRef.Name))
		// use the S3TenantRef from the status to fetch the S3Tenant.
		if err := r.Get(ctx, types.NamespacedName{Name: rctx.Account.Status.S3TenantRef.Name, Namespace: rctx.Account.Status.S3TenantRef.Namespace}, rctx.S3Tenant); err != nil {
			// if we cannot find the s3tenant it was probably deleted.
			if client.IgnoreNotFound(err) == nil {
				r.reconcileDeletedTenant(ctx, rctx)
			} else {
				// any other error is a real error, we cannot proceed.
				log.Error(err, fmt.Sprintf("Failed to retrieve S3Tenant %s from API", rctx.Account.Status.S3TenantRef.Name))
				return err
			}
		} else if !rctx.S3Tenant.DeletionTimestamp.IsZero() {
			// S3Tenant exists but is being deleted (has deletion timestamp)
			log.V(1).Info("S3Tenant is being deleted (has deletion timestamp), treating as deleted")
			r.reconcileDeletedTenant(ctx, rctx)
		}

		return nil
	}

	// if the S3TenantRef is set, we need to fetch the S3Tenant.
	if err := r.Get(ctx, types.NamespacedName{Name: rctx.Account.Spec.S3TenantRef.Name, Namespace: rctx.Account.Spec.S3TenantRef.Namespace}, rctx.S3Tenant); err != nil {
		log.Info(fmt.Sprintf("Failed to retrieve S3Tenant %s from API", rctx.Account.Spec.S3TenantRef.Name))

		if client.IgnoreNotFound(err) == nil {
			// Check if status is also nil.
			// The status should only be nil if:
			// The account was pre-bound by an admin (setting spec but not status)
			// meaning we can ignore this error for now, as the S3Tenant might not be created yet.
			if rctx.Account.Status.S3TenantRef == nil {
				log.V(1).Info("Spec.S3TenantRef is set but status not yet bound - account available for claiming")
				r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeBound, metav1.ConditionFalse, "S3TenantNotBound", fmt.Sprintf("Account pre-bound to S3Tenant %s, awaiting binding", rctx.Account.Spec.S3TenantRef.Name))
				return nil
			}

			// If we cannot find the s3tenant and we do have a status reference,
			// We assume it was likely deleted
			// This should actually never happen - but we handle it gracefully just in case.
			log.V(1).Info("Referenced S3Tenant not found, was it deleted?")
			r.reconcileDeletedTenant(ctx, rctx)
			return nil
		} else {
			// any other error is a real error, we cannot proceed.
			return err
		}
	}

	// Check if the S3Tenant is being deleted (has deletion timestamp)
	if !rctx.S3Tenant.DeletionTimestamp.IsZero() {
		log.V(1).Info("S3Tenant is being deleted (has deletion timestamp), treating as deleted")
		r.reconcileDeletedTenant(ctx, rctx)
		return nil
	}

	// once fetched, we need to always update the S3TenantRef in the account status.
	if rctx.Account.Status.S3TenantRef == nil || rctx.Account.Status.S3TenantRef.Name != rctx.S3Tenant.Name {
		r.bindTenant(ctx, rctx)
	}

	// Both spec and status are set - account is bound
	log.V(1).Info(fmt.Sprintf("Account bound to S3Tenant %s", rctx.Account.Spec.S3TenantRef.Name))
	r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeBound, metav1.ConditionTrue, "S3TenantBound", fmt.Sprintf("Bound by S3 Tenant %s/%s", rctx.S3Tenant.Namespace, rctx.S3Tenant.Name))

	return nil
}

// bindTenant sets the binding state on the account status when an S3Tenant is confirmed.
// It populates the S3TenantRef, BoundTenant display field, clears any retention state
// from previous unbind cycles, and removes stale deletion timestamps.
func (r *S3TenantAccountReconciler) bindTenant(ctx context.Context, rctx *accountReconcileContext) {
	log := log.FromContext(ctx)
	log.V(1).Info("Binding account to S3Tenant", "tenant", rctx.S3Tenant.Name, "namespace", rctx.S3Tenant.Namespace)

	rctx.Account.Status.S3TenantRef = &corev1.ObjectReference{
		Name:       rctx.S3Tenant.Name,
		Namespace:  rctx.S3Tenant.Namespace,
		UID:        rctx.S3Tenant.UID,
		Kind:       rctx.S3Tenant.Kind,
		APIVersion: rctx.S3Tenant.APIVersion,
	}
	rctx.Account.Status.BoundTenant = fmt.Sprintf("%s/%s", rctx.S3Tenant.Namespace, rctx.S3Tenant.Name)

	// Clear retention state from previous unbind cycles
	meta.RemoveStatusCondition(&rctx.Account.Status.Conditions, s3v1alpha1.ConditionTypeRetained)
	meta.RemoveStatusCondition(&rctx.Account.Status.Conditions, s3v1alpha1.ConditionTypeRetainThenDelete)
	meta.RemoveStatusCondition(&rctx.Account.Status.Conditions, s3v1alpha1.ConditionTypeDeletionTimestampReached)

	// Make sure no deletion timestamp is configured from any old bindings.
	if rctx.Account.Status.DeletionTimestamp != nil {
		log.V(1).Info("Clearing deletion timestamp, S3Tenant is bound")
		rctx.Account.Status.DeletionTimestamp = nil
	}
}

// unbindTenant clears the binding state from the account when the S3Tenant is gone.
// It clears both status and spec S3TenantRef, the BoundTenant display field,
// resets the in-memory tenant, and marks the object as updated.
func (r *S3TenantAccountReconciler) unbindTenant(ctx context.Context, rctx *accountReconcileContext) {
	log := log.FromContext(ctx)
	log.V(1).Info("Unbinding account from S3Tenant")

	rctx.Account.Status.S3TenantRef = nil
	rctx.Account.Status.BoundTenant = ""
	rctx.Account.Spec.S3TenantRef = nil
	rctx.S3Tenant = &s3v1alpha1.S3Tenant{} // clear in-memory tenant so reconcileSecretRefs resolves the correct namespace
	rctx.ObjectUpdated = true

	r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeBound, metav1.ConditionFalse, "S3TenantNotBound", "S3Tenant was deleted, account is now unbound")
}

func (r *S3TenantAccountReconciler) reconcileDeletedTenant(ctx context.Context, rctx *accountReconcileContext) {
	log := log.FromContext(ctx)

	// Unbind the tenant reference — common to all deletion policies
	r.unbindTenant(ctx, rctx)

	// Apply policy-specific behavior
	switch rctx.Account.Status.TenantDeletionPolicy.Policy {
	case s3v1alpha1.TenantDeletionPolicyDelete:
		log.V(1).Info("Tenant deletion policy is set to delete, deleting the account")
		// Set deletion timestamp to now, so the account is deleted immediately.
		rctx.Account.Status.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	case s3v1alpha1.TenantDeletionPolicyRetain:
		log.V(1).Info("Tenant deletion policy is set to retain")
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeRetained, metav1.ConditionTrue, "TenantRetained", "S3Tenant was deleted, but account is retained due to deletion policy")
	case s3v1alpha1.TenantDeletionPolicyRetainThenDelete:
		log.V(1).Info("Tenant deletion policy is set to retain then delete")

		retentionDuration := rctx.Account.Status.TenantDeletionPolicy.RetentionDuration

		// Calculate the deletion timestamp based on the retention duration.
		if retentionDuration != nil {
			log.V(1).Info(fmt.Sprintf("Setting deletion timestamp to %s", retentionDuration.String()))
			rctx.Account.Status.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Add(retentionDuration.Duration)}
			r.emitEvent(rctx, corev1.EventTypeNormal, EventDeletionPolicyApplied,
				fmt.Sprintf("Deletion policy 'RetainThenDelete' applied: tenant will be retained until %s", rctx.Account.Status.DeletionTimestamp.Format(time.RFC3339)))
		} else {
			log.V(1).Info("No retention duration set, using default deletion timestamp")
			rctx.Account.Status.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
			r.emitEvent(rctx, corev1.EventTypeNormal, EventDeletionPolicyApplied,
				"Deletion policy 'RetainThenDelete' applied with no retention period")
		}

		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeRetainThenDelete, metav1.ConditionTrue, "TenantRetainThenDelete", fmt.Sprintf("S3Tenant was deleted, account will be retained until  %s", rctx.Account.Status.DeletionTimestamp.String()))
	}
}

func (r *S3TenantAccountReconciler) deleteAccount(ctx context.Context, account *s3v1alpha1.S3TenantAccount) error {
	log := log.FromContext(ctx)
	if err := r.Delete(ctx, account); err != nil {
		log.Error(err, fmt.Sprintf("Failed to delete S3TenantAccount %s after S3Tenant deletion", account.Name))
		return err
	}

	log.V(1).Info(fmt.Sprintf("Successfully deleted S3TenantAccount %s after S3Tenant deletion", account.Name))
	return nil
}

func (r *S3TenantAccountReconciler) reconcileGridReference(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	log.V(1).Info("Fetching backing storagegrid")

	if err := r.Get(ctx, types.NamespacedName{Name: rctx.Account.Spec.StorageGridRef.Name}, rctx.SG); err != nil {
		log.Error(err, fmt.Sprintf("Failed to retrieve StorageGrid from API %s", rctx.Account.Spec.StorageGridRef.Name))
		return err
	}

	log.V(1).Info(fmt.Sprintf("Successfully retrieved StorageGrid %s", rctx.SG.Name))

	return nil
}

func (r *S3TenantAccountReconciler) reconcilePolicyDeletionTimestamp(ctx context.Context, account *s3v1alpha1.S3TenantAccount) error {
	log := log.FromContext(ctx)

	// Recovery: if a previous delete attempt failed but the condition was already set,
	// re-issue the delete. This self-heals stuck accounts in phase Deleting.
	if account.DeletionTimestamp.IsZero() {
		deletionReached := meta.FindStatusCondition(account.Status.Conditions, s3v1alpha1.ConditionTypeDeletionTimestampReached)
		if deletionReached != nil && deletionReached.Status == metav1.ConditionTrue && account.Status.S3TenantRef == nil {
			log.Info("Recovering stuck account: DeletionTimestampReached is True but account not yet deleted, retrying delete")
			return r.deleteAccount(ctx, account)
		}
	}

	// if the deletion timestamp is set, we need to check if the S3Tenant is still bound.
	if !account.Status.DeletionTimestamp.IsZero() {
		log.V(1).Info("Deletion timestamp is set, checking if S3Tenant is still bound")

		// this should actually never happen, since we always reconcile the S3Tenant reference before this method is called.
		if account.Status.S3TenantRef != nil {
			log.V(1).Info("S3Tenant is still bound, not deleting the account and removing deletion timestamp")
			// if the S3Tenant is still bound, we do not delete the account.
			account.Status.DeletionTimestamp = nil
			return nil
		}

		// if the S3Tenant is not bound anymore, we can delete the account if the timestamp is reached.
		if account.Status.DeletionTimestamp.Time.Before(metav1.Now().Time) {
			log.V(1).Info("Deletion timestamp is reached, deleting the account")
			r.setCondition(account, s3v1alpha1.ConditionTypeDeletionTimestampReached, metav1.ConditionTrue, "DeletionTimestampReached", fmt.Sprintf("Deletion timestamp reached at %s, deleting account", account.Status.DeletionTimestamp.String()))
			account.Status.Phase = s3v1alpha1.PhaseDeleting
			// Keep DeletionTimestamp set — it will be retried on next reconcile if deleteAccount fails.
			// Once the k8s object is deleted, it doesn't matter.
			return r.deleteAccount(ctx, account)
		}
		log.V(1).Info("Deletion timestamp not yet reached, waiting")
	}

	return nil
}

func (r *S3TenantAccountReconciler) derivePhase(ctx context.Context, rctx *accountReconcileContext) {
	log := log.FromContext(ctx)

	// all of these conditions need to be be in state true for this account to be considered ready.
	reconciliation := false
	backendReady := false
	sufficientQuota := false

	// If the account is ready and bound it will be in phase bound.
	bound := false

	// any of these conditions being true means that the account was bound but the tenant was deleted.
	// These need to be checked separately because they are only set when the tenant was deleted.
	retain := false
	retainThenDelete := false
	deletion := false

	// message that will show on the ready condition.
	message := ""

	// define a struct to hold the condition checks.

	readyChecks := []conditionCheck{
		{
			conditionType: s3v1alpha1.ConditionTypeReconcileSucceeded,
			successMsg:    "Reconciliation succeeded",
			failMsg:       "Reconciliation failed",
			resultVar:     &reconciliation,
		},
		{
			conditionType: s3v1alpha1.ContitionTypeBackingResourceReady,
			successMsg:    "Backing resource is ready",
			failMsg:       "Backing resource is not ready",
			resultVar:     &backendReady,
		},
		{
			conditionType: s3v1alpha1.ConditionTypeQuotaSufficient,
			successMsg:    "Tenant has sufficient quota",
			failMsg:       "Tenant does not have sufficient quota",
			resultVar:     &sufficientQuota,
		},
		{
			conditionType: s3v1alpha1.ConditionTypeBound,
			successMsg:    "S3Tenant is binding this account",
			failMsg:       "No S3Tenant is binding this account",
			resultVar:     &bound,
		},
	}

	message += r.checkRecurringConditions(ctx, rctx.Account, &readyChecks)

	// check for the retention conditions.
	retentionChecks := []conditionCheck{
		{
			conditionType: s3v1alpha1.ConditionTypeRetained,
			successMsg:    "S3Tenant is retained",
			failMsg:       "",
			resultVar:     &retain,
		},
		{
			conditionType: s3v1alpha1.ConditionTypeRetainThenDelete,
			successMsg:    "S3Tenant is retained then deleted",
			failMsg:       "",
			resultVar:     &retainThenDelete,
		},
		{
			conditionType: s3v1alpha1.ConditionTypeDeletionTimestampReached,
			successMsg:    "Deletion timestamp reached",
			failMsg:       "",
			resultVar:     &deletion,
		},
	}

	message += r.checkRetentionConditions(ctx, rctx.Account, &retentionChecks)

	// workout if the account is ready.
	if reconciliation && backendReady && sufficientQuota {
		log.V(1).Info("Accunt is ready")
		if bound {
			log.V(1).Info("Account is bound to S3Tenant")
			rctx.Account.Status.Phase = s3v1alpha1.PhaseBound
		} else {
			log.V(1).Info("Account is not bound to S3Tenant but ready")
			rctx.Account.Status.Phase = s3v1alpha1.PhaseReady
		}
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, "S3TenantAccountReady", message)
	} else {
		log.V(1).Info("S3Tenant is not ready")
		rctx.Account.Status.Phase = s3v1alpha1.PhaseFailed
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "S3TenantAccountReady", message)
	}

	// Handle retention-related phase transitions.
	// Note: Retain policy doesn't override the phase - account returns to PhaseReady when unbound.
	// The ConditionTypeRetained provides observability that it came from a deleted tenant.
	if retainThenDelete {
		log.V(1).Info("S3Tenant is retained then deleted, setting phase to RetainingThenDeleting")
		rctx.Account.Status.Phase = s3v1alpha1.PhaseRetainThenDelete
		// on retain then delete we have to make sure it is requeued when the deletion timestamp is reached for deletion.
		rctx.RequeAfter = metav1.Duration{Duration: rctx.Account.Status.DeletionTimestamp.Time.Sub(metav1.Now().Time)}
	} else if deletion {
		log.V(1).Info("S3Tenant deletion timestamp reached, setting phase to Deleting")
		rctx.Account.Status.Phase = s3v1alpha1.PhaseDeleting
	}
}

func (r *S3TenantAccountReconciler) checkRecurringConditions(ctx context.Context, account *s3v1alpha1.S3TenantAccount, checks *[]conditionCheck) string {
	log := log.FromContext(ctx)

	message := ""
	for _, check := range *checks {
		// find the condition in the account status.
		condition := meta.FindStatusCondition(account.Status.Conditions, check.conditionType)

		switch {
		case condition == nil:
			log.V(1).Info(fmt.Sprintf("Condition %s not found", check.conditionType))
			message += fmt.Sprintf("Condition %s not found, ", check.conditionType)

		case condition.ObservedGeneration != account.GetGeneration():
			log.V(1).Info(fmt.Sprintf("Condition %s is not from the current generation", check.conditionType))
			message += fmt.Sprintf("Condition %s is not from the current generation, ", check.conditionType)

		case condition.Status == metav1.ConditionTrue:
			log.V(1).Info(check.successMsg)
			message += fmt.Sprintf("%s, ", check.successMsg)
			*check.resultVar = true
		case condition.Status == metav1.ConditionFalse:
			log.V(1).Info(check.failMsg)
			message += fmt.Sprintf("%s, ", check.failMsg)

		default:
			log.V(1).Info(fmt.Sprintf("Condition %s has unknown status %s", check.conditionType, condition.Status))
			message += fmt.Sprintf("Condition %s has unknown status %s, ", check.conditionType, condition.Status)
		}
	}

	// remove the last comma and space from the message.
	if len(message) > 2 {
		message = message[:len(message)-2]
	} else {
		message = "No conditions found"
	}
	return message
}

// retentionConditions are only set once when the linked tenant was deleted.
// thus the checking has to differ from the recurring conditions.
// Also the message only needs to be updated once the conditions exist.
func (r *S3TenantAccountReconciler) checkRetentionConditions(ctx context.Context, account *s3v1alpha1.S3TenantAccount, checks *[]conditionCheck) string {
	log := log.FromContext(ctx)

	message := ""
	for _, check := range *checks {
		// find the condition in the account status.
		condition := meta.FindStatusCondition(account.Status.Conditions, check.conditionType)

		switch {
		case condition == nil:
			log.V(1).Info(fmt.Sprintf("Condition %s not found", check.conditionType))

		case condition.Status == metav1.ConditionTrue:
			log.V(1).Info(check.successMsg)
			message += check.successMsg
			*check.resultVar = true

		default:
			log.V(1).Info(fmt.Sprintf("Condition %s has unknown status %s", check.conditionType, condition.Status))
			message += fmt.Sprintf("Condition %s has unknown status %s, ", check.conditionType, condition.Status)
		}
	}

	return message
}

func (r *S3TenantAccountReconciler) reconcileFinalizerAndDlelete(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// check if the object is being deleted.
	if rctx.Account.DeletionTimestamp.IsZero() {
		// if the object is not being deleted, add our finalizer if it is not already present.
		if !controllerutil.ContainsFinalizer(rctx.Account, tenantFinalizer) {
			controllerutil.AddFinalizer(rctx.Account, tenantFinalizer)
			log.V(1).Info("Adding finalizer to S3Tenant")
			rctx.DoRequeue = true // we need to requeue to ensure the finalizer is added
			rctx.ObjectUpdated = true
			return nil
		}
	} else {
		// the object is being deleted.
		log.V(1).Info("Object is being deleted")
		if controllerutil.ContainsFinalizer(rctx.Account, tenantFinalizer) {
			// our finalizer is present, so lets handle any external dependency.
			if err := r.finalize(ctx, rctx); err != nil {
				log.Error(err, "Failed to finalize tenant")
				return err
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(rctx.Account, tenantFinalizer)
			log.V(1).Info("Removing finalizer from S3Tenant")
			rctx.DoRequeue = true // we need to requeue to ensure the finalizer is removed on the next reconciliation
			rctx.ObjectUpdated = true
			return nil
		}

		// no finalizer is present, so we can proceed with deletion.
		log.V(1).Info("Finalizer not present, deletion can proceed without further action")
		return nil
	}

	return nil
}

func (r *S3TenantAccountReconciler) reconcileTenantName(ctx context.Context, rctx *accountReconcileContext) {
	log := log.FromContext(ctx)

	prefix := ""
	if rctx.SG.Spec.TenantPrefix == s3v1alpha1.TenantPrefixNamespace {
		// if the tenant prefix is set to namespace, we use the namespace of the binding S3Tenant.
		if rctx.S3Tenant.Name == "" {
			log.V(1).Info("S3Tenant is not set, cannot use namespace as prefix, leaving empty")
		} else {
			prefix = fmt.Sprintf("%s-", rctx.S3Tenant.Namespace)
		}
	}

	// use resource name as default tenant name.
	tenantName := rctx.Account.Name
	// use spec name if set and if not use s3tenant name if available.
	if rctx.Account.Spec.Name != "" {
		tenantName = rctx.Account.Spec.Name
	} else if rctx.S3Tenant.Name != "" {
		tenantName = rctx.S3Tenant.Name
	}

	// combine prefix and name if prefix is set.
	tenantName = fmt.Sprintf("%s%s", prefix, tenantName)

	if rctx.Account.Status.DesiredTenantBackendName != &tenantName {
		log.V(1).Info(fmt.Sprintf("Setting desired tenant backend name %s", tenantName))
		rctx.Account.Status.DesiredTenantBackendName = &tenantName
	}
}

func (r S3TenantAccountReconciler) reconcileTenantNameUpdate(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// if no name is yet observed on the backend the tenant is not created yet.
	// skipping update on the backend.
	if *rctx.Account.Status.ObservedTenantBackendName == "" {
		return nil
	}

	// if the observedName has a value we can compare it with the desired name.
	if *rctx.Account.Status.DesiredTenantBackendName != *rctx.Account.Status.ObservedTenantBackendName {
		log.V(1).Info("Tenant name does not match, updating from %s to %s", *rctx.Account.Status.ObservedTenantBackendName, *rctx.Account.Status.DesiredTenantBackendName)
		r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantUpdating,
			fmt.Sprintf("Updating tenant name to '%s'", *rctx.Account.Status.DesiredTenantBackendName))

		err := grid.UpdateName(ctx, *rctx.Account.Status.DesiredTenantBackendName, rctx.BackendTenant, rctx.GridClient)
		if err != nil {
			log.Error(err, "Failed to update tenant name")
			r.emitEvent(rctx, corev1.EventTypeWarning, EventTenantUpdateFailed,
				fmt.Sprintf("Failed to update tenant name: %v", err))
			return err
		}
	}

	// update the status with the observed name.
	rctx.Account.Status.ObservedTenantBackendName = rctx.Account.Status.DesiredTenantBackendName

	return nil
}

func (r *S3TenantAccountReconciler) reconcileTenantDescription(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)
	log.V(1).Info("Reconciling tenant description")

	namespace := r.OperatorNamespace
	if rctx.S3Tenant != nil {
		namespace = rctx.S3Tenant.Namespace
	}

	// combine default fields for the description.
	userDescription := ""
	if rctx.Account.Spec.Description != nil {
		userDescription = *rctx.Account.Spec.Description
	}

	description := map[string]string{
		// USER FIELDS
		"user_description": userDescription,

		// OPERATOR MANAGED FIELDS
		"managed_by":           "storagegrid-operator",
		"cr_uid":               string(rctx.Account.UID),
		"cr_name":              rctx.Account.Name,
		"kubernetes_namespace": namespace,
		"last_reconciled":      time.Now().Format(time.RFC3339),
	}

	// add any additional metadata specified by the user.
	if rctx.Account.Spec.AdditionalTenantMetadata != nil {
		for key, value := range rctx.Account.Spec.AdditionalTenantMetadata {
			description[key] = value
		}
	}

	// convert the description to a string.
	descriptionStr := ""
	for key, value := range description {
		if descriptionStr == "" {
			descriptionStr = fmt.Sprintf("%s:%s ", key, value)
			continue
		}

		descriptionStr = fmt.Sprintf("%s\n%s:%s ", descriptionStr, key, value)
	}

	// update description if changed.
	if &rctx.Account.Status.Description != &descriptionStr {
		log.V(1).Info(fmt.Sprintf("Setting tenant description to %s", descriptionStr))
		r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantUpdating,
			"Updating tenant description and metadata")

		err := grid.UpdateDescription(ctx, descriptionStr, rctx.BackendTenant, rctx.GridClient)
		if err != nil {
			log.Error(err, "Failed to update tenant description")
			r.emitEvent(rctx, corev1.EventTypeWarning, EventTenantUpdateFailed,
				fmt.Sprintf("Failed to update description: %v", err))
			return err
		}
		log.V(1).Info("Tenant description updated successfully")
		r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantUpdated,
			"Successfully updated tenant description and metadata")
		rctx.Account.Status.Description = descriptionStr
	} else {
		log.V(1).Info("Tenant description is already up to date, skipping update")
	}

	return nil
}

func (r *S3TenantAccountReconciler) reconcileGridEndpoint(ctx context.Context, rctx *accountReconcileContext) {
	log := log.FromContext(ctx)

	if rctx.Account.Status.GridEndpoint == "" {
		log.V(1).Info("Grid endpoint is not set, setting it to the StorageGrid endpoint")
		rctx.Account.Status.GridEndpoint = rctx.SG.Spec.ManagementEndpoint
	} else if rctx.Account.Status.GridEndpoint != rctx.SG.Spec.ManagementEndpoint {
		log.V(1).Info("Grid endpoint has changed, updating from %s to %s", rctx.Account.Status.GridEndpoint, rctx.SG.Spec.ManagementEndpoint)
		rctx.Account.Status.GridEndpoint = rctx.SG.Spec.ManagementEndpoint
	}
}

func (r *S3TenantAccountReconciler) reconcileGridReadiness(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	if !rctx.SG.Status.Ready {
		log.Error(fmt.Errorf("StorageGrid is not ready"), fmt.Sprintf("StorageGrid %s not ready", rctx.Account.Spec.StorageGridRef.Name))
		return fmt.Errorf("StorageGrid %s is not ready", rctx.Account.Spec.StorageGridRef.Name)
	}

	log.Info(fmt.Sprintf("StorageGrid %s is ready", rctx.Account.Spec.StorageGridRef.Name))
	return nil
}

func (r *S3TenantAccountReconciler) initGridClient(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	log.V(1).Info("Fetching credentials for client")
	username, password, err := kube.FetchCredentialsFromSecret(ctx, r.Client, rctx.SG.Spec.SecretRef.Namespace, rctx.SG.Spec.SecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch credentials")
		return err
	}

	log.V(1).Info("Initializing grid client if not initialized")
	client, err := grid.InitGridClient(username, password, rctx.SG.Spec.ManagementEndpoint)
	if err != nil {
		log.Error(err, "Failed to initialize grid client")
		return err
	}

	rctx.GridClient = client

	return nil
}

func (r *S3TenantAccountReconciler) initTenantClient(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// get credentials from the secret.
	username, password, err := kube.FetchCredentialsFromSecret(ctx, r.Client, rctx.Account.Status.RootSecretRef.Namespace, rctx.Account.Status.RootSecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch credentials from root secret")
		return err
	}

	// use initialize the tenant client.
	client, err := grid.InitTenantClient(username, password, rctx.Account.Status.GridEndpoint, rctx.Account.Status.TenantID)
	if err != nil {
		log.Error(err, "Failed to initialize tenant client")
		return err
	}

	rctx.TenantClient = client

	return nil
}

func (r *S3TenantAccountReconciler) fetchTenant(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	log.V(1).Info(fmt.Sprintf("Fetching tenant with id %s", rctx.Account.Status.TenantID))
	tenant, err := grid.FetchTenant(ctx, rctx.Account.Status.TenantID, rctx.GridClient)
	if err != nil {
		// if this fails either storageGrid is not available or tenant does not exist on the backend.
		log.Error(err, "Failed to fetch tenant")

		// if it's missing on the backend we need to set bucketCount and objectCount to 0 to allow tenant deletion.
		if err.Error() == grid.ErrTenantNotFound {
			log.Info("Tenant does not exist on the backend, setting bucketCount and objectCount to 0")
			rctx.Account.Status.TenantUsage.BucketCount = 0
			rctx.Account.Status.TenantUsage.ObjectCount = 0
		}
		return err
	}

	rctx.BackendTenant = tenant
	log.V(1).Info(fmt.Sprintf("Successfully fetched tenant with id %s", rctx.Account.Status.TenantID))

	return nil
}

func (r *S3TenantAccountReconciler) reconcileRecreateAnnotation(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// check if annotations exist and exit if unset.
	if rctx.Account.Annotations == nil {
		return nil
	}

	if _, ok := rctx.Account.Annotations[s3v1alpha1.AnnotationRecreateTenant]; ok {
		log.V(1).Info("Recreate tenant annotation found, recreating tenant")

		// remove tenant created condition to allow re-creation.
		meta.RemoveStatusCondition(&rctx.Account.Status.Conditions, s3v1alpha1.ConditionTypeCreated)

		// remove the annotation.
		delete(rctx.Account.Annotations, s3v1alpha1.AnnotationRecreateTenant)
		log.V(1).Info("Successfully recreated credentials and removed annotation")

		rctx.ObjectUpdated = true
	}

	return nil
}

func (r *S3TenantAccountReconciler) reconcileCreate(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	log.V(1).Info("Tenant does not exist yet, creating it")
	r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantCreating,
		fmt.Sprintf("Creating tenant '%s' in StorageGrid backend", *rctx.Account.Status.DesiredTenantBackendName))

	// initial description will include some details about the account in case the reconcileDescription fails later.
	initialDescription := fmt.Sprintf("Created by storagegrid-operator for S3TenantAccount %s in namespace %s at %s", rctx.Account.Name, rctx.Account.Namespace, time.Now().Format(time.RFC3339))

	tenantID, password, err := grid.CreateTenant(ctx, *rctx.Account.Status.DesiredTenantBackendName, initialDescription, rctx.Account.Spec.StorageQuota.Value(), desiredAllowComplianceMode(rctx.Account.Spec.S3ObjectLock), desiredMaxRetentionDays(rctx.Account.Spec.S3ObjectLock), rctx.GridClient)
	if err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventTenantCreateFailed,
			fmt.Sprintf("Failed to create tenant: %v", err))
		log.Error(err, "Failed to create tenant")
		return err
	}

	// store credentials in a secret.
	err = kube.CreateCredentialSecret(ctx, r.Client, rctx.Account.Status.RootSecretRef.Namespace, rctx.Account.Status.RootSecretRef.Name, "root", password, rctx.Account, rctx.Account.Kind)
	if err != nil {
		log.Error(err, "Failed to create secret")
		return err
	}

	// Add protective finalizer to root secret to prevent premature deletion during namespace teardown.
	if err := kube.AddSecretFinalizer(ctx, r.Client, rctx.Account.Status.RootSecretRef.Namespace, rctx.Account.Status.RootSecretRef.Name); err != nil {
		log.Error(err, "Failed to add finalizer to root secret")
		return err
	}

	// Immediately persist the tenant ID and Created condition via a status PATCH.
	// We use Patch (not Update) because it does NOT require a matching resourceVersion,
	// so it cannot conflict even if the S3Tenant controller modified this object
	// while CreateTenant() was in-flight. This is the standard pattern used by
	// cert-manager, Cluster API, and Crossplane to safely persist status after
	// irreversible external side effects.
	//
	// Without this, there's a race: the deferred Status().Update() in Reconcile()
	// can fail with a conflict (stale resourceVersion), the Created condition is
	// never persisted, and the next reconcile creates a SECOND tenant — overwriting
	// the root-credentials secret with the wrong password → permanent 401 errors.
	base := rctx.Account.DeepCopy()

	// update status fields before marking as created.
	rctx.Account.Status.ObservedTenantBackendName = rctx.Account.Status.DesiredTenantBackendName
	rctx.Account.Status.TenantID = tenantID
	rctx.Account.Status.TenantManagerURL = fmt.Sprintf("%s?accountId=%s", rctx.SG.Spec.ManagementEndpoint, tenantID)
	r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "TenantCreated", fmt.Sprintf("Created Tenant with id %s in backend", tenantID))

	if patchErr := r.Status().Patch(ctx, rctx.Account, client.MergeFrom(base)); patchErr != nil {
		log.Error(patchErr, "Failed to patch Created status after tenant creation")
		return patchErr
	}

	r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantCreated,
		fmt.Sprintf("Successfully created tenant with ID %s", tenantID))

	log.Info(fmt.Sprintf("Tenant created successfully, ID: %s", tenantID))

	// set requeue to true
	rctx.DoRequeue = true

	return nil
}

func (r *S3TenantAccountReconciler) reconcileRegions(ctx context.Context, rctx *accountReconcileContext) {
	log := log.FromContext(ctx)

	if rctx.Account.Spec.DefaultBucketRegion != nil {
		log.V(1).Info("Default region is set, checking if it needs to be updated in status")
		if rctx.Account.Status.DefaultBucketRegion == "" {
			log.V(1).Info("Default region is not set in status, setting it to the specified default region in spec")
			rctx.Account.Status.DefaultBucketRegion = *rctx.Account.Spec.DefaultBucketRegion
		}
		if rctx.Account.Status.DefaultBucketRegion != *rctx.Account.Spec.DefaultBucketRegion {
			log.V(1).Info("Default region has changed, updating from %s to %s", rctx.Account.Status.DefaultBucketRegion, *rctx.Account.Spec.DefaultBucketRegion)
			rctx.Account.Status.DefaultBucketRegion = *rctx.Account.Spec.DefaultBucketRegion
		}
	} else {
		if rctx.Account.Status.DefaultBucketRegion == "" {
			log.V(1).Info("Default region is not set in status, setting it to the StorageGrid default region")
			rctx.Account.Status.DefaultBucketRegion = rctx.SG.Status.DefaultBucketRegion
		} else if rctx.Account.Status.DefaultBucketRegion != rctx.SG.Status.DefaultBucketRegion {
			log.V(1).Info("Default region has changed, updating from %s to %s", rctx.Account.Status.DefaultBucketRegion, rctx.SG.Status.DefaultBucketRegion)
			rctx.Account.Status.DefaultBucketRegion = rctx.SG.Status.DefaultBucketRegion
		}
	}

	// propagate regions from grid to tenant if changed.
	slices.Sort(rctx.Account.Status.Regions)
	slices.Sort(rctx.SG.Status.Regions)
	if !slices.Equal(rctx.Account.Status.Regions, rctx.SG.Status.Regions) {
		log.V(1).Info("Updating region list in tenant status")
		rctx.Account.Status.Regions = rctx.SG.Status.Regions
	}
}

func (r *S3TenantAccountReconciler) reconcileSecretRefs(ctx context.Context, rctx *accountReconcileContext) {
	log := log.FromContext(ctx).WithValues("function", "reconcileSecretRefs", "account", rctx.Account.Name)
	log.V(1).Info("Starting secret references reconciliation")

	// Determine namespaces for different secret types.
	userSecretNamespace := r.determineUserSecretNamespace(ctx, rctx)
	platformSecretNamespace := r.determinePlatformSecretNamespace(ctx, rctx)
	secretBaseName := r.determineSecretBaseName(ctx, rctx)

	log.V(1).Info("Secret namespaces determined",
		"userNamespace", userSecretNamespace,
		"platformNamespace", platformSecretNamespace,
		"baseName", secretBaseName)

	// Update secret references in status. Old secrets are deleted inline if reference changes.
	// Root secret stays in platform namespace (no change on binding).
	r.updateSecretRef(ctx, &rctx.Account.Status.RootSecretRef,
		rctx.Account.Spec.RootSecretRef,
		platformSecretNamespace,
		fmt.Sprintf("%s-root-credentials", rctx.Account.Name))

	// Admin credentials - namespace changes on binding/unbinding.
	r.updateSecretRef(ctx, &rctx.Account.Status.AdminSecretRef,
		rctx.Account.Spec.AdminSecretRef,
		userSecretNamespace,
		fmt.Sprintf("%s-admin-credentials", secretBaseName))

	// S3 admin keys - namespace changes on binding/unbinding.
	r.updateSecretRef(ctx, &rctx.Account.Status.S3AdminKeysSecretRef,
		rctx.Account.Spec.S3AdminKeysSecretRef,
		userSecretNamespace,
		fmt.Sprintf("%s-s3-admin-keypair", secretBaseName))

	log.V(1).Info("Secret references reconciliation completed")
}

// determineUserSecretNamespace returns the namespace for user-facing secrets.
// Priority: S3Tenant namespace > spec.secretNamespace > operator namespace.
func (r *S3TenantAccountReconciler) determineUserSecretNamespace(ctx context.Context, rctx *accountReconcileContext) string {
	log := log.FromContext(ctx).WithValues("function", "determineUserSecretNamespace")

	if rctx.S3Tenant != nil && rctx.S3Tenant.Name != "" {
		log.V(1).Info("Using S3Tenant namespace for user secrets",
			"namespace", rctx.S3Tenant.Namespace,
			"s3TenantName", rctx.S3Tenant.Name)
		return rctx.S3Tenant.Namespace
	}
	if rctx.Account.Spec.SecretNamespace != "" {
		log.V(1).Info("Using spec.secretNamespace for user secrets",
			"namespace", rctx.Account.Spec.SecretNamespace)
		return rctx.Account.Spec.SecretNamespace
	}
	log.V(1).Info("Using operator namespace for user secrets (fallback)",
		"namespace", r.OperatorNamespace)
	return r.OperatorNamespace
}

// determinePlatformSecretNamespace returns the namespace for platform team secrets.
// Priority: spec.secretNamespace > operator namespace.
func (r *S3TenantAccountReconciler) determinePlatformSecretNamespace(ctx context.Context, rctx *accountReconcileContext) string {
	log := log.FromContext(ctx).WithValues("function", "determinePlatformSecretNamespace")

	if rctx.Account.Spec.SecretNamespace != "" {
		log.V(1).Info("Using spec.secretNamespace for platform secrets",
			"namespace", rctx.Account.Spec.SecretNamespace)
		return rctx.Account.Spec.SecretNamespace
	}
	log.V(1).Info("Using operator namespace for platform secrets",
		"namespace", r.OperatorNamespace)
	return r.OperatorNamespace
}

// determineSecretBaseName returns the base name for secrets.
func (r *S3TenantAccountReconciler) determineSecretBaseName(ctx context.Context, rctx *accountReconcileContext) string {
	log := log.FromContext(ctx).WithValues("function", "determineSecretBaseName")

	if rctx.S3Tenant != nil && rctx.S3Tenant.Name != "" {
		log.V(1).Info("Using S3Tenant name as secret base name",
			"baseName", rctx.S3Tenant.Name,
			"s3TenantName", rctx.S3Tenant.Name)
		return rctx.S3Tenant.Name
	}
	log.V(1).Info("Using account name as secret base name",
		"baseName", rctx.Account.Name)
	return rctx.Account.Name
}

// updateSecretRef updates a secret reference and deletes the old secret if the reference changes.
func (r *S3TenantAccountReconciler) updateSecretRef(
	ctx context.Context,
	statusRef **corev1.ObjectReference,
	specRef *corev1.ObjectReference,
	namespace string,
	defaultName string) {
	log := log.FromContext(ctx).WithValues("function", "updateSecretRef")

	var name string
	if specRef != nil && specRef.Name != "" {
		name = specRef.Name
		log.V(1).Info("Using secret name from spec",
			"name", name,
			"namespace", namespace,
			"source", "spec")
	} else {
		name = defaultName
		log.V(1).Info("Using default secret name",
			"name", name,
			"namespace", namespace,
			"source", "default")
	}

	newRef := &corev1.ObjectReference{
		Name:      name,
		Namespace: namespace,
	}

	// Only update if different.
	if *statusRef == nil {
		log.Info("Setting initial secret reference",
			"name", newRef.Name,
			"namespace", newRef.Namespace)
		*statusRef = newRef
		return
	}

	if (*statusRef).Name != newRef.Name || (*statusRef).Namespace != newRef.Namespace {
		// Delete old secret before updating reference.
		// Remove protective finalizer first to allow deletion during namespace teardown.
		log.Info("Secret reference changed, deleting old secret",
			"oldName", (*statusRef).Name,
			"oldNamespace", (*statusRef).Namespace,
			"newName", newRef.Name,
			"newNamespace", newRef.Namespace)
		if err := kube.RemoveSecretFinalizer(ctx, r.Client, (*statusRef).Namespace, (*statusRef).Name); err != nil {
			log.Error(err, "Failed to remove finalizer from old secret",
				"name", (*statusRef).Name,
				"namespace", (*statusRef).Namespace)
		}
		if err := kube.DeleteSecret(ctx, r.Client, (*statusRef).Namespace, (*statusRef).Name); err != nil {
			log.Error(err, "Failed to delete old secret",
				"name", (*statusRef).Name,
				"namespace", (*statusRef).Namespace)
		}
		*statusRef = newRef
		return
	}

	log.V(1).Info("Secret reference unchanged",
		"name", newRef.Name,
		"namespace", newRef.Namespace)
}

func (r *S3TenantAccountReconciler) reconcileStorageQuota(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// check if there was an update to the quota.
	if rctx.Account.Spec.StorageQuota.Value() != grid.GetConfiguredQuota(rctx.BackendTenant) {
		log.V(1).Info("Quota was updated, updating tenant")
		r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantUpdating,
			fmt.Sprintf("Updating storage quota to %s", rctx.Account.Spec.StorageQuota.String()))

		// update tenant.
		err := grid.UpdateQuota(ctx, rctx.Account.Spec.StorageQuota.Value(), rctx.BackendTenant, rctx.GridClient)

		if err != nil {
			log.Error(err, "Failed to update tenant quota")
			r.emitEvent(rctx, corev1.EventTypeWarning, EventTenantUpdateFailed,
				fmt.Sprintf("Failed to update quota: %v", err))
			// revert to original value.
			return err
		}

		r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantUpdated,
			fmt.Sprintf("Successfully updated storage quota to %s", rctx.Account.Spec.StorageQuota.String()))
	}

	rctx.Account.Status.Quota.Limit = kube.ParseBytes(grid.GetConfiguredQuota(rctx.BackendTenant))
	log.V(1).Info(fmt.Sprintf("Updated tenant quota to %s", rctx.Account.Spec.StorageQuota.String()))
	return nil
}

// desiredAllowComplianceMode maps the spec object-lock mode to the backend tenant's
// AllowComplianceMode flag. Only Compliance grants the capability.
func desiredAllowComplianceMode(spec *s3v1alpha1.S3ObjectLockTenantSpec) bool {
	if spec == nil {
		return false
	}
	return spec.Mode == s3v1alpha1.S3ObjectLockModeCompliance
}

// desiredMaxRetentionDays maps the spec MaxRetentionInDays to the backend tenant's
// MaxRetentionDays. Returns nil when object lock is Disabled (no per-tenant cap).
func desiredMaxRetentionDays(spec *s3v1alpha1.S3ObjectLockTenantSpec) *int {
	if spec == nil || spec.Mode == "" || spec.Mode == s3v1alpha1.S3ObjectLockModeDisabled {
		return nil
	}
	v := int(spec.MaxRetentionInDays)
	return &v
}

// intPtrEqual compares two *int values for equality, treating nil == nil as equal.
func intPtrEqual(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// reconcileObjectLockPolicy syncs the backend tenant's S3 Object Lock policy fields
// (AllowComplianceMode, MaxRetentionDays) with the spec. Issues a single full PUT on drift.
func (r *S3TenantAccountReconciler) reconcileObjectLockPolicy(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	desiredAllow := desiredAllowComplianceMode(rctx.Account.Spec.S3ObjectLock)
	desiredMax := desiredMaxRetentionDays(rctx.Account.Spec.S3ObjectLock)

	currentAllow := grid.GetConfiguredAllowComplianceMode(rctx.BackendTenant)
	currentMax := grid.GetConfiguredMaxRetentionDays(rctx.BackendTenant)

	if currentAllow == desiredAllow && intPtrEqual(currentMax, desiredMax) {
		log.V(1).Info("Object lock policy already in sync")
		return nil
	}

	log.V(1).Info("Object lock policy drift detected, updating tenant",
		"currentAllowComplianceMode", currentAllow, "desiredAllowComplianceMode", desiredAllow,
		"currentMaxRetentionDays", currentMax, "desiredMaxRetentionDays", desiredMax)
	r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantUpdating,
		fmt.Sprintf("Updating S3 Object Lock policy (allowComplianceMode=%v, maxRetentionDays=%v)", desiredAllow, desiredMax))

	if err := grid.UpdateTenantObjectLockPolicy(ctx, desiredAllow, desiredMax, rctx.BackendTenant, rctx.GridClient); err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventTenantUpdateFailed,
			fmt.Sprintf("Failed to update S3 Object Lock policy: %v", err))
		return err
	}

	r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantUpdated, "Successfully updated S3 Object Lock policy")
	return nil
}

func (r *S3TenantAccountReconciler) reconcileTenantUsage(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)
	log.Info(fmt.Sprintf("Updating tenant usage of %s", rctx.Account.Name))

	// get tenant usage.
	usage, err := grid.FetchTenantUsage(ctx, rctx.Account.Status.TenantID, rctx.GridClient)
	if err != nil {
		return err
	}

	rctx.BackendTenantUsage = usage
	rctx.Account.Status.Quota.Used = kube.ParseBytes(grid.GetTenantUsedBytes(rctx.BackendTenantUsage))
	rctx.Account.Status.Quota.Limit = kube.ParseBytes(grid.GetConfiguredQuota(rctx.BackendTenant))
	rctx.Account.Status.TenantUsage.ObjectCount = grid.GetTenantObjectCount(rctx.BackendTenantUsage)
	rctx.Account.Status.TenantUsage.BucketCount = grid.GetBucketCount(rctx.BackendTenantUsage)

	return nil
}

func (r *S3TenantAccountReconciler) evaluateQuotaConditions(ctx context.Context, rctx *accountReconcileContext) {
	log := log.FromContext(ctx)
	account := rctx.Account

	// Calculate usage percentage
	usedBytes := account.Status.Quota.Used.Value()
	limitBytes := account.Status.Quota.Limit.Value()
	var usagePercent float64
	if limitBytes > 0 {
		usagePercent = float64(usedBytes) / float64(limitBytes) * 100
	}

	// check if quota was exceeded and mark tenant as ready or not.
	if usedBytes > limitBytes {
		log.Info(fmt.Sprintf("Quota exceeded: %s/%s (%.1f%%)", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String(), usagePercent))
		r.setCondition(account, s3v1alpha1.ConditionTypeQuotaSufficient, metav1.ConditionFalse, "QuotaExceeded", fmt.Sprintf("Quota exceeded: used %s is more than configured limit %s", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String()))

		// Emit quota exceeded event (only if condition changed)
		quotaCondition := meta.FindStatusCondition(account.Status.Conditions, s3v1alpha1.ConditionTypeQuotaSufficient)
		if quotaCondition == nil || quotaCondition.Status != metav1.ConditionFalse || quotaCondition.Reason != "QuotaExceeded" {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventQuotaExceeded,
				fmt.Sprintf("Storage quota exceeded: using %s of %s (%.1f%%)", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String(), usagePercent))
		}
	} else if usagePercent >= 80 {
		// Quota is sufficient but warn at 80% threshold
		log.V(1).Info(fmt.Sprintf("Quota warning: %s/%s (%.1f%%)", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String(), usagePercent))
		r.setCondition(account, s3v1alpha1.ConditionTypeQuotaSufficient, metav1.ConditionTrue, "QuotaSufficient", fmt.Sprintf("Quota sufficient: used %s is less than configured limit %s", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String()))

		// Emit warning event (only once when crossing threshold)
		quotaCondition := meta.FindStatusCondition(account.Status.Conditions, s3v1alpha1.ConditionTypeQuotaSufficient)
		if quotaCondition == nil || quotaCondition.Reason == "QuotaExceeded" {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventQuotaWarning,
				fmt.Sprintf("Storage quota warning: using %s of %s (%.1f%%), approaching limit", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String(), usagePercent))
		}
	} else {
		// Quota is normal
		r.setCondition(account, s3v1alpha1.ConditionTypeQuotaSufficient, metav1.ConditionTrue, "QuotaSufficient", fmt.Sprintf("Quota sufficient: used %s is less than configured limit %s", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String()))

		// Emit recovery event if previously had issues
		quotaCondition := meta.FindStatusCondition(account.Status.Conditions, s3v1alpha1.ConditionTypeQuotaSufficient)
		if quotaCondition != nil && quotaCondition.Status == metav1.ConditionFalse {
			r.emitEvent(rctx, corev1.EventTypeNormal, EventQuotaNormal,
				fmt.Sprintf("Storage usage back to normal: using %s of %s (%.1f%%)", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String(), usagePercent))
		}
	}
}

func (r *S3TenantAccountReconciler) reconcileS3TenantClass(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// Initialize S3EndpointConfig if nil
	if rctx.Account.Status.S3EndpointConfig == nil {
		rctx.Account.Status.S3EndpointConfig = &s3v1alpha1.S3EndpointConfig{}
	}

	// check if the S3TenantClass is set, if not set it to the default value.
	if rctx.Account.Status.S3EndpointConfig.S3TenantClassName == "" {
		// if unset we can just update the value.
		if rctx.Account.Spec.S3TenantClassName == "" {
			// if the S3TenantClassName is not set in the spec, we can use the default class.
			log.V(1).Info("S3TenantClassName is not set, using default class")
			rctx.Account.Status.S3EndpointConfig.S3TenantClassName = "default"
		} else {
			// if the S3TenantClassName is set in the spec, we can use that value.
			log.V(1).Info(fmt.Sprintf("S3TenantClassName is set to %s", rctx.Account.Status.S3EndpointConfig.S3TenantClassName))
			rctx.Account.Status.S3EndpointConfig.S3TenantClassName = rctx.Account.Spec.S3TenantClassName
		}
	}

	// if the was changed we need to be a bit more careful.
	if rctx.Account.Status.S3EndpointConfig.S3TenantClassName != rctx.Account.Spec.S3TenantClassName {
		// first verify that the annotation is set to allow changing the tenant class name.
		if rctx.Account.Annotations == nil {
			return fmt.Errorf("S3TenantClassName is set to %s, but annotation %s to allow change is not set, not updating", rctx.Account.Spec.S3TenantClassName, s3v1alpha1.AnnotationAllowTenantClassNameChange)
		}
		val, ok := rctx.Account.Annotations[s3v1alpha1.AnnotationAllowTenantClassNameChange]
		if !ok {
			return fmt.Errorf("S3TenantClassName is set to %s, but annotation %s to allow change is not set, not updating", rctx.Account.Spec.S3TenantClassName, s3v1alpha1.AnnotationAllowTenantClassNameChange)
		}
		if val != "true" {
			return fmt.Errorf("S3TenantClassName is set to %s, but annotation %s to allow change is %s instead of true, not updating", rctx.Account.Spec.S3TenantClassName, s3v1alpha1.AnnotationAllowTenantClassNameChange, val)
		}

		// if the annotation is set accordingly we can update the tenant class name.
		log.V(1).Info(fmt.Sprintf("S3TenantClassName was updated from %s to %s", rctx.Account.Status.S3EndpointConfig.S3TenantClassName, rctx.Account.Spec.S3TenantClassName))
		rctx.Account.Status.S3EndpointConfig.S3TenantClassName = rctx.Account.Spec.S3TenantClassName

		// reprotect the S3TenantClassName by setting the annotation.
		log.V(1).Info("Removing annotation to protect S3TenantClassName from further changes")
		delete(rctx.Account.Annotations, s3v1alpha1.AnnotationAllowTenantClassNameChange)
		rctx.ObjectUpdated = true
	}

	// get s3 endpoint config from the tenantclass.
	accountClass := &s3v1alpha1.S3TenantClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: rctx.Account.Status.S3EndpointConfig.S3TenantClassName}, accountClass); err != nil {
		log.Error(err, "Failed to retrieve S3TenantClass")
		return err
	}

	// Always copy S3EndpointConfig from TenantClass status (addresses can change).
	// This ensures we always reflect the current state of the gateway configuration.
	rctx.Account.Status.S3EndpointConfig = accountClass.Status.S3EndpointConfig

	return nil
}

func (r *S3TenantAccountReconciler) reconcileTenantAdminCredentials(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// check if the secret exists.
	_, _, err := kube.FetchCredentialsFromSecret(ctx, r.Client, rctx.Account.Status.AdminSecretRef.Namespace, rctx.Account.Status.AdminSecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch admin credentials from secret, creating new admin credentials")
		err = r.createTenantAdminCredentials(ctx, rctx, false)
		if err != nil {
			log.Error(err, "Failed to create tenant admin credentials")
			return err
		}

		return nil
	}

	log.V(1).Info("Admin credentials secret exists, checking if reset was requested")

	// if the annotation is set to recreate the tenant keypairs a new keypair needs to be created.
	if rctx.Account.Annotations[s3v1alpha1.AnnotationResetTenantAdminPassword] == "true" {
		log.Info("Recreating tenant admin credentials due to annotation")

		// trigger recreation of the S3 admin keypair.
		err := r.createTenantAdminCredentials(ctx, rctx, true)
		if err != nil {
			log.Error(err, "Failed to reset tenant admin keypair")
			return err
		}

		delete(rctx.Account.Annotations, s3v1alpha1.AnnotationResetTenantAdminPassword)
		rctx.ObjectUpdated = true
	}

	return nil
}

func (r *S3TenantAccountReconciler) createTenantAdminCredentials(ctx context.Context, rctx *accountReconcileContext, recreate bool) (err error) {
	log := log.FromContext(ctx)

	// initialize variables for access key ID, access key, and secret key.
	username, password := "", ""

	if recreate {
		log.V(1).Info("Recreating admin password")
		r.emitEvent(rctx, corev1.EventTypeNormal, EventCredentialsRotated,
			"Rotating tenant admin credentials")

		// recreate the admin password.
		username, password, err = grid.SetTenantAdminPassword(ctx, rctx.TenantClient)
		if err != nil {
			log.Error(err, "Failed to recreate S3 admin keypair")
			r.emitEvent(rctx, corev1.EventTypeWarning, EventCredentialsRotationFailed,
				fmt.Sprintf("Failed to rotate admin credentials: %v", err))
			return err
		}
	} else {
		log.V(1).Info("Creating initial S3 admin keypair")
		// create admin user.
		username, password, err = grid.CreateTenantAdminUser(ctx, rctx.TenantClient)
		if err != nil {
			log.Error(err, "Failed to create s3 keys")
			return err
		}
	}

	// Determine owner for the admin secret.
	// Always use S3Tenant as owner if it exists (user-facing secret).
	// Otherwise fall back to Account (platform-managed scenario).
	var owner metav1.Object
	ownerKind := "S3TenantAccount"
	if rctx.Account.Status.S3TenantRef != nil {
		owner = rctx.S3Tenant
		ownerKind = "S3Tenant"
	} else {
		owner = rctx.Account
	}

	err = kube.CreateCredentialSecret(ctx, r.Client, rctx.Account.Status.AdminSecretRef.Namespace, rctx.Account.Status.AdminSecretRef.Name, username, password, owner, ownerKind)
	if err != nil {
		log.Error(err, "Failed to create secret")
		return err
	}

	log.V(1).Info("Successfully created or updated tenant admin credentials secret")
	return nil
}

func (r *S3TenantAccountReconciler) reconcileS3Credentials(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// check if theres no admin access key ID set, then we need to recreate the keypair.
	_, _, err := kube.FetchKeyPairFromSecret(ctx, r.Client, rctx.Account.Status.S3AdminKeysSecretRef.Namespace, rctx.Account.Status.S3AdminKeysSecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch s3 keypair from secret, creating new s3 admin keypair")
		err := r.createS3AdminKeypair(ctx, rctx, false)
		if err != nil {
			log.Error(err, "Failed to create S3 admin keypair")
			return err
		}

		return nil
	}

	// if the annotation is set to recreate the tenant keypairs a new keypair needs to be created.
	if rctx.Account.Annotations != nil {
		if rctx.Account.Annotations[s3v1alpha1.AnnotationRecreateTenantKeypairs] == "true" {
			log.Info("Recreating S3 admin keypair due to annotation")

			// trigger recreation of the S3 admin keypair.
			err := r.createS3AdminKeypair(ctx, rctx, true)
			if err != nil {
				log.Error(err, "Failed to recreate S3 admin keypair")
				return err
			}

			delete(rctx.Account.Annotations, s3v1alpha1.AnnotationRecreateTenantKeypairs)
			rctx.ObjectUpdated = true
		}
	}

	return nil
}

func (r *S3TenantAccountReconciler) createS3AdminKeypair(ctx context.Context, rctx *accountReconcileContext, recreate bool) (err error) {
	log := log.FromContext(ctx)

	// initialize variables for access key ID, access key, and secret key.
	accessKeyId, accessKey, secretKey := "", "", ""

	if recreate {
		log.V(1).Info("Recreating S3 admin keypair")
		r.emitEvent(rctx, corev1.EventTypeNormal, EventCredentialsRotated,
			"Rotating S3 access keys")

		// recreate the S3 admin keypair with existing access key ID.
		accessKeyId, accessKey, secretKey, err = grid.RecreateAdminS3Credentials(ctx, rctx.Account.Status.S3AdminAccessKeyId, rctx.TenantClient)
		if err != nil {
			log.Error(err, "Failed to recreate S3 admin keypair")
			r.emitEvent(rctx, corev1.EventTypeWarning, EventCredentialsRotationFailed,
				fmt.Sprintf("Failed to rotate S3 access keys: %v", err))
			return err
		}
	} else {
		log.V(1).Info("Creating initial S3 admin keypair")
		// create s3 keys.
		accessKeyId, accessKey, secretKey, err = grid.CreateAdminS3Credentials(ctx, rctx.TenantClient)
		if err != nil {
			log.Error(err, "Failed to create s3 keys")
			return err
		}
	}

	// if tenantref is set use this as owner instead.
	var owner metav1.Object
	ownerKind := rctx.Account.Kind
	if rctx.Account.Status.S3TenantRef != nil {
		owner = rctx.S3Tenant
		ownerKind = rctx.S3Tenant.Kind
	} else {
		owner = rctx.Account
	}
	// store s3 keys in a secret.
	err = kube.CreateKeyPairSecret(ctx, r.Client, rctx.Account.Status.S3AdminKeysSecretRef.Namespace, rctx.Account.Status.S3AdminKeysSecretRef.Name, accessKey, secretKey, owner, ownerKind)
	if err != nil {
		log.Error(err, "Failed to create secret")
		return err
	}

	// Add protective finalizer to S3 admin keys secret to prevent premature deletion during namespace teardown.
	if err := kube.AddSecretFinalizer(ctx, r.Client, rctx.Account.Status.S3AdminKeysSecretRef.Namespace, rctx.Account.Status.S3AdminKeysSecretRef.Name); err != nil {
		log.Error(err, "Failed to add finalizer to S3 admin keys secret")
		return err
	}

	rctx.Account.Status.S3AdminAccessKeyId = accessKeyId

	return nil
}

// finalize handles any cleanup logic when the S3Tenant is being deleted.
func (r *S3TenantAccountReconciler) finalize(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)
	log.Info(fmt.Sprintf("Finalizing S3TenantAccount %s", rctx.Account.Name))

	// Remove protective finalizers from secrets to allow Kubernetes GC via OwnerReferences.
	if err := r.removeSecretFinalizers(ctx, rctx); err != nil {
		log.Error(err, "Failed to remove secret finalizers")
		// Continue with finalization even if finalizer removal fails
	}

	// Check if tenant still exists on the backend
	stillExists := true
	if err := r.fetchTenant(ctx, rctx); err != nil {
		log.Error(err, "Failed to fetch tenant")
		stillExists = false
	}

	// If tenant doesn't exist on backend, finalization complete
	if !stillExists {
		log.V(1).Info(fmt.Sprintf("Tenant %s does not exist on the backend, finalization done", rctx.Account.Name))
		return nil
	}

	// Check deletion policy - determine whether to delete or retain
	if rctx.Account.Status.TenantDeletionPolicy != nil && rctx.Account.Status.TenantDeletionPolicy.Policy == s3v1alpha1.TenantDeletionPolicyRetain {
		// Retain policy: Remove ownership metadata, making tenant importable
		log.Info(fmt.Sprintf("Retain policy detected for tenant %s - removing ownership metadata", rctx.Account.Status.TenantID))
		r.emitEvent(rctx, corev1.EventTypeNormal, "TenantRetaining",
			fmt.Sprintf("Retaining tenant %s in StorageGrid - removing operator ownership metadata", rctx.Account.Status.TenantID))

		// add timestamp to description to indicate when it was retained.
		description := fmt.Sprintf("Tenant %s removed from Kubernetes on %s", rctx.Account.Name, time.Now().Format(time.RFC3339))

		if err := grid.UpdateDescription(ctx, description, rctx.BackendTenant, rctx.GridClient); err != nil {
			log.Error(err, "Failed to remove ownership metadata")
			r.emitEvent(rctx, corev1.EventTypeWarning, "TenantRetainFailed",
				fmt.Sprintf("Failed to remove ownership metadata: %v", err))
			return fmt.Errorf("failed to remove ownership metadata: %w", err)
		}

		r.emitEvent(rctx, corev1.EventTypeNormal, "TenantRetained",
			fmt.Sprintf("Tenant %s retained in StorageGrid and is now available for re-import", rctx.Account.Status.TenantID))
		log.Info(fmt.Sprintf("Successfully retained tenant %s - ownership metadata removed", rctx.Account.Status.TenantID))
		return nil
	}

	// Delete policy (default): Full deletion from StorageGrid
	log.V(1).Info(fmt.Sprintf("Tenant %s still exists on the backend, starting deletion process", rctx.Account.Name))
	r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantDeleting,
		fmt.Sprintf("Deleting tenant %s from StorageGrid backend", rctx.Account.Status.TenantID))

	// Send delete request to the grid
	if err := grid.DeleteTenant(ctx, rctx.Account.Status.TenantID, rctx.GridClient); err != nil {
		log.Error(err, "Failed to delete tenant on the backend")
		r.emitEvent(rctx, corev1.EventTypeWarning, EventTenantDeleteFailed,
			fmt.Sprintf("Failed to delete tenant: %v", err))
		return fmt.Errorf("failed to delete tenant on the backend: %w", err)
	}
	r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantDeleted,
		"Tenant deletion initiated, waiting for backend confirmation")

	log.V(1).Info(fmt.Sprintf("Successfully sent delete request of S3TenantAccount %s to the backend, requeuing to check progress", rctx.Account.Name))
	rctx.RequeAfter = metav1.Duration{Duration: time.Minute * 1}

	return fmt.Errorf("S3TenantAccount %s deletion in progress, requeuing to check again", rctx.Account.Name)
}

// tenantClassEndpointChangePredicate creates a predicate that only triggers when
// S3EndpointConfig in the TenantClass status actually changes.
// This ensures we only reconcile accounts when endpoint configuration changes,
// ignoring other irrelevant status updates.
func (r *S3TenantAccountReconciler) tenantClassEndpointChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			// Trigger on create so new TenantClasses with endpoint config get picked up
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldClass, okOld := e.ObjectOld.(*s3v1alpha1.S3TenantClass)
			newClass, okNew := e.ObjectNew.(*s3v1alpha1.S3TenantClass)

			if !okOld || !okNew {
				return false
			}

			// Only trigger if S3EndpointConfig in status actually changed
			return !reflect.DeepEqual(oldClass.Status.S3EndpointConfig, newClass.Status.S3EndpointConfig)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// Don't trigger on delete - accounts will handle their own cleanup
			return false
		},
	}
}

// mapTenantClassToAccounts maps S3TenantClass changes to S3TenantAccounts that reference it.
// This ensures endpoint configuration changes propagate immediately to dependent accounts.
// Uses the field indexer to efficiently find only accounts referencing the changed TenantClass.
func (r *S3TenantAccountReconciler) mapTenantClassToAccounts(ctx context.Context, obj client.Object) []ctrl.Request {
	log := log.FromContext(ctx)
	tenantClass := obj.(*s3v1alpha1.S3TenantClass)

	// Find all accounts using this TenantClass via field indexer
	// This only returns accounts that reference this specific class
	accounts := &s3v1alpha1.S3TenantAccountList{}
	if err := r.List(ctx, accounts, client.MatchingFields{
		"status.s3EndpointConfig.s3TenantClassName": tenantClass.Name,
	}); err != nil {
		log.Error(err, "Failed to list accounts for TenantClass", "tenantClass", tenantClass.Name)
		return []ctrl.Request{}
	}

	// Create reconciliation requests for each affected account
	requests := make([]ctrl.Request, len(accounts.Items))
	for i := range accounts.Items {
		requests[i] = ctrl.Request{
			NamespacedName: types.NamespacedName{Name: accounts.Items[i].Name},
		}
	}

	if len(requests) > 0 {
		log.V(1).Info(fmt.Sprintf("Triggering reconciliation for %d account(s) due to TenantClass endpoint config change", len(requests)), "tenantClass", tenantClass.Name)
	}

	return requests
}

// removeSecretFinalizers removes protective finalizers from operator-managed secrets,
// allowing Kubernetes garbage collection to delete them via OwnerReferences.
func (r *S3TenantAccountReconciler) removeSecretFinalizers(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// Remove finalizer from root secret if present.
	if rctx.Account.Status.RootSecretRef != nil && rctx.Account.Status.RootSecretRef.Name != "" {
		if err := kube.RemoveSecretFinalizer(ctx, r.Client, rctx.Account.Status.RootSecretRef.Namespace, rctx.Account.Status.RootSecretRef.Name); err != nil {
			log.Error(err, "Failed to remove finalizer from root secret")
			return err
		}
	}

	// Remove finalizer from S3 admin keys secret if present.
	if rctx.Account.Status.S3AdminKeysSecretRef != nil && rctx.Account.Status.S3AdminKeysSecretRef.Name != "" {
		if err := kube.RemoveSecretFinalizer(ctx, r.Client, rctx.Account.Status.S3AdminKeysSecretRef.Namespace, rctx.Account.Status.S3AdminKeysSecretRef.Name); err != nil {
			log.Error(err, "Failed to remove finalizer from S3 admin keys secret")
			return err
		}
	}

	log.V(1).Info("Successfully removed finalizers from operator-managed secrets")
	return nil
}

// reconcileImport handles the import of an existing tenant from the backend.
// It fetches the tenant by ID and takes full ownership. Fails with hard error
// if the tenant is already managed by another CR.
// As NetApp has no metadata field to track ownership, we rely on the description
// This function is only called during initial creation (not updates).
func (r *S3TenantAccountReconciler) reconcileImport(ctx context.Context, rctx *accountReconcileContext, tenantID string) error {
	log := log.FromContext(ctx)

	log.Info("Starting tenant import", "tenantID", tenantID)
	r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantCreating,
		fmt.Sprintf("Importing existing tenant with ID %s", tenantID))

	// Validate root secret was set (required for import, webhook should catch this)
	if rctx.Account.Spec.RootSecretRef == nil || rctx.Account.Spec.RootSecretRef.Name == "" {
		err := fmt.Errorf("import requires spec.rootSecretRef to be specified")
		r.emitEvent(rctx, corev1.EventTypeWarning, EventTenantImportFailed,
			fmt.Sprintf("Import validation failed: %v", err))
		return err
	}

	// Verify root secret works by initializing a tenant client
	err := r.initTenantClient(ctx, rctx)
	if err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventTenantImportFailed, fmt.Sprintf("Unable to initialize client: %v", err))
		return fmt.Errorf("Unable to initialize tenant client: %w", err)
	}

	// Fetch tenant by ID from backend
	tenant, err := grid.FetchTenant(ctx, tenantID, rctx.GridClient)
	if err != nil {
		r.emitEvent(rctx, corev1.EventTypeWarning, EventTenantImportFailed,
			fmt.Sprintf("Failed to fetch tenant %s: %v", tenantID, err))
		return fmt.Errorf("failed to fetch tenant %s for import: %w", tenantID, err)
	}

	// Cache tenant for this reconciliation in context
	rctx.BackendTenant = tenant

	// Sync basic status from backend
	rctx.Account.Status.TenantID = tenant.Id
	rctx.Account.Status.ObservedTenantBackendName = tenant.Name
	rctx.Account.Status.TenantManagerURL = fmt.Sprintf("%s?accountId=%s", rctx.SG.Spec.ManagementEndpoint, tenant.Id)

	// Remove import annotation
	delete(rctx.Account.Annotations, s3v1alpha1.AnnotationImportTenant)
	rctx.ObjectUpdated = true

	// update condition and emit event
	r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "TenantImported", fmt.Sprintf("Imported Tenant with id %s from backend", rctx.Account.Status.TenantID))
	r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantImported,
		fmt.Sprintf("Successfully imported tenant with ID %s", tenantID))

	// trigger requeue
	rctx.DoRequeue = true

	return nil
}

// as descriptions can be edited manually outside of the operator we need to verify
// that this tenant is still owned by us.
// this should be called early on in every reconciliation to ensure we do not accidentally take over tenants.
func (r *S3TenantAccountReconciler) reconcileOwnership(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	log.Info("Verifying tenant ownership", "tenantID", rctx.Account.Status.TenantID)

	// Check ownership
	// we try to be really careful here to avoid taking over tenants managed by other CRs.
	ownsResource, currentOwnerName, currentOwnerUID, metadata := r.checkOwnership(ctx, rctx)

	// Hard error if owned by different CR.
	if currentOwnerUID != "" && !ownsResource {
		namespace := metadata["kubernetes_namespace"]

		errMsg := fmt.Sprintf("Cannot import tenant %s: already managed by another CR '%s' (UID: %s)",
			rctx.Account.Status.TenantID, currentOwnerName, currentOwnerUID)

		resolutionInstructions := fmt.Sprintf("\n\nConflict Resolution Options:\n"+
			"1. Delete the other CR '%s' in namespace '%s' if it's stale\n"+
			"2. Delete this CR and use the existing one instead\n"+
			"3. If the tenant was orphaned, manually edit the tenant description in StorageGrid to remove the 'cr_uid' field or the whole desceription",
			currentOwnerName, namespace)

		fullError := errMsg + resolutionInstructions

		log.Error(fmt.Errorf("%s", errMsg), "Import blocked by ownership conflict",
			"tenantID", rctx.Account.Status.TenantID,
			"ownerCR", currentOwnerName,
			"ownerUID", currentOwnerUID)

		r.emitEvent(rctx, corev1.EventTypeWarning, EventOwnershipConflict, errMsg)
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeCreated, metav1.ConditionFalse,
			"OwnershipConflict", fullError)

		return fmt.Errorf("%s", fullError)
	}

	// Idempotent ownership: adopt if unmanaged or already ours
	if currentOwnerUID == "" {
		log.Info("Owning unmanaged tenant", "tenantID", rctx.Account.Status.TenantID, "tenantName", rctx.BackendTenant.Name)
	} else {
		log.Info("Tenant already owned by this CR, nothing todo", "tenantID", rctx.Account.Status.TenantID)
	}

	return nil
}

// checkOwnership verifies ownership based on metadata in the backend tenant description.
// Returns: (ownsResource bool, currentOwnerName string, currentOwnerUID string, metadata map[string]string).
func (r *S3TenantAccountReconciler) checkOwnership(ctx context.Context, rctx *accountReconcileContext) (bool, string, string, map[string]string) {
	log := log.FromContext(ctx)

	metadata, parseErr := parseMetadataFromDescription(*rctx.BackendTenant.Description)
	if parseErr != nil {
		log.Error(parseErr, "Failed to parse metadata from tenant description")
		r.emitEvent(rctx, corev1.EventTypeWarning, "MetadataParseWarning",
			fmt.Sprintf("Failed to parse tenant metadata: %v", parseErr))
		// Return empty ownership info on parse error
		return false, "", "", metadata
	}

	currentOwnerUID := metadata["cr_uid"]
	currentOwnerName := metadata["cr_name"]

	// Empty cr_uid means unmanaged tenant
	if currentOwnerUID == "" {
		return false, "", "", metadata
	}

	// Check if this CR owns the tenant
	ownsResource := currentOwnerUID == string(rctx.Account.UID)
	return ownsResource, currentOwnerName, currentOwnerUID, metadata
}

// parseMetadataFromDescription extracts key-value metadata from tenant description.
// Description format: "key:value \nkey:value \n..."
// Returns error if description appears corrupted.
func parseMetadataFromDescription(description string) (map[string]string, error) {
	metadata := make(map[string]string)

	if description == "" {
		return metadata, nil
	}

	lines := strings.Split(description, "\n")
	malformedLines := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			metadata[key] = value
		} else {
			malformedLines++
		}
	}

	// If more than half the lines are malformed, description is likely corrupted
	if malformedLines > len(lines)/2 && len(lines) > 0 {
		return metadata, fmt.Errorf("tenant description appears corrupted (%d/%d lines malformed)", malformedLines, len(lines))
	}

	return metadata, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *S3TenantAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	tenantRefFunc := func(obj client.Object) []string {
		sg := obj.(*s3v1alpha1.S3Bucket)
		return []string{sg.Spec.S3TenantRef.Name}
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &s3v1alpha1.S3Bucket{}, "spec.accountRef.name", tenantRefFunc); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&s3v1alpha1.S3TenantAccount{}, builder.WithPredicates(
			PredicateWithoutStatusChange(),
		)).
		Owns(&corev1.Secret{}).
		Watches(
			&s3v1alpha1.S3TenantClass{},
			handler.EnqueueRequestsFromMapFunc(r.mapTenantClassToAccounts),
			builder.WithPredicates(r.tenantClassEndpointChangePredicate()),
		).
		Complete(r)
}

// emitEvent emits a Kubernetes event immediately.
func (r *S3TenantAccountReconciler) emitEvent(
	rctx *accountReconcileContext,
	eventType, reason, message string) {
	r.Recorder.Event(rctx.Account, eventType, reason, message)
}
