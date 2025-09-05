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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	s3v1alpha1 "git.mgmtbi.ch/cloud/storagegrid-operator/api/v1alpha1"
	"git.mgmtbi.ch/cloud/storagegrid-operator/pkg/grid"
	"git.mgmtbi.ch/cloud/storagegrid-operator/pkg/kube"
)

// S3TenantAccountReconciler reconciles a S3TenantAccount object.
type S3TenantAccountReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
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

// Reconcile is part of the main kubernetes reconciliation loop which aims to.
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by.
// the S3TenantAccount object against the actual cluster state, and then.
// perform operations to make the cluster state reflect the state specified by.
// the user.
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
	if updateErr := r.Status().Update(ctx, rctx.Account); updateErr != nil {
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

	// Ensure owner reference is set.
	if err := ctrl.SetControllerReference(rctx.SG, rctx.Account, r.Scheme); err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "OwnerReferenceSetFailed", fmt.Sprintf("Failed to set owner reference for S3TenantAccount to backing StorageGrid: %s", err.Error()))
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
		return err
	}
	r.setCondition(rctx.Account, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionTrue, "StorageGridReadyAndAccessible", fmt.Sprintf("StorageGrid %s is ready", rctx.SG.Name))

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
	// update the tenantlass for network access.
	err = r.reconcileS3TenantClass(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantClassReconcileFailed", fmt.Sprintf("Failed to reconcile S3TenantClass: %s", err.Error()))
		return err
	}

	// check for the resourcecreated condition.
	// the condition will only be set if the tenant was created successfully.
	// or if the annotation to recreate the tenant was set.
	err = r.reconcileRecreateAnnotation(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "RecreateAnnotationReconcileFailed", fmt.Sprintf("Failed on reconciling recreate annotation: %s", err.Error()))
		return err
	}

	if meta.FindStatusCondition(rctx.Account.Status.Conditions, s3v1alpha1.ConditionTypeCreated) == nil {
		err := r.reconcileCreate(ctx, rctx)
		if err != nil {
			r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "CreateFailed", fmt.Sprintf("Failed to create tenant: %s", err.Error()))
			return err
		}

		// update status and condition already.

		rctx.DoRequeue = true // we need to requeue to ensure the status is updated
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
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantClientInitFailed", fmt.Sprintf("Failed to fetch tenant from backend due to %s", err.Error()))
		return err
	} else {
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "TenantClientInitSucceeded", "Initialized tenant client successfully")
	}

	err = r.reconcileTenantDescription(ctx, rctx)
	if err != nil {
		// no need to stop reconciliation here, just set the condition.
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantDescriptionReconcileFailed", fmt.Sprintf("Failed to reconcile tenant description: %s", err.Error()))
	}

	err = r.reconcileTenantNameUpdate(ctx, rctx)
	if err != nil {
		// no need to stop reconciliation here, just set the condition.
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantNameUpdateFailed", fmt.Sprintf("Failed to update tenant name: %s", err.Error()))
	}

	// make sure the regions are up to date.
	r.reconcileRegions(ctx, rctx)

	// check usage and quota configuration.
	err = r.reconcileStorageQuota(ctx, rctx)
	if err != nil {
		// no need to stop reconciliation here, just set the condition.
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "StorageQuotaReconcileFailed", fmt.Sprintf("Failed to reconcile storage quota: %s", err.Error()))
	}

	err = r.reconcileTenantUsage(ctx, rctx)
	if err != nil {
		// no need to stop reconciliation here, just set the condition.
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantUsageReconcileFailed", fmt.Sprintf("Failed to reconcile tenant usage: %s", err.Error()))
	}

	r.evaluateQuotaConditions(ctx, rctx.Account)

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
		log.V(1).Info("No S3Tenant reference found in account, was it deleted?")

		// if the spec is missing it can be either:.
		// 1. the S3Tenant was never created and the account is used in isolation
		// 2. the referenced S3Tenant was deleted
		if rctx.Account.Status.S3TenantRef == nil {
			// -> 1. the status is also nil, we can assume that the S3Tenant was never created or already deleted
			r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeBound, metav1.ConditionFalse, "S3TenantNotBound", "No S3 Tenant is binding this account")
			log.V(1).Info("No pending S3Tenant reference found in account status, nothing to do")
			return nil
		}

		// -> 2. the status is not nil, we can assume that the S3Tenant was deleted
		log.V(1).Info(fmt.Sprintf("Found reference to S3Tenant %s in account status, trying to fetch it", rctx.Account.Status.S3TenantRef.Name))
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
		}

		return nil
	}

	r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeBound, metav1.ConditionTrue, "S3TenantBound", fmt.Sprintf("Bound by S3 Tenant %s", rctx.Account.Spec.S3TenantRef.Name))

	// if the S3TenantRef is set, we need to fetch the S3Tenant.
	if err := r.Get(ctx, types.NamespacedName{Name: rctx.Account.Spec.S3TenantRef.Name, Namespace: rctx.Account.Spec.S3TenantRef.Namespace}, rctx.S3Tenant); err != nil {
		log.Error(err, fmt.Sprintf("Failed to retrieve S3Tenant %s from API", rctx.Account.Spec.S3TenantRef.Name))

		// this means that the referenced s3tenant was probably deleted.
		if client.IgnoreNotFound(err) == nil {
			log.V(1).Info("Referenced S3Tenant not found, was it deleted?")
			r.reconcileDeletedTenant(ctx, rctx)
			return nil
		} else {
			// any other error is a real error, we cannot proceed.
			return err
		}
	}

	// once fetched, we need to always update the S3TenantRef in the account status.
	if rctx.Account.Status.S3TenantRef == nil || rctx.Account.Status.S3TenantRef.Name != rctx.S3Tenant.Name {
		log.V(1).Info(fmt.Sprintf("Updating S3TenantRef in account status to %s", rctx.S3Tenant.Name))
		rctx.Account.Status.S3TenantRef = &corev1.ObjectReference{
			Name:       rctx.S3Tenant.Name,
			Namespace:  rctx.S3Tenant.Namespace,
			UID:        rctx.S3Tenant.UID,
			Kind:       rctx.S3Tenant.Kind,
			APIVersion: rctx.S3Tenant.APIVersion,
		}

		// make sure no deletion timestamp is configured from any old bindings.
		if rctx.Account.Status.DeletionTimestamp != nil {
			log.V(1).Info("Clearing deletion timestamp from account, S3Tenant is bound")
			rctx.Account.Status.DeletionTimestamp = nil
		}

		log.V(1).Info(fmt.Sprintf("Successfully retrieved S3Tenant %s", rctx.S3Tenant.Name))
	}

	return nil
}

func (r *S3TenantAccountReconciler) reconcileDeletedTenant(ctx context.Context, rctx *accountReconcileContext) {
	log := log.FromContext(ctx)

	// if the S3Tenant was deleted, we need to check what policy was configured on the grid.
	switch rctx.Account.Status.TenantDeletionPolicy.Policy {
	case s3v1alpha1.TenantDeletionPolicyDelete:
		log.V(1).Info("Tenant deletion policy is set to delete, deleting the account")
		// delete policy is set, we can delete the account on the backend.
		// set deletion timestamp to now, so the account is deleted immediately.
		rctx.Account.Status.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	case s3v1alpha1.TenantDeletionPolicyRetain:
		// retain policy is set, we need to set the S3TenantRef to nil on do nothing else.
		log.V(1).Info("Tenant deletion policy is set to retain, setting S3TenantRef to nil and doing nothing")
		r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeRetained, metav1.ConditionTrue, "TenantRetained", "S3Tenant was deleted, but account is retained due to deletion policy")
		rctx.Account.Status.S3TenantRef = nil
		rctx.Account.Spec.S3TenantRef = nil // also set the spec to nil to avoid confusion
		rctx.ObjectUpdated = true
	case s3v1alpha1.TenantDeletionPolicyRetainThenDelete:
		// retain then delete policy is set, we need to set the S3TenantRef to nil and delete the account.
		log.V(1).Info("Tenant deletion policy is set to retain then delete, setting S3TenantRef to nil aswell as setting the deletion timestamp on the account")

		rctx.Account.Status.S3TenantRef = nil
		retentionDuration := rctx.Account.Status.TenantDeletionPolicy.RetentionDuration
		rctx.Account.Spec.S3TenantRef = nil // also set the spec to nil to avoid confusion
		rctx.ObjectUpdated = true

		// calculate the deletion timestamp based on the retention duration.
		if retentionDuration != nil {
			log.V(1).Info(fmt.Sprintf("Setting deletion timestamp to %s", retentionDuration.String()))
			rctx.Account.Status.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Add(retentionDuration.Duration)}
		} else {
			log.V(1).Info("No retention duration set, using default deletion timestamp")
			rctx.Account.Status.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
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
			// update the deletion timestamp to nil, so we do not delete it again.
			account.Status.DeletionTimestamp = nil
			account.Status.Phase = s3v1alpha1.PhaseDeleting
			return r.deleteAccount(ctx, account)
		}
		log.V(1).Info("S3Tenant is not bound anymore, deleting the account")
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

	// The retention conditions override the phase of the account.
	if retain {
		log.V(1).Info("S3Tenant is retained, setting phase to Retained")
		rctx.Account.Status.Phase = s3v1alpha1.PhaseRetaining
	} else if retainThenDelete {
		log.V(1).Info("S3Tenant is retained then deleted, setting phase to RetainingThenDeleting")
		rctx.Account.Status.Phase = s3v1alpha1.PhaseRetainThenDelete
		// on retain then delete we have to make sure it is requeued when the deletion timestamp is reached for deletion.
		rctx.RequeAfter = metav1.Duration{Duration: rctx.Account.Status.DeletionTimestamp.Time.Sub(metav1.Now().Time)}
	} else if deletion {
		log.V(1).Info("S3Tenant deletion timestamp reached, setting phase to Deleting")
		rctx.Account.Status.Phase = s3v1alpha1.PhaseDeleting
	} else {
		log.V(1).Info("No retention conditions met, phase remains unchanged")
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
	if rctx.SG.Spec.TenantPrefix == "Namespace" {
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
	if rctx.S3Tenant.Name != "" {
		tenantName = rctx.S3Tenant.Name
	} else if rctx.Account.Spec.Name != "" {
		tenantName = rctx.Account.Spec.Name
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
		err := grid.UpdateName(ctx, *rctx.Account.Status.DesiredTenantBackendName, rctx.BackendTenant, rctx.GridClient)
		if err != nil {
			log.Error(err, "Failed to update tenant name")
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
	description := map[string]string{
		"kubernetes_namespace": namespace,
		"user_description":     *rctx.Account.Spec.Description,
		"owner":                *rctx.Account.Spec.Owner,
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
		err := grid.UpdateDescription(ctx, descriptionStr, rctx.BackendTenant, rctx.GridClient)
		if err != nil {
			log.Error(err, "Failed to update tenant description")
			return err
		}
		log.V(1).Info("Tenant description updated successfully")
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
		rctx.Account.Status.GridEndpoint = rctx.SG.Spec.Endpoint
	} else if rctx.Account.Status.GridEndpoint != rctx.SG.Spec.Endpoint {
		log.V(1).Info("Grid endpoint has changed, updating from %s to %s", rctx.Account.Status.GridEndpoint, rctx.SG.Spec.Endpoint)
		rctx.Account.Status.GridEndpoint = rctx.SG.Spec.Endpoint
	}
}

func (r *S3TenantAccountReconciler) reconcileGridReadiness(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// TODO: this should change to using status.Conditions, status.Ready is the human readable version of the conditions.
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
	client, err := grid.InitGridClient(username, password, rctx.SG.Spec.Endpoint)
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

	if _, ok := rctx.Account.Annotations[AnnotationRecreateTenant]; ok {
		log.V(1).Info("Recreate tenant annotation found, recreating tenant")

		// remove tenant created condition to allow re-creation.
		meta.RemoveStatusCondition(&rctx.Account.Status.Conditions, s3v1alpha1.ConditionTypeCreated)

		// remove the annotation.
		delete(rctx.Account.Annotations, AnnotationRecreateTenant)
		log.V(1).Info("Successfully recreated credentials and removed annotation")

		rctx.ObjectUpdated = true
	}

	return nil
}

func (r *S3TenantAccountReconciler) reconcileCreate(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	log.V(1).Info("Tenant does not exist yet, creating it")
	tenantID, password, err := grid.CreateTenant(ctx, *rctx.Account.Status.DesiredTenantBackendName, *rctx.Account.Spec.Description, rctx.Account.Spec.StorageQuota.Value(), rctx.GridClient)
	if err != nil {
		log.Error(err, "Failed to create tenant")
		return err
	}
	r.setCondition(rctx.Account, s3v1alpha1.ConditionTypeCreated, metav1.ConditionTrue, "TenantCreated", fmt.Sprintf("Created Tenant with id %s in backend", rctx.Account.Status.TenantID))

	// store credentials in a secret.
	err = kube.CreateCredentialSecret(ctx, r.Client, rctx.Account.Status.RootSecretRef.Namespace, rctx.Account.Status.RootSecretRef.Name, "root", password, rctx.Account)
	if err != nil {
		log.Error(err, "Failed to create secret")
		return err
	}

	log.Info(fmt.Sprintf("Tenant created successfully, ID: %s", tenantID))

	// update status as needed.
	rctx.Account.Status.ObservedTenantBackendName = rctx.Account.Status.DesiredTenantBackendName
	rctx.Account.Status.TenantID = tenantID
	rctx.Account.Status.TenantManagerURL = fmt.Sprintf("%s?accountId=%s", rctx.SG.Spec.Endpoint, tenantID)

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

	// Update secret references in status.
	r.updateSecretRef(ctx, &rctx.Account.Status.RootSecretRef,
		rctx.Account.Spec.RootSecretRef,
		platformSecretNamespace,
		fmt.Sprintf("%s-root-credentials", rctx.Account.Name))

	r.updateSecretRef(ctx, &rctx.Account.Status.AdminSecretRef,
		rctx.Account.Spec.AdminSecretRef,
		userSecretNamespace,
		fmt.Sprintf("%s-admin-credentials", secretBaseName))

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

func (r *S3TenantAccountReconciler) updateSecretRef(
	ctx context.Context,
	statusRef **corev1.ObjectReference,
	specRef *corev1.ObjectReference,
	namespace string,
	defaultName string) {
	log := log.FromContext(ctx).WithValues("function", "updateSecretRef")

	var name string
	if specRef != nil && specRef.Name != "" { // Need to check if specRef is not nil first
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
	} else if (*statusRef).Name != newRef.Name || (*statusRef).Namespace != newRef.Namespace {
		log.Info("Updating secret reference",
			"oldName", (*statusRef).Name,
			"oldNamespace", (*statusRef).Namespace,
			"newName", newRef.Name,
			"newNamespace", newRef.Namespace)
		*statusRef = newRef
	} else {
		log.V(1).Info("Secret reference unchanged",
			"name", newRef.Name,
			"namespace", newRef.Namespace)
	}
}

func (r *S3TenantAccountReconciler) reconcileStorageQuota(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// check if there was an update to the quota.
	if rctx.Account.Spec.StorageQuota.Value() != grid.GetConfiguredQuota(rctx.BackendTenant) {
		log.V(1).Info("Quota was updated, updating tenant")

		// update tenant.
		err := grid.UpdateQuota(ctx, rctx.Account.Spec.StorageQuota.Value(), rctx.BackendTenant, rctx.GridClient)

		if err != nil {
			log.Error(err, "Failed to update tenant quota")
			// revert to original value.
			return err
		}
	}

	rctx.Account.Status.Quota.Limit = kube.ParseBytes(grid.GetConfiguredQuota(rctx.BackendTenant))
	log.V(1).Info(fmt.Sprintf("Updated tenant quota to %s", rctx.Account.Spec.StorageQuota.String()))
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

func (r *S3TenantAccountReconciler) evaluateQuotaConditions(ctx context.Context, account *s3v1alpha1.S3TenantAccount) {
	log := log.FromContext(ctx)
	// check if quota was exceeded and mark tenant as ready or not.
	if account.Status.Quota.Used.Value() > account.Status.Quota.Limit.Value() {
		log.Info(fmt.Sprintf("Quota exceeded: %s/%s", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String()))
		r.setCondition(account, s3v1alpha1.ConditionTypeQuotaSufficient, metav1.ConditionFalse, "QuotaExceeded", fmt.Sprintf("Quota exceeded: used %s is more than configured limit %s", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String()))
	} else {
		r.setCondition(account, s3v1alpha1.ConditionTypeQuotaSufficient, metav1.ConditionTrue, "QuotaSufficient", fmt.Sprintf("Quota sufficient: used %s is less than configured limit %s", account.Status.Quota.Used.String(), account.Status.Quota.Limit.String()))
	}
}

func (r *S3TenantAccountReconciler) reconcileS3TenantClass(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)

	// check if the S3TenantClass is set, if not set it to the default value.
	if rctx.Account.Status.S3ApiEndpoint.S3TenantClassName == "" {
		// if unset we can just update the value.
		if rctx.Account.Spec.S3TenantClassName == "" {
			// if the S3TenantClassName is not set in the spec, we can use the default class.
			log.V(1).Info("S3TenantClassName is not set, using default class")
			rctx.Account.Status.S3ApiEndpoint.S3TenantClassName = "default"
		} else {
			// if the S3TenantClassName is set in the spec, we can use that value.
			log.V(1).Info(fmt.Sprintf("S3TenantClassName is set to %s", rctx.Account.Status.S3ApiEndpoint.S3TenantClassName))
			rctx.Account.Status.S3ApiEndpoint.S3TenantClassName = rctx.Account.Spec.S3TenantClassName
		}
	}

	// if the was changed we need to be a bit more careful.
	if rctx.Account.Status.S3ApiEndpoint.S3TenantClassName != rctx.Account.Spec.S3TenantClassName {
		// first verify that the annotation is set to allow changing the tenant class name.
		if rctx.Account.Annotations == nil {
			return fmt.Errorf("S3TenantClassName is set to %s, but annotation %s to allow change is not set, not updating", rctx.Account.Spec.S3TenantClassName, AnnotationAllowTenantClassNameChange)
		}
		val, ok := rctx.Account.Annotations[AnnotationAllowTenantClassNameChange]
		if !ok {
			return fmt.Errorf("S3TenantClassName is set to %s, but annotation %s to allow change is not set, not updating", rctx.Account.Spec.S3TenantClassName, AnnotationAllowTenantClassNameChange)
		}
		if val != "true" {
			return fmt.Errorf("S3TenantClassName is set to %s, but annotation %s to allow change is %s instead of true, not updating", rctx.Account.Spec.S3TenantClassName, AnnotationAllowTenantClassNameChange, val)
		}

		// if the annotation is set accordingly we can update the tenant class name.
		log.V(1).Info(fmt.Sprintf("S3TenantClassName was updated from %s to %s", rctx.Account.Status.S3ApiEndpoint.S3TenantClassName, rctx.Account.Spec.S3TenantClassName))
		rctx.Account.Status.S3ApiEndpoint.S3TenantClassName = rctx.Account.Spec.S3TenantClassName

		// reprotect the S3TenantClassName by setting the annotation.
		log.V(1).Info("Removing annotation to protect S3TenantClassName from further changes")
		delete(rctx.Account.Annotations, AnnotationAllowTenantClassNameChange)
		rctx.ObjectUpdated = true
	}

	// get s3 api endpoint from the tenantclass.
	accountClass := &s3v1alpha1.S3TenantClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: rctx.Account.Status.S3ApiEndpoint.S3TenantClassName}, accountClass); err != nil {
		log.Error(err, "Failed to retrieve S3TenantClass")
		return err
	}

	rctx.Account.Status.S3ApiEndpoint = &s3v1alpha1.S3ApiEndpoint{
		S3TenantClassName: accountClass.Name,
		S3Urls:            accountClass.Status.S3Endpoints,
		S3VIPs:            accountClass.Status.S3VIPs,
		Port:              accountClass.Status.Port,
		PathStyleAccess:   &accountClass.Spec.UsePathStyleAccess,
	}

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
	if rctx.Account.Annotations[AnnotationResetTenantAdminPassword] == "true" {
		log.Info("Recreating tenant admin credentials due to annotation")

		// trigger recreation of the S3 admin keypair.
		err := r.createTenantAdminCredentials(ctx, rctx, true)
		if err != nil {
			log.Error(err, "Failed to reset tenant admin keypair")
			return err
		}

		delete(rctx.Account.Annotations, AnnotationResetTenantAdminPassword)
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
		// recreate the admin password.
		username, password, err = grid.SetTenantAdminPassword(ctx, rctx.TenantClient)
		if err != nil {
			log.Error(err, "Failed to recreate S3 admin keypair")
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

	// if tenantref is set use this as owner instead.
	var owner metav1.Object
	if rctx.Account.Spec.S3TenantRef != nil {
		owner = rctx.S3Tenant
	} else {
		owner = rctx.Account
	}
	err = kube.CreateCredentialSecret(ctx, r.Client, rctx.Account.Status.AdminSecretRef.Namespace, rctx.Account.Status.AdminSecretRef.Name, username, password, owner)
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
		if rctx.Account.Annotations[AnnotationRecreateTenantKeypairs] == "true" {
			log.Info("Recreating S3 admin keypair due to annotation")

			// trigger recreation of the S3 admin keypair.
			err := r.createS3AdminKeypair(ctx, rctx, true)
			if err != nil {
				log.Error(err, "Failed to recreate S3 admin keypair")
				return err
			}

			delete(rctx.Account.Annotations, AnnotationRecreateTenantKeypairs)
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
		// recreate the S3 admin keypair with existing access key ID.
		accessKeyId, accessKey, secretKey, err = grid.RecreateAdminS3Credentials(ctx, rctx.Account.Status.S3AdminAccessKeyId, rctx.TenantClient)
		if err != nil {
			log.Error(err, "Failed to recreate S3 admin keypair")
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
	if rctx.Account.Spec.S3TenantRef != nil {
		owner = rctx.S3Tenant
	} else {
		owner = rctx.Account
	}
	// store s3 keys in a secret.
	err = kube.CreateKeyPairSecret(ctx, r.Client, rctx.Account.Status.S3AdminKeysSecretRef.Namespace, rctx.Account.Status.S3AdminKeysSecretRef.Name, accessKey, secretKey, owner)
	if err != nil {
		log.Error(err, "Failed to create secret")
		return err
	}

	rctx.Account.Status.S3AdminAccessKeyId = accessKeyId

	return nil
}

// finalize handles any cleanup logic when the S3Tenant is being deleted.
func (r *S3TenantAccountReconciler) finalize(ctx context.Context, rctx *accountReconcileContext) error {
	log := log.FromContext(ctx)
	log.Info(fmt.Sprintf("Finalizing S3Tenant %s", rctx.Account.Name))

	// delete requests within the backend take some time.
	// we always need to check if the tenant still exists on the backend.
	// while this is already checked using the webhook we still want to be sure.
	stillExists := true
	if err := r.fetchTenant(ctx, rctx); err != nil {
		log.Error(err, "Failed to fetch tenant")
		stillExists = false
	}

	// lucky case the tenant does not exist on the backend anymore.
	if !stillExists {
		log.V(1).Info(fmt.Sprintf("Tenant %s does not exist on the backend, finalization can proceed without further action", rctx.Account.Name))
		return nil
	}

	// if the tenant still exists we need to start the deletion process on the backend and reque.
	log.V(1).Info(fmt.Sprintf("Tenant %s still exists on the backend, starting deletion process", rctx.Account.Name))

	// send delete request to the grid.
	if err := grid.DeleteTenant(ctx, rctx.Account.Status.TenantID, rctx.GridClient); err != nil {
		log.Error(err, "Failed to delete tenant on the backend")
		return fmt.Errorf("failed to delete tenant on the backend: %w", err)
	}

	log.V(1).Info(fmt.Sprintf("Successfully sent delete request of S3TenantAccount %s to the backend, requeing to check again on the process", rctx.Account.Name))
	rctx.RequeAfter = metav1.Duration{Duration: time.Minute * 1} // requeue after 1 minute to check if the deletion was successful

	return fmt.Errorf("S3TenantAccount %s deletion in progress, requeuing to check again", rctx.Account.Name)
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
		Complete(r)
}
