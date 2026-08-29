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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
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
)

// S3TenantReconciler reconciles a S3Tenant object.
type S3TenantReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

type tenantReconcileContext struct {
	// context for the reconciliation loop.
	S3Tenant *s3v1alpha1.S3Tenant
	// storing the account reference here to make it available in the whole reconciliation loop.
	Account *s3v1alpha1.S3TenantAccount
	// tracking if annotations were update to send the update request after the status was updated.
	// this is used to avoid sending multiple updates in the same reconciliation loop.
	ObjectUpdated bool
	// track if we need to requeue the reconciliation loop.
	DoRequeue bool
	// RequeAfter schedules a bounded requeue for an expected wait (for example a
	// deletion blocked on linked buckets). Waiting is not a failure, so it must not be
	// signaled by returning an error: controller-runtime discards the Result whenever
	// the error is non-nil and falls back to exponential backoff that saturates at
	// 16m40s and never resets. Returning RequeueAfter with a nil error instead makes
	// the workqueue Forget the item and poll on this schedule.
	RequeAfter metav1.Duration
}

const (
	s3TenantFinalizer = "s3tenant.s3.bedag.ch/finalizer"

	// legacyForegroundFinalizer is the old finalizer that was incorrectly reused from the
	// Kubernetes garbage collector. Existing resources are migrated automatically.
	legacyForegroundFinalizer = "kubernetes.io/foregroundDeletion"
)

// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenants/finalizers,verbs=update
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenantaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to.
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:.
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile
func (r *S3TenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	log.V(1).Info(fmt.Sprintf("Reconciliation loop started for s3Tenant %s", req.Name))

	// Get the object to reconcile on.
	s3Tenant := &s3v1alpha1.S3Tenant{}
	if err := r.Get(ctx, req.NamespacedName, s3Tenant); err != nil {
		log.Error(err, "Failed to retrieve resource")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.V(1).Info("Successfully retrieved resource in question")

	// initialize the reconcile context.
	rctx := &tenantReconcileContext{
		S3Tenant:      s3Tenant,
		Account:       &s3v1alpha1.S3TenantAccount{},
		ObjectUpdated: false,
		DoRequeue:     false,
	}

	statusBase := s3Tenant.DeepCopy()
	err := r.doReconcile(ctx, rctx)

	// use conditions to derive the readiness state.
	r.deriveReadiness(ctx, rctx.S3Tenant)

	// update the annotations if they were updated.
	// needs to be done before the status is updated because we lose the annotations otherwise.
	if rctx.ObjectUpdated {
		// creating a deep copy of the status to avoid modifying the original object.
		statusCopy := rctx.S3Tenant.DeepCopy()

		log.V(1).Info("Annotations were updated, updating the object")
		if updateErr := r.Update(ctx, rctx.S3Tenant); updateErr != nil {
			log.Error(updateErr, "Failed to update annotations")
			if err != nil {
				err = fmt.Errorf("reconciliation failed: %w, failed to update annotations: %w", err, updateErr)
			} else {
				err = fmt.Errorf("failed to update annotations: %w", updateErr)
			}
		} else {
			log.V(1).Info("Annotations updated successfully")
		}

		rctx.S3Tenant.Status = statusCopy.Status // restore the status from the copy
	}

	// Due to condition tracking status always needs to be updated.
	if updateErr := r.Status().Patch(ctx, rctx.S3Tenant, client.MergeFrom(statusBase)); updateErr != nil {
		log.Error(updateErr, "Failed to update status, requeuing")
		if err == nil {
			// no error occurred during reconciliation, but status update failed.
			rctx.DoRequeue = true // requeue to ensure status is updated
		}
		// if an error already occurred during reconciliation, we just return that error.
	}

	if rctx.RequeAfter.Duration > 0 {
		log.V(1).Info("Requeuing reconciliation after duration", "duration", rctx.RequeAfter.Duration)
		return ctrl.Result{RequeueAfter: rctx.RequeAfter.Duration}, err
	}

	return ctrl.Result{Requeue: rctx.DoRequeue}, err
}

func (r *S3TenantReconciler) doReconcile(ctx context.Context, rctx *tenantReconcileContext) (err error) {
	// ensure the backing account exists or create it if necessary.
	err = r.reconcileTenantAccountReference(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantAccountError", fmt.Sprintf("Failed to reconcile tenant account: %s", err.Error()))
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "TenantAccountBindingError", fmt.Sprintf("S3Tenant Account %s not ready", rctx.Account.Name))
	}
	if rctx.DoRequeue {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "TenantAccountBindingPending", "S3Tenant Account is being created, requeuing")
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "TenantAccountBindingPending", "S3Tenant Account is being created, requeuing")
		return nil // requeue to ensure the account is created
	}

	err = ctrl.SetControllerReference(rctx.Account, rctx.S3Tenant, r.Scheme)
	if err != nil {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "OwnerReferenceError", fmt.Sprintf("Failed to set owner reference: %s", err.Error()))
	}

	// update the tenantAccountSpec if needed.
	err = r.reconcileTenantAccountSpec(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantAccountSpecUpdateFailed", fmt.Sprintf("Failed to reconcile tenant account spec: %s", err.Error()))
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "TenantAccountSpecUpdateFailed", fmt.Sprintf("Failed to update S3TenantAccount spec: %s", err.Error()))
	}
	if rctx.DoRequeue {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "TenantAccountSpecUpdatePending", "S3Tenant Account spec is being updated, requeuing")
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "TenantAccountSpecUpdatePending", "S3Tenant Account spec is being updated, requeuing")
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypePending, metav1.ConditionTrue, "TenantAccountSpecUpdatePending", "S3Tenant Account spec is being updated, requeuing")
		return nil // requeue to ensure the spec is updated
	}
	r.setCondition(rctx.S3Tenant, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionTrue, "TenantAccountReady", fmt.Sprintf("S3Tenant Account %s is ready for storage operations", rctx.Account.Name))

	// Check if account was just bound or configuration was applied
	if rctx.Account.Status.Phase == s3v1alpha1.PhaseBound {
		accountBoundCondition := meta.FindStatusCondition(rctx.S3Tenant.Status.Conditions, s3v1alpha1.ContitionTypeBackingResourceReady)
		if accountBoundCondition == nil || accountBoundCondition.Status != metav1.ConditionTrue {
			r.emitEvent(rctx, corev1.EventTypeNormal, EventAccountBound,
				fmt.Sprintf("Successfully bound to S3TenantAccount %s", rctx.Account.Name))
		}

		// Check if configuration is synced
		if equality.Semantic.DeepEqual(rctx.S3Tenant.Spec.CommonTenantSpec, rctx.Account.Spec.CommonTenantSpec) {
			r.emitEvent(rctx, corev1.EventTypeNormal, EventConfigurationApplied,
				"S3TenantAccount has successfully applied all configuration changes")
			r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeConfigurationSynced, metav1.ConditionTrue,
				"Synced", "Configuration is synchronized with S3TenantAccount")
			rctx.S3Tenant.Status.ObservedGeneration = rctx.S3Tenant.Generation
		} else {
			r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeConfigurationSynced, metav1.ConditionFalse,
				"Pending", "Configuration changes pending")
		}
	}

	// certain annotations need to be set on the tenantAccount.
	err = r.reconcileTenantAnnotations(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantAccountAnnotationUpdateFailed", fmt.Sprintf("Failed to reconcile tenant account annotations: %s", err.Error()))
	}

	// ensure the owner reference is set correctly.
	if err := ctrl.SetControllerReference(rctx.Account, rctx.S3Tenant, r.Scheme); err != nil {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "OwnerReferenceSetFailed", fmt.Sprintf("Failed to set owner reference for S3Tenant to backing S3TenantAccount: %s", err.Error()))
	}

	// copy the status from the account to the tenant.
	r.reconcileTenantAccountStatus(ctx, rctx)

	// examine DeletionTimestamp to determine if object is under deletion.
	// contrary to other resources we can only now safely handle deletion because we need to ensure that the status of the account is in sync
	// before we can safely delete the tenant.
	err = r.reconcileFinalizerAndDlelete(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "ReconcileFailed", fmt.Sprintf("Failed to reconcile delete or finalizer %s", err.Error()))
		return err
	}
	if rctx.DoRequeue {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "FinalizerReconcileSucceeded", "Finalizer and deletion timestamp reconciled successfully, requeuing")
		return nil
	}
	if rctx.RequeAfter.Duration > 0 {
		// Finalization is waiting on a precondition; conditions were already set by finalize.
		return nil
	}

	// get all the buckets linked to this tenant.
	err = r.reconcileLinkedBuckets(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "ReconcileFailed", fmt.Sprintf("Failed to reconcile linked buckets: %s", err.Error()))
	}

	// reconciliation ran successfully.
	r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "ReconcileSucceeded", "Reconciliation completed successfully")

	// Emit ready event if tenant is now fully ready and this is a state transition
	if rctx.Account.Status.Phase == s3v1alpha1.PhaseBound && rctx.S3Tenant.Status.Phase == "Bound" {
		r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantReady,
			fmt.Sprintf("S3Tenant is ready for storage operations with %d linked bucket(s)", len(rctx.S3Tenant.Status.LinkedBuckets)))
	} else if rctx.S3Tenant.Status.Phase != "Bound" {
		r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantNotReady,
			fmt.Sprintf("S3Tenant is not ready (phase: %s)", rctx.S3Tenant.Status.Phase))
	}

	return nil
}

func (r *S3TenantReconciler) setCondition(s3Tenant *s3v1alpha1.S3Tenant, condType string, status metav1.ConditionStatus, reason string, message string) {
	// add or update with given condition.
	condition := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: s3Tenant.GetGeneration(),
	}

	meta.SetStatusCondition(&s3Tenant.Status.Conditions, condition)
}

func (r *S3TenantReconciler) deriveReadiness(ctx context.Context, s3Tenant *s3v1alpha1.S3Tenant) {
	log := log.FromContext(ctx)

	// A terminating tenant is not Bound, whatever the readiness conditions say. Report the
	// lifecycle intent so `kubectl get` shows Deleting; the ConditionTypeDeleting reason
	// carries why it has not completed yet.
	if !s3Tenant.DeletionTimestamp.IsZero() {
		log.V(1).Info("S3Tenant is being deleted")
		s3Tenant.Status.Phase = s3v1alpha1.PhaseDeleting

		message := "Deletion in progress"
		if deleting := meta.FindStatusCondition(s3Tenant.Status.Conditions, s3v1alpha1.ConditionTypeDeleting); deleting != nil {
			message = deleting.Message
		}
		r.setCondition(s3Tenant, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "Deleting", message)

		return
	}

	// all of these conditions need to be be in state true for this method to return true.
	reconciliation := false
	backendReady := false
	backendPending := false
	accountReady := false

	// message that will show on the ready condition.
	message := ""

	// get the reconciliation condition of the current generation.
	reconcileCondition := meta.FindStatusCondition(s3Tenant.Status.Conditions, s3v1alpha1.ConditionTypeReconcileSucceeded)
	if reconcileCondition != nil {
		// check if the condition is from the current generation.
		if reconcileCondition.ObservedGeneration == s3Tenant.GetGeneration() {
			// if the condition is true, we can assume the reconciliation was successful.
			if reconcileCondition.Status == metav1.ConditionTrue {
				log.V(1).Info("Reconciliation succeeded")
				message += "Reconciliation succeeded"

				// set the reconciliation state to true.
				reconciliation = true
			} else {
				log.V(1).Info("Reconciliation failed somewhere, overall ready state is false")
				message += "Reconciliation failed"
			}
		} else {
			log.V(1).Info("Reconciliation condition is not from the current generation, overall ready state is false")
			message += "Reconciliation condition is not from the current generation"
		}
	} else {
		log.V(1).Info("Reconciliation condition not found, overall ready state is false")
		message += "Reconciliation condition not found"
	}

	// check whether the backing resource is ready.
	backendCondition := meta.FindStatusCondition(s3Tenant.Status.Conditions, s3v1alpha1.ContitionTypeBackingResourceReady)
	// we can directly translate the state of the condition to our variable.
	if backendCondition != nil {
		if backendCondition.ObservedGeneration == s3Tenant.GetGeneration() {
			if backendCondition.Status == metav1.ConditionTrue {
				log.V(1).Info("Backing resource is ready")
				message += ", Backing resource is ready"
				backendReady = true
			} else {
				log.V(1).Info("Backing resource is not ready")
				message += ", Backing resource is not ready"
			}
		} else {
			log.V(1).Info("Backing resource condition is not from the current generation, overall ready state is false")
			message += ", Backing resource condition is not from the current generation"
		}
	} else {
		log.V(1).Info("Backing resource condition not found, overall ready state is false")
		message += ", Backing resource condition not found"
	}

	// check whether the pending condition is set.
	pendingCondition := meta.FindStatusCondition(s3Tenant.Status.Conditions, s3v1alpha1.ConditionTypePending)
	if pendingCondition != nil {
		if pendingCondition.ObservedGeneration == s3Tenant.GetGeneration() {
			if pendingCondition.Status == metav1.ConditionTrue {
				log.V(1).Info("Backing resource is pending")
				message += ", Backing resource is pending"
				backendPending = true
			} else {
				log.V(1).Info("Backing resource is not pending")
				message = strings.TrimSuffix(message, ", Backing resource is pending")
			}
		} else {
			log.V(1).Info("Pending condition is not from the current generation, will be ignored")
			message += ", Pending condition is not from the current generation. We're assuming it's ready"
		}
	} else {
		log.V(1).Info("Pending condition not found, backend should be ready")
		message = strings.TrimSuffix(message, ", Backing resource is pending")
	}

	// check whether the account is ready.
	accountCondition := meta.FindStatusCondition(s3Tenant.Status.Conditions, s3v1alpha1.ConditionTypeAccountReady)
	if accountCondition != nil {
		if accountCondition.ObservedGeneration == s3Tenant.GetGeneration() {
			if accountCondition.Status == metav1.ConditionTrue {
				log.V(1).Info("S3TenantAccount is ready")
				message += ", Account is ready"
				accountReady = true
			} else {
				log.V(1).Info("S3TenantAccount is not ready")
				message += ", Account is not ready"
			}
		} else {
			log.V(1).Info("Account condition is not from the current generation, overall ready state is false")
			message += ", Account condition is not from the current generation"
		}
	} else {
		log.V(1).Info("Account condition not found, overall ready state is false")
		message += ", Account condition not found"
	}

	if reconciliation && backendReady && !backendPending && accountReady {
		log.V(1).Info("S3Tenant is ready")
		s3Tenant.Status.Phase = s3v1alpha1.PhaseBound
		r.setCondition(s3Tenant, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, "S3TenantReady", message)
	} else if reconciliation && backendReady && backendPending {
		log.V(1).Info("S3Tenant is almost ready, you can perform storage operations, but some background tasks are still running")
		s3Tenant.Status.Phase = s3v1alpha1.PhaseBound
		r.setCondition(s3Tenant, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "S3TenantPending", message)
	} else {
		log.V(1).Info("S3Tenant is not ready")
		s3Tenant.Status.Phase = s3v1alpha1.PhaseBound
		r.setCondition(s3Tenant, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "S3TenantNotReady", message)
	}
}

func (r *S3TenantReconciler) reconcileTenantAccountReference(ctx context.Context, rctx *tenantReconcileContext) (err error) {
	// Route to either claim existing account or create new account based on spec
	if rctx.S3Tenant.Spec.S3TenantAccountRef != nil {
		// User wants to claim an existing account
		return r.claimExistingAccount(ctx, rctx)
	}

	// Default behavior: create a new account with generated name
	return r.createNewAccount(ctx, rctx)
}

func (r *S3TenantReconciler) claimExistingAccount(ctx context.Context, rctx *tenantReconcileContext) error {
	log := log.FromContext(ctx)
	accountName := rctx.S3Tenant.Spec.S3TenantAccountRef.Name

	log.V(1).Info("Attempting to claim existing S3TenantAccount", "accountName", accountName)

	// Fetch the account
	account := &s3v1alpha1.S3TenantAccount{}
	if err := r.Get(ctx, types.NamespacedName{Name: accountName}, account); err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Error(err, "S3TenantAccount not found for claiming", "accountName", accountName)
			r.emitEvent(rctx, corev1.EventTypeWarning, EventAccountClaimFailed,
				fmt.Sprintf("S3TenantAccount %s not found", accountName))
			return fmt.Errorf("S3TenantAccount %s not found: %w", accountName, err)
		}
		log.Error(err, "Failed to get S3TenantAccount", "accountName", accountName)
		return fmt.Errorf("failed to get S3TenantAccount %s: %w", accountName, err)
	}

	// Check if spec is already fully claimed by this exact tenant (all fields must match)
	alreadyClaimed := false
	if account.Spec.S3TenantRef != nil {
		if account.Spec.S3TenantRef.Name == rctx.S3Tenant.Name &&
			account.Spec.S3TenantRef.Namespace == rctx.S3Tenant.Namespace &&
			account.Spec.S3TenantRef.Kind == rctx.S3Tenant.Kind &&
			account.Spec.S3TenantRef.UID == rctx.S3Tenant.UID {
			alreadyClaimed = true
			log.V(1).Info("Account already claimed by this tenant", "accountName", accountName)
		}
	}

	// If not already claimed, set the spec to claim the account
	if !alreadyClaimed {
		log.V(1).Info("Setting spec.S3TenantRef to claim account", "accountName", accountName)
		r.emitEvent(rctx, corev1.EventTypeNormal, EventAccountClaiming,
			fmt.Sprintf("Claiming S3TenantAccount %s", accountName))

		// Validate the claim before proceeding
		if err := ValidateAccountClaim(ctx, r.Client, rctx.S3Tenant, account); err != nil {
			log.Error(err, "Account claim validation failed", "accountName", accountName)
			r.emitEvent(rctx, corev1.EventTypeWarning, EventAccountClaimFailed,
				fmt.Sprintf("Claim validation failed: %v", err))
			return fmt.Errorf("account claim validation failed: %w", err)
		}

		// Update account spec to claim it
		account.Spec.S3TenantRef = &corev1.ObjectReference{
			Name:      rctx.S3Tenant.Name,
			Kind:      rctx.S3Tenant.Kind,
			Namespace: rctx.S3Tenant.Namespace,
			UID:       rctx.S3Tenant.UID,
		}

		if err := r.Update(ctx, account); err != nil {
			log.Error(err, "Failed to update account spec for claiming", "accountName", accountName)
			r.emitEvent(rctx, corev1.EventTypeWarning, EventAccountClaimFailed,
				fmt.Sprintf("Failed to update account spec: %v", err))
			return fmt.Errorf("failed to update account spec for claiming: %w", err)
		}

		log.V(1).Info("Successfully set spec.S3TenantRef for claiming", "accountName", accountName)
		r.emitEvent(rctx, corev1.EventTypeNormal, EventAccountClaimed,
			fmt.Sprintf("Successfully claimed S3TenantAccount %s", accountName))

		rctx.DoRequeue = true // requeue to let account controller process the binding
	}

	// Update reconcile context and status to reference the claimed account
	rctx.Account = account
	if err := r.updateTenantStatusReference(ctx, rctx); err != nil {
		return err
	}

	return nil
}

func (r *S3TenantReconciler) createNewAccount(ctx context.Context, rctx *tenantReconcileContext) error {
	log := log.FromContext(ctx)

	// check if the tenant account already exists.
	accountName := fmt.Sprintf("s3tenant-%s", rctx.S3Tenant.UID)

	if err := r.Get(ctx, types.NamespacedName{Name: accountName}, rctx.Account); err != nil {
		// if the account was not found we're assuming it needs to be created.
		if client.IgnoreNotFound(err) == nil {
			log.V(1).Info(fmt.Sprintf("S3TenantAccount %s not found, creating it", accountName))
			r.emitEvent(rctx, corev1.EventTypeNormal, EventAccountCreating,
				fmt.Sprintf("Creating backing S3TenantAccount %s", accountName))

			account := r.generateTenantAccount(rctx.S3Tenant, accountName)
			if err := r.Create(ctx, account); err != nil {
				log.Error(err, fmt.Sprintf("Failed to create S3TenantAccount %s", accountName))
				r.emitEvent(rctx, corev1.EventTypeWarning, EventAccountCreateFailed,
					fmt.Sprintf("Failed to create S3TenantAccount: %v", err))
				return fmt.Errorf("failed to create S3TenantAccount %s: %w", accountName, err)
			}

			log.V(1).Info(fmt.Sprintf("S3TenantAccount %s created successfully, requeing", accountName))
			r.emitEvent(rctx, corev1.EventTypeNormal, EventAccountCreated,
				fmt.Sprintf("Successfully created S3TenantAccount %s", accountName))
			rctx.DoRequeue = true // requeue to ensure the account is fully initialized
			return nil
		} else {
			log.Error(err, fmt.Sprintf("Failed to get S3TenantAccount %s", accountName))
			return fmt.Errorf("failed to get S3TenantAccount %s: %w", accountName, err)
		}
	}

	// Update reconcile context and status to reference the created account
	if err := r.updateTenantStatusReference(ctx, rctx); err != nil {
		return err
	}

	return nil
}

func (r *S3TenantReconciler) updateTenantStatusReference(ctx context.Context, rctx *tenantReconcileContext) error {
	log := log.FromContext(ctx)

	reference := &corev1.ObjectReference{
		Name:       rctx.Account.Name,
		Kind:       rctx.Account.Kind,
		UID:        rctx.Account.UID,
		APIVersion: rctx.Account.APIVersion,
	}

	if !equality.Semantic.DeepEqual(rctx.S3Tenant.Status.S3TenantAccountRef, reference) {
		log.V(1).Info(fmt.Sprintf("Updating S3TenantAccount reference in S3Tenant %s", rctx.S3Tenant.Name))
		rctx.S3Tenant.Status.S3TenantAccountRef = reference
	}

	return nil
}

func (r *S3TenantReconciler) generateTenantAccount(s3Tenant *s3v1alpha1.S3Tenant, accountName string) *s3v1alpha1.S3TenantAccount {
	return &s3v1alpha1.S3TenantAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name: accountName,
		},
		Spec: s3v1alpha1.S3TenantAccountSpec{
			S3TenantRef: &corev1.ObjectReference{
				Name:      s3Tenant.Name,
				Kind:      s3Tenant.Kind,
				Namespace: s3Tenant.Namespace,
				UID:       s3Tenant.UID,
			},
			CommonTenantSpec: s3Tenant.Spec.CommonTenantSpec,
		},
		Status: s3v1alpha1.S3TenantAccountStatus{},
	}
}

//nolint:dupl // structurally similar to S3TenantAccount's reconcileFinalizerAndDlelete but operates on different types.
func (r *S3TenantReconciler) reconcileFinalizerAndDlelete(ctx context.Context, rctx *tenantReconcileContext) error {
	log := log.FromContext(ctx)

	// Migrate legacy finalizer to custom finalizer.
	if controllerutil.ContainsFinalizer(rctx.S3Tenant, legacyForegroundFinalizer) {
		controllerutil.RemoveFinalizer(rctx.S3Tenant, legacyForegroundFinalizer)
		controllerutil.AddFinalizer(rctx.S3Tenant, s3TenantFinalizer)
		log.Info("Migrated legacy finalizer to custom finalizer on S3Tenant")
		rctx.ObjectUpdated = true
		rctx.DoRequeue = true
		return nil
	}

	// check if the object is being deleted.
	if rctx.S3Tenant.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(rctx.S3Tenant, s3TenantFinalizer) {
			controllerutil.AddFinalizer(rctx.S3Tenant, s3TenantFinalizer)
			log.V(1).Info("Adding finalizer to S3Tenant")
			rctx.DoRequeue = true
			rctx.ObjectUpdated = true
			return nil
		}
	} else {
		log.V(1).Info("S3Tenant is being deleted")
		if controllerutil.ContainsFinalizer(rctx.S3Tenant, s3TenantFinalizer) {
			if err := r.finalize(ctx, rctx); err != nil {
				log.Error(err, "Failed to finalize S3Tenant")
				return err
			}

			if rctx.RequeAfter.Duration > 0 {
				// Finalization is waiting on a precondition. Keep the finalizer and poll again.
				log.V(1).Info("Finalization is waiting, keeping finalizer", "requeueAfter", rctx.RequeAfter.Duration)
				return nil
			}

			controllerutil.RemoveFinalizer(rctx.S3Tenant, s3TenantFinalizer)
			log.V(1).Info("Removing finalizer from S3Tenant")
			rctx.DoRequeue = true
			rctx.ObjectUpdated = true
			return nil
		}

		log.V(1).Info("Finalizer not present, deletion can proceed without further action")
		return nil
	}

	return nil
}

func (r *S3TenantReconciler) reconcileTenantAccountSpec(ctx context.Context, rctx *tenantReconcileContext) error {
	log := log.FromContext(ctx)

	// we basically just want to do a deepEqual check for the common tenant spec.
	if !equality.Semantic.DeepEqual(rctx.S3Tenant.Spec.CommonTenantSpec, rctx.Account.Spec.CommonTenantSpec) {
		log.V(1).Info("S3Tenant spec changed, updating S3TenantAccount spec")
		r.emitEvent(rctx, corev1.EventTypeNormal, EventConfigurationChanged,
			"Configuration changes detected, updating S3TenantAccount")

		rctx.Account.Spec.CommonTenantSpec = rctx.S3Tenant.Spec.CommonTenantSpec

		// we need to update the account spec.
		if err := r.Update(ctx, rctx.Account); err != nil {
			log.Error(err, "Failed to update S3TenantAccount spec")
			r.emitEvent(rctx, corev1.EventTypeWarning, EventConfigurationFailed,
				fmt.Sprintf("Failed to update S3TenantAccount: %v", err))
			return fmt.Errorf("failed to update S3TenantAccount spec: %w", err)
		}

		// if the spec was updated, we need to requeue the reconciliation loop.
		log.V(1).Info("S3TenantAccount spec updated, requeuing reconciliation loop")
		r.emitEvent(rctx, corev1.EventTypeNormal, EventConfigurationPending,
			"Waiting for S3TenantAccount to apply configuration changes")
		rctx.DoRequeue = true
	} else {
		log.V(1).Info("S3Tenant spec did not change, no update needed for S3TenantAccount spec")
	}

	return nil
}

func (r *S3TenantReconciler) reconcileTenantAccountStatus(ctx context.Context, rctx *tenantReconcileContext) {
	log := log.FromContext(ctx)

	// Always copy status from account to tenant, regardless of account phase
	if !equality.Semantic.DeepEqual(rctx.S3Tenant.Status.CommonTenantStatus, rctx.Account.Status.CommonTenantStatus) {
		log.V(1).Info("S3TenantAccount status changed, updating S3Tenant status")
		rctx.S3Tenant.Status.CommonTenantStatus = rctx.Account.Status.CommonTenantStatus
	} else {
		log.V(1).Info("S3TenantAccount status did not change, no update needed for S3Tenant status")
	}

	// Set AccountReady condition based on account phase
	previous := meta.FindStatusCondition(rctx.S3Tenant.Status.Conditions, s3v1alpha1.ConditionTypeAccountReady)
	if rctx.Account.Status.Phase == s3v1alpha1.PhaseBound {
		log.V(1).Info("S3TenantAccount is bound and ready")
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeAccountReady, metav1.ConditionTrue,
			"AccountReady", fmt.Sprintf("S3TenantAccount %s is ready", rctx.Account.Name))
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypePending, metav1.ConditionFalse,
			"TenantAccountReady", fmt.Sprintf("S3TenantAccount %s is ready", rctx.Account.Name))
	} else {
		log.V(1).Info("S3TenantAccount is not bound", "phase", rctx.Account.Status.Phase)
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeAccountReady, metav1.ConditionFalse,
			"AccountNotReady", fmt.Sprintf("S3TenantAccount %s is in phase %s", rctx.Account.Name, rctx.Account.Status.Phase))
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypePending, metav1.ConditionTrue,
			"TenantAccountNotPhaseBound", fmt.Sprintf("S3TenantAccount %s is still in state %s", rctx.Account.Name, rctx.Account.Status.Phase))

		// Emit only on entry. This runs before the finalizer step, so a tenant whose deletion
		// is blocked reaches it on every poll - every 30s now that waits are bounded, rather
		// than every ~17m under the old backoff.
		if previous == nil || previous.Status != metav1.ConditionFalse {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventAccountNotReady,
				fmt.Sprintf("S3TenantAccount %s is not ready (current phase: %s)", rctx.Account.Name, rctx.Account.Status.Phase))
		}
	}
}

func (r *S3TenantReconciler) reconcileLinkedBuckets(ctx context.Context, rctx *tenantReconcileContext) error {
	log := log.FromContext(ctx)

	// fetch all buckets linked to this tenant.
	buckets := &s3v1alpha1.S3BucketList{}
	opts := []client.ListOption{
		client.MatchingFields{"spec.s3TenantRef.namespacedName": rctx.S3Tenant.Namespace + "/" + rctx.S3Tenant.Name},
	}
	if err := r.List(ctx, buckets, opts...); err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "Failed to list buckets")
			return err
		}

		log.Info("No buckets registered to this tenant")
	}

	lbs := []string{}
	for i := range buckets.Items {
		lbs = append(lbs, buckets.Items[i].Name)
	}

	if equality.Semantic.DeepEqual(rctx.S3Tenant.Status.LinkedBuckets, lbs) {
		log.V(1).Info("No changes in linked buckets, skipping update")
		return nil
	}
	log.V(1).Info("Updating linked buckets in S3Tenant status", "buckets", lbs)
	rctx.S3Tenant.Status.LinkedBuckets = lbs

	return nil
}

// finalize handles any cleanup logic when the S3Tenant is being deleted.
func (r *S3TenantReconciler) finalize(ctx context.Context, rctx *tenantReconcileContext) error {
	// Add your finalization logic here.
	log := log.FromContext(ctx)
	log.Info(fmt.Sprintf("Finalizing S3Tenant %s", rctx.S3Tenant.Name))

	// fetch most current usage to make sure we can delete the tenant.
	if err := r.reconcileLinkedBuckets(ctx, rctx); err != nil {
		log.Error(err, "Failed to update tenant usage")
	}

	// if annotation is set to ignore unmanaged buckets only linkedBuckets are considered.
	ignoreUnmanaged := false
	if val, exists := rctx.S3Tenant.Annotations[s3v1alpha1.AnnotationIgnoreUnmanagedBuckets]; exists && strings.EqualFold(val, "true") {
		ignoreUnmanaged = true
	}

	// Always block deletion if operator-managed buckets still reference this tenant.
	// LinkedBuckets is freshly populated by reconcileLinkedBuckets via the field indexer.
	//
	// This is a wait, not a failure: the tenant is held here until the buckets are gone.
	// It is signaled through rctx.RequeAfter rather than by returning an error so the
	// workqueue polls on a bounded schedule instead of backing off exponentially, and so
	// the status reads as "deleting" rather than "broken". The watch on S3Bucket wakes us
	// as soon as the last bucket disappears; this interval is only a backstop.
	if len(rctx.S3Tenant.Status.LinkedBuckets) > 0 {
		r.waitForDeletion(ctx, rctx, "WaitingForBuckets",
			fmt.Sprintf("Waiting for %d linked S3Bucket(s) to be deleted: [%s]",
				len(rctx.S3Tenant.Status.LinkedBuckets), strings.Join(rctx.S3Tenant.Status.LinkedBuckets, ", ")))
		return nil
	}

	// Additionally check backend bucket count for manually-created buckets (unless opted out).
	if !ignoreUnmanaged && rctx.S3Tenant.Status.TenantUsage.BucketCount > 0 {
		r.waitForDeletion(ctx, rctx, "WaitingForBuckets",
			fmt.Sprintf("Backend still reports %d bucket(s). Delete them, or set annotation %s=true to ignore unmanaged buckets",
				rctx.S3Tenant.Status.TenantUsage.BucketCount, s3v1alpha1.AnnotationIgnoreUnmanagedBuckets))
		return nil
	}

	// make sure the tenantref is removed from the account.
	rctx.Account.Spec.S3TenantRef = nil
	if err := r.Update(ctx, rctx.Account); err != nil {
		log.Error(err, "Failed to remove S3TenantRef from S3TenantAccount")
		return fmt.Errorf("failed to remove S3TenantRef from S3TenantAccount: %w", err)
	}

	return nil
}

// waitForDeletion records that deletion is blocked on a precondition and schedules a
// bounded requeue. It deliberately leaves ConditionTypeReconcileSucceeded true: waiting
// for linked buckets is expected progress, not a reconciliation failure, and reporting it
// as a failure is what previously made a normal deletion look broken.
//
// The event is emitted only when the reason changes, per the state-change emission
// convention in docs/architecture/events.md - with bounded polling this path runs every
// 30s rather than every ~17m, so emitting per pass would be pure spam.
func (r *S3TenantReconciler) waitForDeletion(ctx context.Context, rctx *tenantReconcileContext, reason, message string) {
	log := log.FromContext(ctx)
	log.V(1).Info("Deletion is waiting on a precondition", "reason", reason, "message", message)

	existing := meta.FindStatusCondition(rctx.S3Tenant.Status.Conditions, s3v1alpha1.ConditionTypeDeleting)
	if existing == nil || existing.Reason != reason || existing.Status != metav1.ConditionTrue {
		r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantDeleting, message)
	}

	r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeDeleting, metav1.ConditionTrue, reason, message)
	r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, reason, message)
	rctx.RequeAfter = metav1.Duration{Duration: r.computeDeletionPollInterval(ctx, rctx)}
}

// computeDeletionPollInterval resolves how often a blocked deletion re-checks its
// precondition. Precedence mirrors the drain configuration in the S3Bucket controller:
// resource spec > StorageGrid spec.operations.deletion > hardcoded default.
func (r *S3TenantReconciler) computeDeletionPollInterval(ctx context.Context, rctx *tenantReconcileContext) time.Duration {
	if rctx.S3Tenant.Spec.DeletionPollInterval != nil {
		return rctx.S3Tenant.Spec.DeletionPollInterval.Duration
	}

	sg := &s3v1alpha1.StorageGrid{}
	if err := r.Get(ctx, client.ObjectKey{Name: rctx.S3Tenant.Spec.StorageGridRef.Name}, sg); err != nil {
		return s3v1alpha1.DefaultDeletionPollInterval
	}

	if sg.Spec.Operations != nil && sg.Spec.Operations.Deletion != nil && sg.Spec.Operations.Deletion.PollInterval != nil {
		return sg.Spec.Operations.Deletion.PollInterval.Duration
	}

	return s3v1alpha1.DefaultDeletionPollInterval
}

// shouldKeepOnTenant checks if an annotation should remain on S3Tenant
// rather than being automatically removed after propagation to S3TenantAccount.
// all of these annotations either are to be removed by the user or through a dedicated function when implementing.
func shouldKeepOnTenant(annotationKey string) bool {
	for _, keepAnnotation := range s3v1alpha1.TenantAnnotationsToKeep {
		if annotationKey == keepAnnotation {
			return true
		}
	}
	return false
}

func (r *S3TenantReconciler) reconcileTenantAnnotations(ctx context.Context, rctx *tenantReconcileContext) error {
	log := log.FromContext(ctx)

	if rctx.S3Tenant.Annotations == nil {
		log.V(1).Info("No annotations on S3Tenant, skipping annotation reconciliation")
		return nil
	}

	// we need to ensure certain annotations are set on the account.
	// filter away all annotations that start with the tenant prefix.
	updatedAnnotations := map[string]string{}
	for key, value := range rctx.S3Tenant.Annotations {
		if strings.HasPrefix(key, s3v1alpha1.AnnotationPrefixTenant) {
			// add to the list of annotations to be set on the account.
			updatedAnnotations[key] = value

			// Remove from Tenant UNLESS it's whitelisted to stay
			if !shouldKeepOnTenant(key) {
				delete(rctx.S3Tenant.Annotations, key)
				rctx.ObjectUpdated = true
			}
		}
	}

	// if the updated annotations are empty, we can skip the rest.
	if len(updatedAnnotations) == 0 {
		log.V(1).Info("No tenant annotations found on S3Tenant, skipping annotation")
		return nil
	}

	// make sure the account has an annotations map.
	if rctx.Account.Annotations == nil {
		rctx.Account.Annotations = map[string]string{}
	}

	// add any missing annotations to the account.
	accountUpdateRequired := false
	for key, value := range updatedAnnotations {
		// if the annotation is not set by the tenant, we keep it.
		if val, exists := rctx.Account.Annotations[key]; !exists {
			log.V(1).Info("Updating annotation on S3TenantAccount", "key", key, "value", value)
			rctx.Account.Annotations[key] = value
			accountUpdateRequired = true
		} else if val != value {
			// if the annotation is set by the tenant, we check if it is the same.
			log.V(1).Info("Overriding annotation on S3TenantAccount", "key", key, "oldValue", val, "newValue", value)
			rctx.Account.Annotations[key] = value
			accountUpdateRequired = true
		}
	}

	if accountUpdateRequired {
		log.V(1).Info("Updating annotations on S3TenantAccount")
		return r.Update(ctx, rctx.Account)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *S3TenantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	tenantRefFunc := func(obj client.Object) []string {
		bucket := obj.(*s3v1alpha1.S3Bucket)
		ns := bucket.Spec.S3TenantRef.Namespace
		if ns == "" {
			ns = bucket.Namespace
		}
		return []string{ns + "/" + bucket.Spec.S3TenantRef.Name}
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &s3v1alpha1.S3Bucket{}, "spec.s3TenantRef.namespacedName", tenantRefFunc); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&s3v1alpha1.S3Tenant{}, builder.WithPredicates(
			PredicateWithoutStatusChange(),
		)).
		Owns(&corev1.Secret{}).
		Watches(
			&s3v1alpha1.S3TenantAccount{},
			handler.EnqueueRequestsFromMapFunc(r.mapAccountToTenant),
		).
		Watches(
			&s3v1alpha1.S3Bucket{},
			handler.EnqueueRequestsFromMapFunc(r.mapBucketToTenant),
			builder.WithPredicates(bucketDeletionRelevantPredicate()),
		).
		Complete(r)
}

// bucketDeletionRelevantPredicate limits the S3Bucket watch to the events that can change
// a tenant's LinkedBuckets or unblock its deletion. Delete is the one that matters most:
// without it nothing wakes a terminating tenant when its last bucket disappears, and it
// only reconciles again on the workqueue backoff. Status-only bucket updates are ignored so
// a tenant with many buckets is not reconciled on every usage refresh.
//
// Note PredicateWithoutStatusChange is applied to For(), not to Watches(), so this watch
// needs its own predicate.
func bucketDeletionRelevantPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return true },
		DeleteFunc: func(e event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Only the moment a bucket starts terminating is interesting.
			return e.ObjectOld.GetDeletionTimestamp().IsZero() && !e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

// mapBucketToTenant enqueues the S3Tenant that the given S3Bucket references, so that a
// tenant blocked on linked buckets reacts immediately when one is created or deleted.
func (r *S3TenantReconciler) mapBucketToTenant(ctx context.Context, obj client.Object) []ctrl.Request {
	bucket, ok := obj.(*s3v1alpha1.S3Bucket)
	if !ok {
		return nil
	}

	return []ctrl.Request{{NamespacedName: resolveBucketTenantKey(bucket)}}
}

func (r *S3TenantReconciler) mapAccountToTenant(ctx context.Context, obj client.Object) []ctrl.Request {
	log := log.FromContext(ctx)
	log.V(1).Info("Mapping S3TenantAccount to S3Tenant")

	account, ok := obj.(*s3v1alpha1.S3TenantAccount)
	if !ok {
		// should actually never happen as this method is only called for S3TenantAccount objects.
		log.Error(fmt.Errorf("object is not a S3TenantAccount"), "Failed to map S3TenantAccount to S3Tenant")
		return nil
	}

	if account.Spec.S3TenantRef == nil {
		log.V(1).Info("S3TenantAccount does not have a S3TenantRef, skipping")
		return nil
	}

	return []ctrl.Request{
		{
			NamespacedName: types.NamespacedName{
				Name:      account.Spec.S3TenantRef.Name,
				Namespace: account.Spec.S3TenantRef.Namespace,
			},
		},
	}
}

// emitEvent emits a Kubernetes event immediately.
func (r *S3TenantReconciler) emitEvent(
	rctx *tenantReconcileContext,
	eventType, reason, message string) {
	r.Recorder.Event(rctx.S3Tenant, eventType, reason, message)
}
