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
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

	s3v1alpha1 "git.mgmtbi.ch/cloud/storagegrid-operator/api/v1alpha1"
)

// S3TenantReconciler reconciles a S3Tenant object.
type S3TenantReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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
}

const (
	tenantFinalizer           = "kubernetes.io/foregroundDeletion"
	errorAccountNotPhaseBound = "backing S3TenantAccount is not in phase Bound"
)

// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenants/finalizers,verbs=update
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenantaccounts,verbs=get;list;watch;create;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to.
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by.
// the S3Tenant object against the actual cluster state, and then.
// perform operations to make the cluster state reflect the state specified by.
// the user.
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
	if updateErr := r.Status().Update(ctx, rctx.S3Tenant); updateErr != nil {
		log.Error(updateErr, "Failed to update status, requeuing")
		if err == nil {
			// no error occurred during reconciliation, but status update failed.
			rctx.DoRequeue = true // requeue to ensure status is updated
		}
		// if an error already occurred during reconciliation, we just return that error.
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

	// examine DeletionTimestamp to determine if object is under deletion.
	err = r.reconcileFinalizerAndDlelete(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "ReconcileFailed", fmt.Sprintf("Failed to reconcile delete or finalizer %s", err.Error()))
		return err
	}
	if rctx.DoRequeue {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "FinalizerReconcileSucceeded", "Finalizer and deletion timestamp reconciled successfully, requeuing")
		return nil
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
	err = r.reconcileTenantAccountStatus(ctx, rctx)
	if err != nil && err.Error() == errorAccountNotPhaseBound {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypePending, metav1.ConditionTrue, "TenantAccountNotPhaseBound", fmt.Sprintf("S3TenantAccount %s is still in state %s", rctx.Account.Name, rctx.Account.Status.Phase))
	} else if err != nil {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "TenantAccountStatusSyncFailed", fmt.Sprintf("Failed to sync tenant account status: %s", err.Error()))
	}
	r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypePending, metav1.ConditionFalse, "TenantAccountReady", fmt.Sprintf("S3TenantAccount %s is ready", rctx.Account.Name))

	// get all the buckets linked to this tenant.
	err = r.reconcileLinkedBuckets(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "ReconcileFailed", fmt.Sprintf("Failed to reconcile linked buckets: %s", err.Error()))
	}

	// reconciliation ran successfully.
	r.setCondition(rctx.S3Tenant, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "ReconcileSucceeded", "Reconciliation completed successfully")

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

	// all of these conditions need to be be in state true for this method to return true.
	reconciliation := false
	backendReady := false
	backendPending := false

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

	if reconciliation && backendReady && !backendPending {
		log.V(1).Info("S3Tenant is ready")
		s3Tenant.Status.Phase = "Bound"
		r.setCondition(s3Tenant, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, "S3TenantReady", message)
	} else if reconciliation && backendReady && backendPending {
		log.V(1).Info("S3Tenant is almost ready, you can perform storage operations, but some background tasks are still running")
		s3Tenant.Status.Phase = "Pending"
		r.setCondition(s3Tenant, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "S3TenantPending", message)
	} else {
		log.V(1).Info("S3Tenant is not ready")
		s3Tenant.Status.Phase = "Failed"
		r.setCondition(s3Tenant, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "S3TenantNotReady", message)
	}
}

func (r *S3TenantReconciler) reconcileTenantAccountReference(ctx context.Context, rctx *tenantReconcileContext) (err error) {
	log := log.FromContext(ctx)

	// check if the tenant account already exists.
	accountName := fmt.Sprintf("s3tenant-%s", rctx.S3Tenant.UID)

	if err := r.Get(ctx, types.NamespacedName{Name: accountName}, rctx.Account); err != nil {
		// if the account was not found we're assuming it needs to be created.
		if client.IgnoreNotFound(err) == nil {
			log.V(1).Info(fmt.Sprintf("S3TenantAccount %s not found, creating it", accountName))
			account := r.generateTenantAccount(rctx.S3Tenant, accountName)
			if err := r.Create(ctx, account); err != nil {
				log.Error(err, fmt.Sprintf("Failed to create S3TenantAccount %s", accountName))
				return fmt.Errorf("failed to create S3TenantAccount %s: %w", accountName, err)
			}

			log.V(1).Info(fmt.Sprintf("S3TenantAccount %s created successfully, requeing", accountName))
			rctx.DoRequeue = true // requeue to ensure the account is fully initialized
			return nil
		} else {
			log.Error(err, fmt.Sprintf("Failed to get S3TenantAccount %s", accountName))
			return fmt.Errorf("failed to get S3TenantAccount %s: %w", accountName, err)
		}
	} else {
		log.V(1).Info(fmt.Sprintf("S3TenantAccount %s exists", accountName))
		// if the account exists, we need to update the reference in the tenant.
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
	}

	return nil
}

func (r *S3TenantReconciler) generateTenantAccount(s3Tenant *s3v1alpha1.S3Tenant, accountName string) *s3v1alpha1.S3TenantAccount {
	return &s3v1alpha1.S3TenantAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name: accountName,
			// make sure annotation is added to allow class change.
			Annotations: map[string]string{
				AnnotationAllowTenantClassNameChange: "true",
			},
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

func (r *S3TenantReconciler) reconcileFinalizerAndDlelete(ctx context.Context, rctx *tenantReconcileContext) error {
	log := log.FromContext(ctx)

	// check if the object is being deleted.
	if rctx.S3Tenant.DeletionTimestamp.IsZero() {
		// if the object is not being deleted, add our finalizer if it is not already present.
		if !controllerutil.ContainsFinalizer(rctx.S3Tenant, tenantFinalizer) {
			controllerutil.AddFinalizer(rctx.S3Tenant, tenantFinalizer)
			log.V(1).Info("Adding finalizer to S3Tenant")
			rctx.DoRequeue = true // we need to requeue to ensure the finalizer is added
			rctx.ObjectUpdated = true
			return nil
		}
	} else {
		log.V(1).Info("Object is being deleted")
		if controllerutil.ContainsFinalizer(rctx.S3Tenant, tenantFinalizer) {
			// our finalizer is present, so lets handle any external dependency.
			if err := r.finalize(ctx, rctx); err != nil {
				log.Error(err, "Failed to finalize tenant")
				return err
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(rctx.S3Tenant, tenantFinalizer)
			log.V(1).Info("Removing finalizer from S3Tenant")
			rctx.DoRequeue = true // we need to requeue to ensure the finalizer is added
			rctx.ObjectUpdated = true
			return nil
		}

		// no finalizer is present, so we can proceed with deletion.
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
		rctx.Account.Spec.CommonTenantSpec = rctx.S3Tenant.Spec.CommonTenantSpec

		// we need to update the account spec.
		if err := r.Update(ctx, rctx.Account); err != nil {
			log.Error(err, "Failed to update S3TenantAccount spec")
			return fmt.Errorf("failed to update S3TenantAccount spec: %w", err)
		}

		// if the spec was updated, we need to requeue the reconciliation loop.
		log.V(1).Info("S3TenantAccount spec updated, requeuing reconciliation loop")
		rctx.DoRequeue = true
	} else {
		log.V(1).Info("S3Tenant spec did not change, no update needed for S3TenantAccount spec")
	}

	return nil
}

func (r *S3TenantReconciler) reconcileTenantAccountStatus(ctx context.Context, rctx *tenantReconcileContext) error {
	log := log.FromContext(ctx)

	// if the account is not ready, we cannot update the status.
	if rctx.Account.Status.Phase != s3v1alpha1.PhaseBound {
		log.V(1).Info("S3TenantAccount is not bound, skipping status update")
		return errors.New(errorAccountNotPhaseBound)
	}

	// copy the common tenant fields from the account to the tenant.
	if !equality.Semantic.DeepEqual(rctx.S3Tenant.Status.CommonTenantStatus, rctx.Account.Status.CommonTenantStatus) {
		log.V(1).Info("S3TenantAccount status changed, updating S3Tenant status")
		rctx.S3Tenant.Status.CommonTenantStatus = rctx.Account.Status.CommonTenantStatus
	} else {
		log.V(1).Info("S3TenantAccount status did not change, no update needed for S3Tenant status")
	}

	return nil
}

func (r *S3TenantReconciler) reconcileLinkedBuckets(ctx context.Context, rctx *tenantReconcileContext) error {
	log := log.FromContext(ctx)

	// fetch all buckets linked to this tenant.
	buckets := &s3v1alpha1.S3BucketList{}
	opts := []client.ListOption{
		client.MatchingFields{"spec.s3TenantRef.name": rctx.S3Tenant.Name},
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

	// if tenant still has buckets, abort deletion.
	if rctx.S3Tenant.Status.TenantUsage.BucketCount > 0 {
		return fmt.Errorf("cannot delete tenant with buckets")
	}

	// make sure the tenantref is removed from the account.
	rctx.Account.Spec.S3TenantRef = nil
	if err := r.Update(ctx, rctx.Account); err != nil {
		log.Error(err, "Failed to remove S3TenantRef from S3TenantAccount")
		return fmt.Errorf("failed to remove S3TenantRef from S3TenantAccount: %w", err)
	}

	return nil
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
		if strings.HasPrefix(key, TenantPrefix) {
			updatedAnnotations[key] = value

			// remove annotations from tenant.
			delete(rctx.S3Tenant.Annotations, key)
			rctx.ObjectUpdated = true
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
			log.V(1).Info("Overriding annotation on S3TenantAccount", "key", key, "oldValue", value, "newValue", val)
			rctx.Account.Annotations[key] = val
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
		sg := obj.(*s3v1alpha1.S3Bucket)
		return []string{sg.Spec.S3TenantRef.Name}
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &s3v1alpha1.S3Bucket{}, "spec.s3TenantRef.name", tenantRefFunc); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&s3v1alpha1.S3Tenant{}, builder.WithPredicates(
			PredicateWithoutStatusChange(),
		)).
		Owns(&corev1.Secret{}).
		Owns(&s3v1alpha1.S3Bucket{}).
		Watches(
			&s3v1alpha1.S3TenantAccount{},
			handler.EnqueueRequestsFromMapFunc(r.mapAccountToTenant),
		).
		Complete(r)
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
