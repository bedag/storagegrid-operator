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

//nolint:dupl // S3Policy and GlobalS3Policy reconcilers are intentionally parallel implementations for different scopes.
package s3policies

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	log "sigs.k8s.io/controller-runtime/pkg/log"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

// GlobalS3PolicyReconciler reconciles a GlobalS3Policy object.
type GlobalS3PolicyReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

type globalPolicyReconcileContext struct {
	ObjectUpdated bool
	DoRequeue     bool
	Policy        *s3v1alpha1.GlobalS3Policy
}

// +kubebuilder:rbac:groups=s3.bedag.ch,resources=globals3policies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=globals3policies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=globals3policies/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the GlobalS3Policy object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *GlobalS3PolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("s3globalPolicy", req.NamespacedName)
	log.V(1).Info("Starting reconciliation")

	// initialize reconcile context
	rctx := &globalPolicyReconcileContext{
		Policy:        &s3v1alpha1.GlobalS3Policy{},
		ObjectUpdated: false,
		DoRequeue:     false,
	}

	// fetch the GlobalS3Policy instance^
	if err := r.Get(ctx, req.NamespacedName, rctx.Policy); err != nil {
		log.Error(err, "Failed to get GlobalS3Policy")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.V(1).Info("Successfully fetched GlobalS3Policy", "name", rctx.Policy.Name)

	err := r.doReconcile(ctx, rctx)

	// update the annotations if they were updated.
	// needs to be done before the status is updated because we lose the annotations otherwise.
	if rctx.ObjectUpdated {
		// creating a deep copy of the status to avoid modifying the original object.
		statusCopy := rctx.Policy.DeepCopy()

		log.V(1).Info("Object was updated")
		if updateErr := r.Update(ctx, rctx.Policy); updateErr != nil {
			log.Error(updateErr, "Failed to update annotations")
			if err != nil {
				err = fmt.Errorf("reconciliation failed: %w, failed to update annotations: %w", err, updateErr)
			} else {
				err = fmt.Errorf("failed to update annotations: %w", updateErr)
			}
		} else {
			log.V(1).Info("Annotations updated successfully")
		}

		rctx.Policy.Status = statusCopy.Status // restore the status from the copy
	}

	// update the status of the policy if it was updated during reconciliation.
	if updateErr := r.Status().Update(ctx, rctx.Policy); updateErr != nil {
		log.Error(updateErr, "Failed to update status, requeuing")
		if err == nil {
			// no error occurred during reconciliation, but status update failed.
			rctx.DoRequeue = true // we need to requeue to ensure the status is updated
		}
		// if an error already occurred during reconciliation, we just return that error.
	}

	return ctrl.Result{Requeue: rctx.DoRequeue}, err
}

func (r *GlobalS3PolicyReconciler) doReconcile(ctx context.Context, rctx *globalPolicyReconcileContext) error {
	err := r.reconcileFinalizerAndDelete(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.Policy, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "FinalizerError", fmt.Sprintf("Failed to reconcile delete or finalizer: %v", err))
		return err
	}
	if rctx.DoRequeue {
		r.setCondition(rctx.Policy, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "FinalizerReconcileSucceeded", "Finalizer and deletion timestamp reconciled successfully, requeuing")
		return nil
	}

	if err = r.reconcileRenderedPolicy(ctx, rctx); err != nil {
		r.setCondition(rctx.Policy, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "PolicyRenderError", fmt.Sprintf("Failed to render policy: %v", err))
		r.setCondition(rctx.Policy, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "PolicyRenderError", fmt.Sprintf("Failed to render policy: %v", err))
		rctx.Policy.Status.CommonPolicyStatus.Ready = false
		r.Recorder.Eventf(rctx.Policy, corev1.EventTypeWarning, "PolicyRenderFailed", "Failed to render policy: %v", err)
		return err
	}

	// set ready condition
	r.setCondition(rctx.Policy, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, "PolicyReady", "Policy rendered and ready to use")
	rctx.Policy.Status.CommonPolicyStatus.Ready = true

	return nil
}

func (r *GlobalS3PolicyReconciler) finalize(ctx context.Context, pol *s3v1alpha1.GlobalS3Policy) error {
	log := log.FromContext(ctx)
	log.Info("Finalizing policy", "name", pol.Name)

	// make sure delete is not possible as long as its still used in an access
	inUse, accessList := policyInUse(ctx, r.Client, pol.Name, pol.Namespace, pol.Kind)
	if inUse {
		err := fmt.Errorf("policy %s is still in use by s3accesses: %s; cannot delete. Remove policy from listed s3accesses first", pol.Name, strings.Join(accessList, ", "))
		log.Error(err, "Failed to finalize policy")
		return err
	}

	return nil
}

func (r *GlobalS3PolicyReconciler) reconcileFinalizerAndDelete(ctx context.Context, rctx *globalPolicyReconcileContext) error {
	log := log.FromContext(ctx)

	// examine DeletionTimestamp to determine if object is under deletion.
	if rctx.Policy.DeletionTimestamp.IsZero() {
		// if the object is not being deleted, add our finalizer if it is not already present.
		if !controllerutil.ContainsFinalizer(rctx.Policy, PolicyFinalizer) {
			controllerutil.AddFinalizer(rctx.Policy, PolicyFinalizer)
			log.V(1).Info("Adding finalizer to S3GlobalPolicy")
			rctx.DoRequeue = true // requeue to ensure finalizer is added
			rctx.ObjectUpdated = true
			return nil
		}
	} else {
		// the object is being deleted.
		log.V(1).Info("S3GlobalPolicy is being deleted, checking for finalizer")
		if controllerutil.ContainsFinalizer(rctx.Policy, PolicyFinalizer) {
			// our finalizer is present, so lets handle any external dependency.
			if err := r.finalize(ctx, rctx.Policy); err != nil {
				log.Error(err, "Failed to finalize S3GlobalPolicy")
				return err
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(rctx.Policy, PolicyFinalizer)
			log.V(1).Info("Removing finalizer from S3GlobalPolicy")
			rctx.DoRequeue = true // requeue to ensure finalizer is added
			rctx.ObjectUpdated = true
			return nil
		}
		// Stop reconciliation as the item is being deleted.

		log.V(1).Info("S3GlobalPolicy is being deleted, no finalizer present")
		return nil
	}

	return nil
}

func (r *GlobalS3PolicyReconciler) reconcileRenderedPolicy(ctx context.Context, rctx *globalPolicyReconcileContext) error {
	log := log.FromContext(ctx).WithValues("function", "reconcileRenderedPolicy")

	rendered, err := renderPolicy(rctx.Policy.Spec.CommonPolicySpec)
	if err != nil {
		log.Error(err, "Failed to render policy")
		return err
	}

	log.V(1).Info("Policy rendered successfully")

	rctx.Policy.Status.CommonPolicyStatus.RenderedPolicy = rendered
	return nil
}

func (r *GlobalS3PolicyReconciler) setCondition(pol *s3v1alpha1.GlobalS3Policy, condType string, status metav1.ConditionStatus, reason string, message string) {
	// add or update with given condition.
	condition := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: pol.GetGeneration(),
	}

	meta.SetStatusCondition(&pol.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *GlobalS3PolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&s3v1alpha1.GlobalS3Policy{}).
		Named("globals3policy").
		Complete(r)
}
