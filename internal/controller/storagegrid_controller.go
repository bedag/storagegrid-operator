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
	grid "github.com/bedag/storagegrid-operator/pkg/grid"
	"github.com/bedag/storagegrid-operator/pkg/kube"
)

// StorageGridReconciler reconciles a StorageGrid object.
type StorageGridReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

type sgReconcileContext struct {
	SG            *s3v1alpha1.StorageGrid
	DoReque       bool             // flag to indicate if we need to requeue the reconciliation
	GridClient    *grid.GridClient // cached grid client for this reconciliation
	ObjectUpdated bool
}

const (
	// this is important to ensure that all linked tenants are deleted before the storageGrid is deleted.
	sgFinalizer = "storagegrid.s3.bedag.ch/finalizer"
)

// +kubebuilder:rbac:groups=s3.bedag.ch,resources=storagegrids,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=storagegrids/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=storagegrids/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenants,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to.
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:.
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile
func (r *StorageGridReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	sg := &s3v1alpha1.StorageGrid{}

	log.V(1).Info(fmt.Sprintf("Reconciliation loop started for storageGrid %s", req.Name))

	if err := r.Get(ctx, req.NamespacedName, sg); err != nil {
		log.Error(err, "Failed to retrieve resource")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log.V(1).Info("Successfully retrieved resource in question")

	// initialize reconcile context.
	rctx := &sgReconcileContext{
		SG:            sg,
		DoReque:       false,
		ObjectUpdated: false,
	}

	statusBase := sg.DeepCopy()
	err := r.doReconcile(ctx, rctx)

	// use conditions to derive the readiness state.
	r.deriveReadiness(ctx, rctx.SG)

	// update the annotations if they were updated.
	// needs to be done before the status is updated because we lose the annotations otherwise.
	if rctx.ObjectUpdated {
		// creating a deep copy of the status to avoid modifying the original object.
		statusCopy := rctx.SG.DeepCopy()

		log.V(1).Info("Annotations were updated, updating the object")
		if updateErr := r.Update(ctx, rctx.SG); updateErr != nil {
			log.Error(updateErr, "Failed to update annotations")
			if err != nil {
				err = fmt.Errorf("reconciliation failed: %w, failed to update annotations: %w", err, updateErr)
			} else {
				err = fmt.Errorf("failed to update annotations: %w", updateErr)
			}
		} else {
			log.V(1).Info("Annotations updated successfully")
		}

		rctx.SG.Status = statusCopy.Status // restore the status from the copy
	}

	// Due to condition tracking status always needs to be updated.
	if updateErr := r.Status().Patch(ctx, rctx.SG, client.MergeFrom(statusBase)); updateErr != nil {
		log.Error(updateErr, "Failed to update status, requeuing")
		if err == nil {
			// no error occurred during reconciliation, but status update failed.
			rctx.DoReque = true // requeue to ensure status is updated
		}
		// if an error already occurred during reconciliation, we just return that error.
	}

	return ctrl.Result{Requeue: rctx.DoReque}, err
}

func (r *StorageGridReconciler) doReconcile(ctx context.Context, rctx *sgReconcileContext) (err error) {
	log := log.FromContext(ctx)

	err = r.reconcileFinalizerAndDlelete(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.SG, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "FinalizerError", fmt.Sprintf("Failed to reconcile delete or finalizer: %v", err))
		return err
	}
	if rctx.DoReque {
		r.setCondition(rctx.SG, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "FinalizerReconcileSucceeded", "Finalizer and deletion timestamp reconciled successfully, requeuing")
		return nil
	}

	r.reconcileDeletionPolicy(ctx, rctx.SG)

	// fetch credentials and initialize grid client.
	err = r.initGridClient(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.SG, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "GridClientInitError", fmt.Sprintf("Failed to initialize grid client: %v", err))
		// Check if it's a credentials issue or connection issue
		if err.Error() != "" {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventGridConnectionFailed,
				fmt.Sprintf("Failed to connect to StorageGrid: %v", err))
		}
		return err
	}

	// Check if we're recovering from a connection failure
	reachableCondition := meta.FindStatusCondition(rctx.SG.Status.Conditions, s3v1alpha1.ConditionTypeReachable)
	if reachableCondition != nil && reachableCondition.Status == metav1.ConditionFalse {
		r.emitEvent(rctx, corev1.EventTypeNormal, EventGridConnectionEstablished,
			fmt.Sprintf("Successfully connected to StorageGrid at %s", rctx.SG.Spec.ManagementEndpoint))
	}

	// make sure regions are fetched and set.
	err = r.reconcileRegions(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.SG, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "RegionUpdateError", fmt.Sprintf("Failed to fetch regions: %v", err))
		r.emitEvent(rctx, corev1.EventTypeWarning, EventRegionsFetchFailed,
			fmt.Sprintf("Failed to fetch regions: %v", err))
		return err
	}

	// checking if the grid is reachable and healthy.
	err = r.reconcileGridHealth(ctx, rctx)
	if err != nil {
		log.Error(err, "Failed to check grid health")
		r.setCondition(rctx.SG, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "GridHealthCheckError", fmt.Sprintf("Failed to check grid health: %v", err))
		r.setCondition(rctx.SG, s3v1alpha1.ConditionTypeReachable, metav1.ConditionFalse, "GridHealthCheckError", fmt.Sprintf("Failed to check grid health, considered unreachable: %v", err))
		return err
	}
	r.setCondition(rctx.SG, s3v1alpha1.ConditionTypeReachable, metav1.ConditionTrue, "GridReachable", "Grid is reachable and healthy")

	// set the condition to true, as we successfully reconciled the storageGrid.
	r.setCondition(rctx.SG, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "ReconcileSucceeded", "StorageGrid reconciled successfully")

	return nil
}

func (r *StorageGridReconciler) deriveReadiness(ctx context.Context, sg *s3v1alpha1.StorageGrid) {
	log := log.FromContext(ctx)

	// all of these conditions need to be be in state true for this method to return true.
	reconciliation := false
	reachable := false

	// message that will show on the ready condition.
	message := ""

	// get the reconciliation condition of the current generation.
	reconcileCondition := meta.FindStatusCondition(sg.Status.Conditions, s3v1alpha1.ConditionTypeReconcileSucceeded)
	if reconcileCondition != nil {
		// check if the condition is from the current generation.
		if reconcileCondition.ObservedGeneration == sg.GetGeneration() {
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
	reachableCondition := meta.FindStatusCondition(sg.Status.Conditions, s3v1alpha1.ConditionTypeReachable)
	// we can directly translate the state of the condition to our variable.
	if reachableCondition != nil {
		if reachableCondition.ObservedGeneration == sg.GetGeneration() {
			if reachableCondition.Status == metav1.ConditionTrue {
				log.V(1).Info("Grid is reachable and healthy")
				message += ", Grid is reachable and healthy"
				reachable = true
			} else {
				log.V(1).Info("Grid is not reachable or healthy, overall ready state is false")
				message += ", Grid is not reachable or healthy"
			}
		} else {
			log.V(1).Info("Reachable resource condition is not from the current generation, overall ready state is false")
			message += ", Reachable resource condition is not from the current generation"
		}
	} else {
		log.V(1).Info("Reachable resource condition not found, overall ready state is false")
		message += ", Reachable resource condition not found"
	}

	if reconciliation && reachable {
		log.V(1).Info("S3TenantClass is ready")
		sg.Status.Ready = true
		r.setCondition(sg, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, "StoragegridReady", message)
	} else {
		log.V(1).Info("S3TenantClass is not ready")
		sg.Status.Ready = false
		r.setCondition(sg, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "StoragegridNotReady", message)
	}
}

func (r *StorageGridReconciler) finalize(ctx context.Context, sg *s3v1alpha1.StorageGrid) error {
	log := log.FromContext(ctx)
	log.Info("Finalizing storageGrid", "name", sg.Name)

	// Block finalization while S3TenantAccounts still reference this StorageGrid
	accountList := &s3v1alpha1.S3TenantAccountList{}
	if err := r.List(ctx, accountList); err != nil {
		return fmt.Errorf("failed to list S3TenantAccounts: %w", err)
	}

	var boundAccounts []string
	for _, account := range accountList.Items {
		if account.Spec.StorageGridRef.Name == sg.Name {
			boundAccounts = append(boundAccounts, account.Name)
		}
	}

	if len(boundAccounts) > 0 {
		return fmt.Errorf("cannot finalize StorageGrid %s: %d S3TenantAccount(s) still reference it: %v",
			sg.Name, len(boundAccounts), boundAccounts)
	}

	return nil
}

func (r *StorageGridReconciler) reconcileFinalizerAndDlelete(ctx context.Context, rctx *sgReconcileContext) error {
	log := log.FromContext(ctx)

	// examine DeletionTimestamp to determine if object is under deletion.
	if rctx.SG.DeletionTimestamp.IsZero() {
		// if the object is not being deleted, add our finalizer if it is not already present.
		if !controllerutil.ContainsFinalizer(rctx.SG, sgFinalizer) {
			controllerutil.AddFinalizer(rctx.SG, sgFinalizer)
			log.V(1).Info("Adding finalizer to storageGrid")
			rctx.DoReque = true // requeue to ensure finalizer is added
			rctx.ObjectUpdated = true
			return nil
		}
	} else {
		// the object is being deleted.
		log.V(1).Info("StorageGrid is being deleted, checking for finalizer")
		if controllerutil.ContainsFinalizer(rctx.SG, sgFinalizer) {
			// our finalizer is present, so lets handle any external dependency.
			if err := r.finalize(ctx, rctx.SG); err != nil {
				log.Error(err, "Failed to finalize storageGrid")
				return err
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(rctx.SG, sgFinalizer)
			log.V(1).Info("Removing finalizer from storageGrid")
			rctx.DoReque = true // requeue to ensure finalizer is added
			rctx.ObjectUpdated = true
			return nil
		}
		// Stop reconciliation as the item is being deleted.

		log.V(1).Info("StorageGrid is being deleted, no finalizer present")
		return nil
	}

	return nil
}

// being really careful and verbose here, we want to ensure that the status is updated correctly.
// could lead to accidental deletion of tenants if not handled properly.
func (r *StorageGridReconciler) reconcileDeletionPolicy(ctx context.Context, sg *s3v1alpha1.StorageGrid) {
	log := log.FromContext(ctx)

	// make sure tenant deletion policy is set.
	// default to 7d if not set.
	duration, err := time.ParseDuration(s3v1alpha1.DefaultRetentionDuration)
	if err != nil {
		log.Error(err, fmt.Sprintf("Failed to parse default retention duration string %s, using 7d", s3v1alpha1.DefaultRetentionDuration))
		duration = 7 * 24 * time.Hour // default to 7 days
	}

	// if tenant deletion policy is not set, set it to the default retention duration.
	DefaultTenantDeletionPolicy := sg.Spec.DefaultTenantDeletionPolicy
	if DefaultTenantDeletionPolicy != nil {
		// if retention duration is not set, set it to the default retention duration.
		if DefaultTenantDeletionPolicy.RetentionDuration == nil {
			log.V(1).Info(fmt.Sprintf("Retention duration not set, using default retention duration of %s", duration.String()))
			DefaultTenantDeletionPolicy.RetentionDuration = &metav1.Duration{Duration: duration}
		} else {
			log.V(1).Info(fmt.Sprintf("Using retention duration of %s", DefaultTenantDeletionPolicy.RetentionDuration.Duration.String()))
		}

		// if policy is not set, set it to the default policy.
		if DefaultTenantDeletionPolicy.Policy == "" {
			log.V(1).Info(fmt.Sprintf("Deletion policy not set, using default policy %s", s3v1alpha1.DefaultTenantDeletionProcedure))
			DefaultTenantDeletionPolicy.Policy = s3v1alpha1.DefaultTenantDeletionProcedure
		}
	}

	// copy changed tenant deletion policy fields to the status.
	if sg.Status.DefaultTenantDeletionPolicy == nil {
		sg.Status.DefaultTenantDeletionPolicy = &s3v1alpha1.TenantDeletionPolicy{}
	}

	if DefaultTenantDeletionPolicy.RetentionDuration.Duration != sg.Status.DefaultTenantDeletionPolicy.RetentionDuration.Duration {
		log.V(1).Info(fmt.Sprintf("Updating retention duration from %s to %s", sg.Status.DefaultTenantDeletionPolicy.RetentionDuration.Duration.String(), DefaultTenantDeletionPolicy.RetentionDuration.Duration.String()))
		sg.Status.DefaultTenantDeletionPolicy.RetentionDuration = DefaultTenantDeletionPolicy.RetentionDuration
	}

	if DefaultTenantDeletionPolicy.Policy != sg.Status.DefaultTenantDeletionPolicy.Policy {
		log.V(1).Info(fmt.Sprintf("Updating deletion policy from %s to %s", sg.Status.DefaultTenantDeletionPolicy.Policy, DefaultTenantDeletionPolicy.Policy))
		sg.Status.DefaultTenantDeletionPolicy.Policy = DefaultTenantDeletionPolicy.Policy
	}
}

func (r *StorageGridReconciler) initGridClient(ctx context.Context, rctx *sgReconcileContext) error {
	log := log.FromContext(ctx)

	log.V(1).Info("Fetching grid client credentials")
	username, password, err := kube.FetchCredentialsFromSecret(ctx, r.Client, rctx.SG.Spec.SecretRef.Namespace, rctx.SG.Spec.SecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch grid client credentials")
		r.emitEvent(rctx, corev1.EventTypeWarning, EventGridCredentialsFailed,
			fmt.Sprintf("Failed to fetch credentials from secret %s/%s: %v", rctx.SG.Spec.SecretRef.Namespace, rctx.SG.Spec.SecretRef.Name, err))
		return err
	}

	// initialize client for grid.
	log.V(1).Info("Initializing grid client if not initialized")
	client, err := grid.InitGridClient(username, password, rctx.SG.Spec.ManagementEndpoint)
	if err != nil {
		log.Error(err, "Failed to initialize grid client")
		return err
	}
	log.V(1).Info("Grid client initialized successfully")

	rctx.GridClient = client

	return nil
}

func (r *StorageGridReconciler) reconcileRegions(ctx context.Context, rctx *sgReconcileContext) error {
	log := log.FromContext(ctx)

	regions, err := grid.GetRegions(ctx, rctx.GridClient)
	if err != nil {
		log.Error(err, "Failed to get regions")
		return err
	}

	slices.Sort(*regions)
	slices.Sort(rctx.SG.Status.Regions)
	if slices.Equal(rctx.SG.Status.Regions, *regions) {
		log.V(1).Info("Regions are already up to date")
	} else {
		log.V(1).Info("Updating regions", "oldRegions", rctx.SG.Status.Regions, "newRegions", *regions)
		rctx.SG.Status.Regions = *regions
		r.emitEvent(rctx, corev1.EventTypeNormal, EventRegionsUpdated,
			fmt.Sprintf("Updated available regions: %v", *regions))
	}

	// make sure a default region is set.
	defaultRegion := rctx.SG.Status.Regions[0]
	if rctx.SG.Spec.DefaultBucketRegion != "" {
		// use first region from slice.
		defaultRegion = rctx.SG.Spec.DefaultBucketRegion
	} else {
		// check if the specified region is valid.
		if !slices.Contains(rctx.SG.Status.Regions, rctx.SG.Spec.DefaultBucketRegion) {
			log.Error(fmt.Errorf("specified region %s does not exist", rctx.SG.Spec.DefaultBucketRegion), "Region does not exist")
		} else {
			defaultRegion = rctx.SG.Spec.DefaultBucketRegion
		}
	}

	if rctx.SG.Status.DefaultBucketRegion != defaultRegion {
		log.V(1).Info(fmt.Sprintf("Updating default region from %s to %s", rctx.SG.Status.DefaultBucketRegion, defaultRegion))
		old := rctx.SG.Status.DefaultBucketRegion
		rctx.SG.Status.DefaultBucketRegion = defaultRegion
		if old != "" {
			r.emitEvent(rctx, corev1.EventTypeNormal, EventDefaultRegionSet,
				fmt.Sprintf("Default bucket region changed from %s to %s", old, defaultRegion))
		} else {
			r.emitEvent(rctx, corev1.EventTypeNormal, EventDefaultRegionSet,
				fmt.Sprintf("Default bucket region set to %s", defaultRegion))
		}
	}

	return nil
}

func (r *StorageGridReconciler) reconcileGridHealth(ctx context.Context, rctx *sgReconcileContext) error {
	log := log.FromContext(ctx)

	log.V(1).Info("Checking grid health")
	isOperative, err := grid.IsOperative(ctx, rctx.GridClient, rctx.SG.Spec.MaxUnavailableNodes)
	if err != nil {
		log.Error(err, "Failed to check grid health")
		r.emitEvent(rctx, corev1.EventTypeWarning, EventGridHealthCheckFailed,
			fmt.Sprintf("Health check failed: %v", err))
		return err
	}

	// Check previous health state
	reachableCondition := meta.FindStatusCondition(rctx.SG.Status.Conditions, s3v1alpha1.ConditionTypeReachable)
	wasHealthy := reachableCondition != nil && reachableCondition.Status == metav1.ConditionTrue

	if isOperative {
		log.V(1).Info("Grid is healthy and operative")
		// Emit recovery event if grid was previously unhealthy
		if !wasHealthy && reachableCondition != nil {
			r.emitEvent(rctx, corev1.EventTypeNormal, EventGridHealthRecovered,
				"StorageGrid has recovered and is now healthy")
		}
	} else {
		log.V(1).Info("Grid is not healthy or operative, checking for reasons")
		reasons, err := grid.GetOperativeReason(ctx, rctx.GridClient, rctx.SG.Spec.MaxUnavailableNodes)
		if err != nil {
			log.Error(err, "Failed to get reason for grid health issues")
			return fmt.Errorf("failed to get reason for grid health issues: %w", err)
		}

		if len(reasons) > 0 {
			log.V(1).Info("Grid health issues found", "reasons", reasons)
			r.emitEvent(rctx, corev1.EventTypeWarning, EventGridUnhealthy,
				fmt.Sprintf("StorageGrid is unhealthy: %v", reasons))
			return fmt.Errorf("grid is not healthy, reasons: %v", reasons)
		} else {
			log.V(1).Info("Grid is considered not healthy but no specific issues found, please check the grid manually")
			r.emitEvent(rctx, corev1.EventTypeWarning, EventGridUnhealthy,
				"StorageGrid is unhealthy but no specific issues identified")
			return fmt.Errorf("grid is not healthy but no specific issues found, please check the grid manually")
		}
	}

	return nil
}

func (r *StorageGridReconciler) setCondition(sg *s3v1alpha1.StorageGrid, condType string, status metav1.ConditionStatus, reason string, message string) {
	// add or update with given condition.
	condition := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: sg.GetGeneration(),
	}

	meta.SetStatusCondition(&sg.Status.Conditions, condition)
}

// emitEvent emits a Kubernetes event immediately.
func (r *StorageGridReconciler) emitEvent(
	rctx *sgReconcileContext,
	eventType, reason, message string) {
	r.Recorder.Event(rctx.SG, eventType, reason, message)
}

// SetupWithManager sets up the controller with the Manager.
func (r *StorageGridReconciler) SetupWithManager(mgr ctrl.Manager) error {
	gridRefFunc := func(obj client.Object) []string {
		sg := obj.(*s3v1alpha1.S3Tenant)
		return []string{sg.Spec.StorageGridRef.Name}
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &s3v1alpha1.S3Tenant{}, "spec.storageGridRef.name", gridRefFunc); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&s3v1alpha1.StorageGrid{}, builder.WithPredicates(
			PredicateWithoutStatusChange(),
		)).
		Complete(r)
}
