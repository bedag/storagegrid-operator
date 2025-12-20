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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	"github.com/bedag/storagegrid-operator/pkg/grid"
	"github.com/bedag/storagegrid-operator/pkg/kube"
)

// S3TenantClassReconciler reconciles a S3TenantClass object.
type S3TenantClassReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

type s3TenantClassReconcileContext struct {
	S3TenantClass *s3v1alpha1.S3TenantClass
	StorageGrid   *s3v1alpha1.StorageGrid
	DoRequeue     bool // indicates if the reconciliation should be requeued
	GridClient    *grid.GridClient
	Gateway       *grid.Gateway
	ObjectUpdated bool // indicates if the object was updated and needs to be persisted
}

const (
	// this is important to ensure that all linked tenants are deleted before the storageGrid is deleted.
	s3tenantClassFinalizer = "s3tenantclass.s3.bedag.ch/finalizer"
)

// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenantclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenantclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenantclasses/finalizers,verbs=update
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=s3tenantaccountss,verbs=get;list;watch
// +kubebuilder:rbac:groups=s3.bedag.ch,resources=storagegrids,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to.
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:.
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile

func (r *S3TenantClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	tenantClass := &s3v1alpha1.S3TenantClass{}

	err := r.Get(ctx, req.NamespacedName, tenantClass)
	if err != nil {
		log.Error(err, "Failed to get S3TenantClass")
		return ctrl.Result{}, err
	}
	log.V(1).Info(fmt.Sprintf("Reconciling S3TenantClass %s", tenantClass.Name))

	// create a new context for the reconciliation.
	rctx := &s3TenantClassReconcileContext{
		S3TenantClass: tenantClass,
		StorageGrid:   &s3v1alpha1.StorageGrid{},
		DoRequeue:     false, // default to not requeue
		ObjectUpdated: false,
	}

	err = r.doReconcile(ctx, rctx)

	// use conditions to derive the readiness state.
	r.deriveReadiness(ctx, rctx.S3TenantClass)

	// update the annotations if they were updated.
	// needs to be done before the status is updated because we lose the annotations otherwise.
	if rctx.ObjectUpdated {
		// creating a deep copy of the status to avoid modifying the original object.
		statusCopy := tenantClass.DeepCopy()

		log.V(1).Info("Annotations were updated, updating the object")
		if updateErr := r.Update(ctx, tenantClass); updateErr != nil {
			log.Error(updateErr, "Failed to update annotations")
			if err != nil {
				err = fmt.Errorf("reconciliation failed: %w, failed to update annotations: %w", err, updateErr)
			} else {
				err = fmt.Errorf("failed to update annotations: %w", updateErr)
			}
		} else {
			log.V(1).Info("Annotations updated successfully")
		}

		tenantClass.Status = statusCopy.Status // restore the status from the copy
	}

	if updateErr := r.Status().Update(ctx, rctx.S3TenantClass); updateErr != nil {
		log.Error(updateErr, "Failed to update status, requeuing")
		if err == nil {
			// no error occurred during reconciliation, but status update failed.
			rctx.DoRequeue = true // requeue to ensure status is updated
		}
		// if an error already occurred during reconciliation, we just return that error.
	}

	return ctrl.Result{Requeue: rctx.DoRequeue}, err
}

func (r *S3TenantClassReconciler) doReconcile(ctx context.Context, rctx *s3TenantClassReconcileContext) (err error) {
	log := log.FromContext(ctx)

	// examine DeletionTimestamp to determine if object is under deletion.
	err = r.reconcileFinalizerAndDlelete(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3TenantClass, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "FinalizerError", fmt.Sprintf("Failed to reconcile delete or finalizer: %v", err))
		return err
	}
	if rctx.DoRequeue {
		r.setCondition(rctx.S3TenantClass, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "FinalizerReconcileSucceeded", "Finalizer and deletion timestamp reconciled successfully, requeuing")
		return nil
	}

	// get the storageGrid.
	if err := r.Get(ctx, types.NamespacedName{Name: rctx.S3TenantClass.Spec.StorageGridRef.Name}, rctx.StorageGrid); err != nil {
		log.Error(err, fmt.Sprintf("Failed to retrieve StorageGrid from API %s", rctx.S3TenantClass.Spec.StorageGridRef.Name))
		r.setCondition(rctx.S3TenantClass, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "StorageGridNotFound", fmt.Sprintf("Failed to retrieve StorageGrid %s from API: %v", rctx.S3TenantClass.Spec.StorageGridRef.Name, err))
		return err
	}

	// ensure owner reference is set to the storageGrid.
	if err := ctrl.SetControllerReference(rctx.StorageGrid, rctx.S3TenantClass, r.Scheme); err != nil {
		r.setCondition(rctx.S3TenantClass, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "OwnerReferenceError", fmt.Sprintf("Failed to set owner reference for S3TenantClass to StorageGrid: %s", err.Error()))
		return err
	}

	// check if the grid is operative.
	err = r.reconcileGridReadiness(ctx, rctx.StorageGrid)
	if err != nil {
		r.setCondition(rctx.S3TenantClass, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "StorageGridNotReady", fmt.Sprintf("StorageGrid %s not ready", rctx.StorageGrid.Name))
		r.setCondition(rctx.S3TenantClass, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "StorageGridNotReady", fmt.Sprintf("StorageGrid %s not ready: %v", rctx.StorageGrid.Name, err))
		return err
	}

	err = r.initGridClient(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3TenantClass, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionFalse, "StorageGridNotReady", fmt.Sprintf("Failed to initialize grid client due to %s even though it reports ready, do you have the correct secret?", rctx.StorageGrid.Name))
		r.setCondition(rctx.S3TenantClass, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "GridClientInitFailed", fmt.Sprintf("Failed to initialize grid client: %v", err))
		r.emitEvent(rctx, corev1.EventTypeWarning, EventBackendConnectionFailed,
			fmt.Sprintf("Failed to connect to StorageGrid backend: %v", err))
		return err
	}
	r.setCondition(rctx.S3TenantClass, s3v1alpha1.ContitionTypeBackingResourceReady, metav1.ConditionTrue, "StorageGridReady", fmt.Sprintf("StorageGrid %s is ready", rctx.StorageGrid.Name))

	// Check if we're recovering from a connection failure
	backendCondition := meta.FindStatusCondition(rctx.S3TenantClass.Status.Conditions, s3v1alpha1.ContitionTypeBackingResourceReady)
	if backendCondition != nil && backendCondition.Status == metav1.ConditionFalse && backendCondition.Reason == "StorageGridNotReady" {
		r.emitEvent(rctx, corev1.EventTypeNormal, EventBackendConnectionRestored,
			fmt.Sprintf("Successfully connected to StorageGrid %s", rctx.StorageGrid.Name))
	}

	// cache the details from the tenantclass within the grid client.
	err = r.fetchFromBackend(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3TenantClass, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "FetchGatewayFailed", fmt.Sprintf("Failed to fetch gateway %s: %v", rctx.S3TenantClass.Spec.BackingID, err))
		r.emitEvent(rctx, corev1.EventTypeWarning, EventGatewayFetchFailed,
			fmt.Sprintf("Failed to fetch gateway %s: %v", rctx.S3TenantClass.Spec.BackingID, err))
		return err
	}

	r.reconcileTenantClassStatus(ctx, rctx)

	// Find all tenants referencing this TenantClass.
	err = r.reconcileLinkedTenants(ctx, rctx)
	if err != nil {
		r.setCondition(rctx.S3TenantClass, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionFalse, "LinkedTenantsError", fmt.Sprintf("Failed to reconcile linked tenants: %v", err))
		return err
	}

	// mark reconciliation as successful.
	r.setCondition(rctx.S3TenantClass, s3v1alpha1.ConditionTypeReconcileSucceeded, metav1.ConditionTrue, "ReconcileSucceeded", "Reconciliation completed successfully")

	return nil
}

func (r *S3TenantClassReconciler) deriveReadiness(ctx context.Context, tenantClass *s3v1alpha1.S3TenantClass) {
	log := log.FromContext(ctx)

	// all of these conditions need to be be in state true for this method to return true.
	reconciliation := false
	backendReady := false

	// message that will show on the ready condition.
	message := ""

	// get the reconciliation condition of the current generation.
	reconcileCondition := meta.FindStatusCondition(tenantClass.Status.Conditions, s3v1alpha1.ConditionTypeReconcileSucceeded)
	if reconcileCondition != nil {
		// check if the condition is from the current generation.
		if reconcileCondition.ObservedGeneration == tenantClass.GetGeneration() {
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
	backendCondition := meta.FindStatusCondition(tenantClass.Status.Conditions, s3v1alpha1.ContitionTypeBackingResourceReady)
	// we can directly translate the state of the condition to our variable.
	if backendCondition != nil {
		if backendCondition.ObservedGeneration == tenantClass.GetGeneration() {
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

	if reconciliation && backendReady {
		log.V(1).Info("S3TenantClass is ready")
		r.setCondition(tenantClass, s3v1alpha1.ConditionTypeReady, metav1.ConditionTrue, "S3TenantClassReady", message)
	} else {
		log.V(1).Info("S3TenantClass is not ready")
		r.setCondition(tenantClass, s3v1alpha1.ConditionTypeReady, metav1.ConditionFalse, "S3TenantClassNotReady", message)
	}
}

func (r *S3TenantClassReconciler) reconcileFinalizerAndDlelete(ctx context.Context, rctx *s3TenantClassReconcileContext) error {
	log := log.FromContext(ctx)

	log.V(1).Info(fmt.Sprintf("Reconciling finalizer for S3TenantClass %s", rctx.S3TenantClass.Name))

	// Check if the object is being deleted.
	if rctx.S3TenantClass.DeletionTimestamp.IsZero() {
		// if the object is not being deleted, add our finalizer if it is not already present.
		if !controllerutil.ContainsFinalizer(rctx.S3TenantClass, s3tenantClassFinalizer) {
			controllerutil.AddFinalizer(rctx.S3TenantClass, s3tenantClassFinalizer)
			log.V(1).Info("Adding finalizer to S3TenantClass", "finalizer", s3tenantClassFinalizer)
			rctx.DoRequeue = true // requeue to ensure finalizer is added
			rctx.ObjectUpdated = true
			return nil
		}
	} else {
		// the object is being deleted.
		if controllerutil.ContainsFinalizer(rctx.S3TenantClass, s3tenantClassFinalizer) {
			// our finalizer is present, so lets handle any external dependency.
			if err := r.finalize(ctx, rctx.S3TenantClass); err != nil {
				log.Error(err, "Failed to finalize s3TenantClass")
				return err
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(rctx.S3TenantClass, s3tenantClassFinalizer)
			log.V(1).Info("Removing finalizer from S3TenantClass", "finalizer", s3tenantClassFinalizer)
			rctx.DoRequeue = true // requeue to ensure finalizer is added
			rctx.ObjectUpdated = true
			return nil
		}

		return nil
	}
	return nil
}

func (r *S3TenantClassReconciler) reconcileGridReadiness(ctx context.Context, sg *s3v1alpha1.StorageGrid) error {
	log := log.FromContext(ctx)

	if !sg.Status.Ready {
		log.Error(fmt.Errorf("StorageGrid is not ready"), fmt.Sprintf("StorageGrid %s not ready", sg.Name))
		return fmt.Errorf("grid %s is not operative", sg.Name)
	}

	log.V(1).Info(fmt.Sprintf("StorageGrid %s is ready", sg.Name))
	return nil
}

func (r *S3TenantClassReconciler) initGridClient(ctx context.Context, rctx *s3TenantClassReconcileContext) error {
	log := log.FromContext(ctx)

	log.V(1).Info("Fetching credentials for client")
	username, password, err := kube.FetchCredentialsFromSecret(ctx, r.Client, rctx.StorageGrid.Spec.SecretRef.Namespace, rctx.StorageGrid.Spec.SecretRef.Name)
	if err != nil {
		log.Error(err, "Failed to fetch credentials")
		return err
	}

	log.V(1).Info("Initializing grid client if not initialized")
	client, err := grid.InitGridClient(username, password, rctx.StorageGrid.Spec.ManagementEndpoint)
	if err != nil {
		log.Error(err, "Failed to initialize grid client")
		return err
	}

	rctx.GridClient = client
	log.V(1).Info("Grid client initialized")

	return nil
}

func (r *S3TenantClassReconciler) fetchFromBackend(ctx context.Context, rctx *s3TenantClassReconcileContext) error {
	log := log.FromContext(ctx)

	gw, err := grid.FetchGateway(ctx, rctx.S3TenantClass.Spec.BackingID, rctx.GridClient)
	if err != nil {
		log.Error(err, "Failed to fetch gateway")
		return err
	}

	rctx.Gateway = gw

	log.V(1).Info(fmt.Sprintf("Fetched gateway %s for S3TenantClass %s", rctx.S3TenantClass.Spec.BackingID, rctx.S3TenantClass.Name))
	return nil
}

func (r *S3TenantClassReconciler) reconcileTenantClassStatus(ctx context.Context, rctx *s3TenantClassReconcileContext) {
	log := log.FromContext(ctx)
	refreshInterval := rctx.S3TenantClass.Spec.RefreshInterval

	// Always Reconcile S3 endpoints with preferred endpoints configuration as it can be changed by admin.
	r.reconcilePreferredEndpoints(ctx, rctx)

	log.V(1).Info(fmt.Sprintf("Reconcile S3TenantClass %s status", rctx.S3TenantClass.Name))
	if rctx.S3TenantClass.Status.LastUpdated.IsZero() || rctx.S3TenantClass.Status.LastUpdated.Add(refreshInterval.Duration).Before(metav1.Now().Time) {
		log.V(1).Info(fmt.Sprintf("Refreshing S3TenantClass %s status", rctx.S3TenantClass.Name))
		// update status fields.
		rctx.S3TenantClass.Status.DisplayName = grid.GetDisplayname(rctx.Gateway)
		rctx.S3TenantClass.Status.Port = grid.GetPort(rctx.Gateway)
		rctx.S3TenantClass.Status.Secure = grid.IsTLSEnabled(rctx.Gateway)
		rctx.S3TenantClass.Status.IPv4AccessEnabled = grid.IsIPv4AccessEnabled(rctx.Gateway)
		rctx.S3TenantClass.Status.IPv6AccessEnabled = grid.IsIPv6AccessEnabled(rctx.Gateway)
		rctx.S3TenantClass.Status.UntrustedNetworksDropped = grid.DropUntrustedNetworks(rctx.Gateway)

		// set last updated.
		rctx.S3TenantClass.Status.LastUpdated = metav1.Now()

		addressCount := 0
		if rctx.S3TenantClass.Status.S3EndpointConfig != nil {
			addressCount = len(rctx.S3TenantClass.Status.S3EndpointConfig.Addresses)
		}
		r.emitEvent(rctx, corev1.EventTypeNormal, EventGatewayStatusRefreshed,
			fmt.Sprintf("Gateway status refreshed: %d address(es)", addressCount))
	} else {
		log.V(1).Info(fmt.Sprintf("Skipping S3TenantClass %s status refresh, last updated at %s", rctx.S3TenantClass.Name, rctx.S3TenantClass.Status.LastUpdated.String()))
	}

	log.V(1).Info(fmt.Sprintf("S3TenantClass %s status reconciled", rctx.S3TenantClass.Name))
}

// reconcilePreferredEndpoints processes the PreferredEndpoints spec and updates the status.
// It handles endpoint discovery, filtering, and deduplication based on admin configuration.
func (r *S3TenantClassReconciler) reconcilePreferredEndpoints(ctx context.Context, rctx *s3TenantClassReconcileContext) {
	log := log.FromContext(ctx)

	// Discover all endpoints from gateway certificate SANs.
	discoveredURLs := grid.GetS3Endpoints(rctx.Gateway)
	discoveredVIPs := grid.GetVIPs(rctx.Gateway)
	allDiscovered := append(discoveredURLs, discoveredVIPs...)

	var finalEndpoints []string
	var defaultEndpoint string
	pathStyleAccess := rctx.S3TenantClass.Spec.UsePathStyleAccess

	if rctx.S3TenantClass.Spec.PreferredEndpoints != nil {
		// Admin explicitly configured preferred endpoints.
		preferredSpec := rctx.S3TenantClass.Spec.PreferredEndpoints
		defaultEndpoint = preferredSpec.DefaultEndpoint

		// Validate default endpoint exists in discovered SANs (warn if not, but still use it).
		if !slices.Contains(allDiscovered, defaultEndpoint) {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventEndpointNotInCertificate,
				fmt.Sprintf("Default endpoint %s not found in gateway certificate SANs", defaultEndpoint))
			log.V(1).Info(fmt.Sprintf("Default endpoint %s not in certificate, but keeping as configured", defaultEndpoint))
		}

		// Always include default as first entry.
		finalEndpoints = []string{defaultEndpoint}

		// Handle additionalEndpoints based on whether it's nil or explicit.
		if preferredSpec.AdditionalEndpoints == nil {
			// nil = include all discovered endpoints.
			log.V(1).Info("AdditionalEndpoints unset, including all discovered endpoints")
			finalEndpoints = append(finalEndpoints, allDiscovered...)
		} else {
			// Explicit list (even if empty) = only include what's specified.
			log.V(1).Info(fmt.Sprintf("AdditionalEndpoints set with %d entries", len(preferredSpec.AdditionalEndpoints)))
			for _, ep := range preferredSpec.AdditionalEndpoints {
				// Keep endpoint even if not in SANs (admin knows best), but warn.
				if !slices.Contains(allDiscovered, ep) {
					r.emitEvent(rctx, corev1.EventTypeWarning, EventEndpointNotInCertificate,
						fmt.Sprintf("Additional endpoint %s not found in gateway certificate SANs", ep))
					log.V(1).Info(fmt.Sprintf("Additional endpoint %s not in certificate, but keeping as configured", ep))
				}
				finalEndpoints = append(finalEndpoints, ep)
			}
		}
	} else {
		// No preferred endpoints = expose all discovered.
		log.V(1).Info("PreferredEndpoints not set, using all discovered endpoints")
		finalEndpoints = allDiscovered

		// Set default to first discovered endpoint (if any exist).
		if len(finalEndpoints) > 0 {
			defaultEndpoint = finalEndpoints[0]
		} else {
			r.emitEvent(rctx, corev1.EventTypeWarning, EventGatewayFetchFailed,
				"No S3 endpoints discovered from gateway certificate")
			log.V(1).Info("Warning: No endpoints available after processing")
		}
	}

	// Deduplicate final list and ensure default is first.
	finalEndpoints = deduplicateEndpoints(finalEndpoints)

	// Update status with processed endpoint configuration.
	rctx.S3TenantClass.Status.S3EndpointConfig = &s3v1alpha1.S3EndpointConfig{
		S3TenantClassName: rctx.S3TenantClass.Name,
		Addresses:         finalEndpoints,
		DefaultAddress:    defaultEndpoint,
		Port:              rctx.S3TenantClass.Status.Port,
		PathStyleAccess:   &pathStyleAccess,
	}

	log.V(1).Info(fmt.Sprintf("Reconciled endpoint config: %d address(es), default: %s", len(finalEndpoints), defaultEndpoint))
}

// deduplicateEndpoints removes duplicate entries from a slice of endpoints while preserving order.
func deduplicateEndpoints(endpoints []string) []string {
	seen := make(map[string]bool)
	result := []string{}
	for _, ep := range endpoints {
		if !seen[ep] {
			seen[ep] = true
			result = append(result, ep)
		}
	}
	return result
}

func (r *S3TenantClassReconciler) reconcileLinkedTenants(ctx context.Context, rctx *s3TenantClassReconcileContext) error {
	log := log.FromContext(ctx)

	// List all tenants that reference this TenantClass.
	tenants := &s3v1alpha1.S3TenantAccountList{}
	opts := []client.ListOption{
		client.MatchingFields{"status.s3EndpointConfig.s3TenantClassName": rctx.S3TenantClass.Name},
	}
	err := r.List(ctx, tenants, opts...)
	if err != nil {
		// if err is about unable to list its good.
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "Failed to list tenants")
			return nil
		}

		log.Info("No tenant registered with this TenantClass")
	}

	// We're assuming that all tenants exist.
	tenantNew := map[string]bool{}
	for i := range tenants.Items {
		tenantNew[tenants.Items[i].Status.TenantID] = true
	}

	// iteratote over all known tenants to check if any tenant was removed.
	for _, tenantID := range rctx.S3TenantClass.Status.S3TenantIDs {
		// this checks if the tenant was already discovered and added on a previous reconciliation.
		_, ok := tenantNew[tenantID]
		if ok {
			tenantNew[tenantID] = false
			continue
		}

		// This means the tenant is no longer on the cluster - meaning it needs to be removed from the allowlist.
		// only execute if the tenantClass is enforcing the allowlist.
		log.V(1).Info(fmt.Sprintf("Tenant %s is no longer in the cluster, removing from allowlist", tenantID))
		if rctx.S3TenantClass.Spec.Enforce {
			err = grid.RemoveTenantFromAllowlist(ctx, tenantID, rctx.Gateway, rctx.GridClient)
			if err != nil {
				log.Error(err, "Failed to remove tenant from allowlist")
				r.emitEvent(rctx, corev1.EventTypeWarning, EventAllowlistUpdateFailed,
					fmt.Sprintf("Failed to remove tenant %s from allowlist: %v", tenantID, err))
				return err
			}
			log.V(1).Info(fmt.Sprintf("Removed tenant %s from allowlist", tenantID))
			r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantRemovedFromAllowlist,
				fmt.Sprintf("Removed tenant %s from gateway allowlist", tenantID))
		} else {
			log.V(1).Info("Not enforcing allowlist, skipping removal")
		}
	}

	// create empty list of tenants to add to status.
	tenantList := []string{}
	// make sure all tenants are in the allowlist.
	for tenantID := range tenantNew {
		if rctx.S3TenantClass.Spec.Enforce {
			err = grid.AddTenantToAllowlist(ctx, tenantID, rctx.Gateway, rctx.GridClient)
			if err != nil {
				log.Error(err, "Failed to add tenant to allowlist")
				r.emitEvent(rctx, corev1.EventTypeWarning, EventAllowlistUpdateFailed,
					fmt.Sprintf("Failed to add tenant %s to allowlist: %v", tenantID, err))
				return err
			}
			if tenantNew[tenantID] {
				r.emitEvent(rctx, corev1.EventTypeNormal, EventTenantAddedToAllowlist,
					fmt.Sprintf("Added tenant %s to gateway allowlist", tenantID))
			}
		}

		tenantList = append(tenantList, tenantID)
	}

	rctx.S3TenantClass.Status.S3TenantIDs = tenantList

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *S3TenantClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&s3v1alpha1.S3TenantClass{}).
		Watches(
			&s3v1alpha1.S3TenantAccount{},
			handler.EnqueueRequestsFromMapFunc(r.mapTenantToTenantClass),
		).
		Complete(r)
}

func (r *S3TenantClassReconciler) mapTenantToTenantClass(ctx context.Context, obj client.Object) []reconcile.Request {
	tenant, ok := obj.(*s3v1alpha1.S3TenantAccount)
	// don't reconcile if the object is not a tenant.
	if !ok {
		return nil
	}

	// Handle delete events (Tenant is being deleted).
	if tenant.Status.S3EndpointConfig != nil && !tenant.DeletionTimestamp.IsZero() && tenant.Status.S3EndpointConfig.S3TenantClassName != "" {
		return []reconcile.Request{
			{NamespacedName: client.ObjectKey{Name: tenant.Status.S3EndpointConfig.S3TenantClassName}},
		}
	}

	// Handle create/update events.
	if tenant.Status.S3EndpointConfig != nil && tenant.Status.S3EndpointConfig.S3TenantClassName != "" {
		return []reconcile.Request{
			{NamespacedName: client.ObjectKey{Name: tenant.Status.S3EndpointConfig.S3TenantClassName}},
		}
	}

	// no reason to reconcile.
	return nil
}

func (r *S3TenantClassReconciler) finalize(ctx context.Context, tenantClass *s3v1alpha1.S3TenantClass) error {
	// Add your finalization logic here.
	log := log.FromContext(ctx)
	log.V(1).Info(fmt.Sprintf("Finalizing s3TenantClass %s", tenantClass.Status.DisplayName))

	// TenantClass can't be removed as long as there are still tenants using it.
	// Therefore, we need to check if there are still tenants using this TenantClass.
	// this is already checked by a validationWebhook but we need to make sure anyway.

	tenantList := &s3v1alpha1.S3TenantList{}
	err := r.List(ctx, tenantList, client.MatchingFields{"status.s3EndpointConfig.s3TenantClassName": tenantClass.Name})
	if err != nil {
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "Failed to list tenants")
			return err
		}

		log.Info("No tenant registered with this TenantClass")
		return nil
	}

	if len(tenantList.Items) > 0 {
		log.V(1).Info(fmt.Sprintf("Can't delete s3TenantClass %s because there are still tenants using it", tenantClass.Status.DisplayName))
		return fmt.Errorf("can't delete s3TenantClass %s because there are still tenants using it", tenantClass.Status.DisplayName)
	}

	return nil
}

func (r *S3TenantClassReconciler) setCondition(tenantClass *s3v1alpha1.S3TenantClass, condType string, status metav1.ConditionStatus, reason string, message string) {
	condition := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: tenantClass.GetGeneration(),
	}

	meta.SetStatusCondition(&tenantClass.Status.Conditions, condition)
}

// emitEvent emits a Kubernetes event immediately.
func (r *S3TenantClassReconciler) emitEvent(
	rctx *s3TenantClassReconcileContext,
	eventType, reason, message string) {
	r.Recorder.Event(rctx.S3TenantClass, eventType, reason, message)
}
