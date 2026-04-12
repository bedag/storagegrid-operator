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

package webhook

import (
	"context"
	"fmt"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var storagegridlog = logf.Log.WithName("storagegrid-webhook")

type StorageGridValidator struct {
	k8sClient client.Client `json:"-"`
}

// SetupWebhookWithManager will setup the manager to manage the webhooks.
func (r *StorageGridValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	r.k8sClient = mgr.GetClient()

	return ctrl.NewWebhookManagedBy(mgr).
		For(&s3v1alpha1.StorageGrid{}).
		WithValidator(r).
		Complete()
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-s3-bedag-ch-v1alpha1-storagegrid,mutating=false,failurePolicy=fail,sideEffects=None,groups=s3.bedag.ch,resources=storagegrids,verbs=create;update;delete,versions=v1alpha1,name=vstoragegrid.kb.io,admissionReviewVersions=v1

var _ webhook.CustomValidator = &StorageGridValidator{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type.
func (r *StorageGridValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	storagegrid, ok := obj.(*s3v1alpha1.StorageGrid)
	if !ok {
		return nil, fmt.Errorf("object is not a StorageGrid")
	}

	storagegridlog.Info("validate create", "name", storagegrid.Name)

	// Validate S3OperationsTenantClass if specified
	if storagegrid.Spec.S3OperationsTenantClass != "" {
		if err := r.validateS3OperationsTenantClass(ctx, storagegrid.Spec.S3OperationsTenantClass); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type.
func (r *StorageGridValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	storagegrid, ok := newObj.(*s3v1alpha1.StorageGrid)
	if !ok {
		return nil, fmt.Errorf("object is not a StorageGrid")
	}

	storagegridlog.Info("validate update", "name", storagegrid.Name)

	// Validate S3OperationsTenantClass if specified
	if storagegrid.Spec.S3OperationsTenantClass != "" {
		if err := r.validateS3OperationsTenantClass(ctx, storagegrid.Spec.S3OperationsTenantClass); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ValidateDelete blocks deletion while S3TenantAccounts still reference this StorageGrid.
func (r *StorageGridValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	storagegrid, ok := obj.(*s3v1alpha1.StorageGrid)
	if !ok {
		return nil, fmt.Errorf("object is not a StorageGrid")
	}

	storagegridlog.Info("validate delete", "name", storagegrid.Name)

	// List all S3TenantAccounts that reference this StorageGrid
	accountList := &s3v1alpha1.S3TenantAccountList{}
	if err := r.k8sClient.List(ctx, accountList); err != nil {
		storagegridlog.Error(err, "failed to list S3TenantAccounts")
		return nil, fmt.Errorf("failed to list S3TenantAccounts: %w", err)
	}

	var boundAccounts []string
	for _, account := range accountList.Items {
		if account.Spec.StorageGridRef.Name == storagegrid.Name {
			boundAccounts = append(boundAccounts, account.Name)
		}
	}

	if len(boundAccounts) > 0 {
		return nil, fmt.Errorf("deletion of StorageGrid %s is blocked: %d S3TenantAccount(s) still reference it: %v",
			storagegrid.Name, len(boundAccounts), boundAccounts)
	}

	return nil, nil
}

// validateS3OperationsTenantClass checks if the specified S3TenantClass exists.
func (r *StorageGridValidator) validateS3OperationsTenantClass(ctx context.Context, tenantClassName string) error {
	log := logf.FromContext(ctx).WithValues("func", "validateS3OperationsTenantClass")
	log.V(1).Info("Validating S3OperationsTenantClass", "name", tenantClassName)

	tenantClass := &s3v1alpha1.S3TenantClass{}
	err := r.k8sClient.Get(ctx, client.ObjectKey{Name: tenantClassName}, tenantClass)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("S3TenantClass does not exist", "name", tenantClassName)
			return fmt.Errorf("S3OperationsTenantClass %s does not exist", tenantClassName)
		}
		log.Error(err, "Failed to get S3TenantClass", "name", tenantClassName)
		return fmt.Errorf("failed to validate S3OperationsTenantClass: %w", err)
	}

	log.Info("S3OperationsTenantClass is valid", "name", tenantClassName)
	return nil
}
