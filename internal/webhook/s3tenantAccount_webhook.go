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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

// log is for logging in this package.
var s3tenantAccountlog = log.Log.WithName("s3tenantAccount-webhook")

type S3TenantAccountValidator struct {
	// +kubebuilder:object:generate=false
	k8sClient client.Client `json:"-"`
	// Scheme    *runtime.Scheme `json:"-"`.
}

// SetupWebhookWithManager will setup the manager to manage the webhooks.
func (r *S3TenantAccountValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	r.k8sClient = mgr.GetClient()

	return ctrl.NewWebhookManagedBy(mgr).
		For(&s3v1alpha1.S3TenantAccount{}).
		WithValidator(r).
		Complete()
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-s3-bedag-ch-v1alpha1-s3tenantaccount,mutating=false,failurePolicy=fail,sideEffects=None,groups=s3.bedag.ch,resources=s3tenantaccounts,verbs=create;update;delete,versions=v1alpha1,name=vs3tenantaccount.kb.io,admissionReviewVersions=v1

var _ webhook.CustomValidator = &S3TenantAccountValidator{}

type S3TenantAccountDefaulter struct{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type.
func (r *S3TenantAccountValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	s3tenantAccount, ok := obj.(*s3v1alpha1.S3TenantAccount)
	if !ok {
		return nil, fmt.Errorf("object is not an S3TenantAccount")
	}

	s3tenantAccountlog.Info("validate create", "name", s3tenantAccount.Name)

	// Validate import requirements
	if s3tenantAccount.Annotations != nil {
		if importTenantID, hasImport := s3tenantAccount.Annotations[s3v1alpha1.AnnotationImportTenant]; hasImport {
			// Import requires root secret reference
			if s3tenantAccount.Spec.RootSecretRef == nil || s3tenantAccount.Spec.RootSecretRef.Name == "" {
				return nil, fmt.Errorf("import requires spec.rootSecretRef to be specified: root credentials for tenant '%s' must be provided in a pre-existing secret (cannot be rotated via API)", importTenantID)
			}
		}
	}

	// Check if the TenantAccountClass exists.
	exists, err := r.tenantClassExists(ctx, s3tenantAccount.Spec.S3TenantClassName)
	if err != nil {
		return nil, err
	}

	// block creation if the TenantAccountClass does not exist.
	if !exists {
		return nil, fmt.Errorf("TenantAccountClass %s does not exist", s3tenantAccount.Spec.S3TenantClassName)
	}

	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type.
func (r *S3TenantAccountValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (admission.Warnings, error) {
	s3tenantAccount, ok := newObj.(*s3v1alpha1.S3TenantAccount)
	if !ok {
		return nil, fmt.Errorf("object is not an S3TenantAccount")
	}

	oldAccount, ok := oldObj.(*s3v1alpha1.S3TenantAccount)
	if !ok {
		return nil, fmt.Errorf("old object is not an S3TenantAccount")
	}

	s3tenantAccountlog.Info("validate update", "name", s3tenantAccount.Name)

	// Block import annotation on update - import is only allowed during creation
	if s3tenantAccount.Annotations != nil {
		if _, hasImport := s3tenantAccount.Annotations[s3v1alpha1.AnnotationImportTenant]; hasImport {
			// Check if annotation was just added (not present in old object)
			oldHasImport := false
			if oldAccount.Annotations != nil {
				_, oldHasImport = oldAccount.Annotations[s3v1alpha1.AnnotationImportTenant]
			}
			if !oldHasImport {
				return nil, fmt.Errorf("import annotation '%s' can only be set during creation, not on updates", s3v1alpha1.AnnotationImportTenant)
			}
		}
	}

	// Check if the TenantAccountClass exists.
	exists, err := r.tenantClassExists(ctx, s3tenantAccount.Spec.S3TenantClassName)
	if err != nil {
		return nil, err
	}

	// block creation if the TenantClass does not exist.
	if !exists {
		return nil, fmt.Errorf("TenantClass %s does not exist", s3tenantAccount.Spec.S3TenantClassName)
	}

	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type.
func (r *S3TenantAccountValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	s3tenantAccount, ok := obj.(*s3v1alpha1.S3TenantAccount)
	if !ok {
		return nil, fmt.Errorf("object is not an S3TenantAccount")
	}

	s3tenantAccountlog.Info("validate delete", "name", s3tenantAccount.Name)

	// Block import annotation on delete
	if s3tenantAccount.Annotations != nil {
		if _, hasImport := s3tenantAccount.Annotations[s3v1alpha1.AnnotationImportTenant]; hasImport {
			return nil, fmt.Errorf("cannot delete S3TenantAccount with import annotation '%s'. Remove the annotation first", s3v1alpha1.AnnotationImportTenant)
		}
	}

	// deletion is blocked until the allow-delete annotation is added.
	if _, ok := s3tenantAccount.Annotations[s3v1alpha1.AnnotationAllowTenantDeletion]; !ok {
		return nil, fmt.Errorf("deletion of s3tenant %s is blocked until annotation %s is set, please add this first", s3tenantAccount.Name, s3v1alpha1.AnnotationAllowTenantDeletion)
	}

	// make sure no tenant is still bound to this account.
	if s3tenantAccount.Status.S3TenantRef != nil {
		return nil, fmt.Errorf("deletion of s3tenantaccount %s is blocked until all tenants are unbound, please remove the reference to tenant %s/%s first", s3tenantAccount.Name, s3tenantAccount.Status.S3TenantRef.Namespace, s3tenantAccount.Status.S3TenantRef.Name)
	}

	return nil, nil
}

func (r *S3TenantAccountValidator) tenantClassExists(ctx context.Context, tenantClassName string) (bool, error) {
	// Check if the TenantClass exists.
	tenantClass := &s3v1alpha1.S3TenantClass{}

	log := log.FromContext(ctx).WithValues("func", "tenantClassExists")
	log.V(1).Info("Checking if TenantClass exists", "name", tenantClassName)

	err := r.k8sClient.Get(ctx, client.ObjectKey{Name: tenantClassName}, tenantClass)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("TenantClass does not exist", "name", tenantClassName)
			return false, nil
		}

		log.Error(err, "Failed to get TenantClass", "name", tenantClassName)
		return false, err
	}

	log.Info("TenantClass exists", "name", tenantClassName)
	return true, nil
}
