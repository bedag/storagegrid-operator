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
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var s3tenantlog = log.Log.WithName("s3tenant-webhook")

type S3TenantValidator struct {
	// +kubebuilder:object:generate=false
	k8sClient client.Client `json:"-"`
	// Scheme    *runtime.Scheme `json:"-"`.
}

// SetupWebhookWithManager will setup the manager to manage the webhooks.
func (r *S3TenantValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	r.k8sClient = mgr.GetClient()

	return ctrl.NewWebhookManagedBy(mgr).
		For(&s3v1alpha1.S3Tenant{}).
		WithValidator(r).
		WithDefaulter(&S3TenantDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-s3-bedag-ch-v1alpha1-s3tenant,mutating=true,failurePolicy=fail,sideEffects=None,groups=s3.bedag.ch,resources=s3tenants,verbs=create;update,versions=v1alpha1,name=ms3tenant.kb.io,admissionReviewVersions=v1

type S3TenantDefaulter struct{}

// Default implements webhook.Defaulter so a webhook will be registered for the type.
func (r *S3TenantDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	s3tenant, ok := obj.(*s3v1alpha1.S3Tenant)
	if !ok {
		return fmt.Errorf("object is not an S3Tenant")
	}

	s3tenantlog.Info("running defaulter", "name", s3tenant.Name)

	// default tenantname if not set.
	if s3tenant.Spec.Name == "" {
		// use the metadata.name as tenant name.
		s3tenant.Spec.Name = s3tenant.Name
	}

	return nil
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-s3-bedag-ch-v1alpha1-s3tenant,mutating=false,failurePolicy=fail,sideEffects=None,groups=s3.bedag.ch,resources=s3tenants,verbs=create;update;delete,versions=v1alpha1,name=vs3tenant.kb.io,admissionReviewVersions=v1

var _ webhook.CustomValidator = &S3TenantValidator{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type.
func (r *S3TenantValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	s3tenant, ok := obj.(*s3v1alpha1.S3Tenant)
	if !ok {
		return nil, fmt.Errorf("object is not an S3Tenant")
	}

	s3tenantlog.Info("validate create", "name", s3tenant.Name)

	// Check if the TenantClass exists.
	exists, err := r.tenantClassExists(ctx, s3tenant.Spec.S3TenantClassName)
	if err != nil {
		return nil, err
	}

	// block creation if the TenantClass does not exist.
	if !exists {
		return nil, fmt.Errorf("TenantClass %s does not exist", s3tenant.Spec.S3TenantClassName)
	}

	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type.
func (r *S3TenantValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (admission.Warnings, error) {
	s3tenant, ok := newObj.(*s3v1alpha1.S3Tenant)
	if !ok {
		return nil, fmt.Errorf("object is not an S3Tenant")
	}

	// oldTenant, ok := oldObj.(*s3v1alpha1.S3Tenant)
	// if !ok {
	// 	return nil, fmt.Errorf("old object is not an S3Tenant")
	// }

	s3tenantlog.Info("validate update", "name", s3tenant.Name)

	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type.
func (r *S3TenantValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	s3tenant, ok := obj.(*s3v1alpha1.S3Tenant)
	if !ok {
		return nil, fmt.Errorf("object is not an S3Tenant")
	}

	s3tenantlog.Info("validate delete", "name", s3tenant.Name)

	// deletion is blocked until the allow-delete annotation is added.
	if _, ok := s3tenant.Annotations[s3v1alpha1.AnnotationAllowTenantDeletion]; ok {
		return nil, nil
	}

	return nil, fmt.Errorf("deletion of s3tenant %s is blocked until annotation %s is set, please add this first", s3tenant.Name, s3v1alpha1.AnnotationAllowTenantDeletion)
}

func (r *S3TenantValidator) tenantClassExists(ctx context.Context, tenantClassName string) (bool, error) {
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
