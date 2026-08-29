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
	"strings"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	"github.com/bedag/storagegrid-operator/internal/controller"
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

	// If claiming an existing account, validate the claim
	if s3tenant.Spec.S3TenantAccountRef != nil {
		// Fetch the account to validate the claim
		account := &s3v1alpha1.S3TenantAccount{}
		err := r.k8sClient.Get(ctx, client.ObjectKey{Name: s3tenant.Spec.S3TenantAccountRef.Name}, account)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("S3TenantAccount %s does not exist", s3tenant.Spec.S3TenantAccountRef.Name)
			}
			return nil, fmt.Errorf("failed to get S3TenantAccount %s: %w", s3tenant.Spec.S3TenantAccountRef.Name, err)
		}

		// Use shared validation function
		if err := controller.ValidateAccountClaim(ctx, r.k8sClient, s3tenant, account); err != nil {
			return nil, fmt.Errorf("account claim validation failed: %w", err)
		}
	}

	if err := r.validateObjectLockGridAvailability(ctx, s3tenant.Spec.StorageGridRef.Name, s3tenant.Spec.S3ObjectLock); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type.
func (r *S3TenantValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (admission.Warnings, error) {
	s3tenant, ok := newObj.(*s3v1alpha1.S3Tenant)
	if !ok {
		return nil, fmt.Errorf("object is not an S3Tenant")
	}

	oldS3Tenant, ok := oldObj.(*s3v1alpha1.S3Tenant)
	if !ok {
		return nil, fmt.Errorf("old object is not an S3Tenant")
	}

	s3tenantlog.Info("validate update", "name", s3tenant.Name)

	if err := r.validateObjectLockGridAvailability(ctx, s3tenant.Spec.StorageGridRef.Name, s3tenant.Spec.S3ObjectLock); err != nil {
		return nil, err
	}

	if err := validateTenantObjectLockTransition(ctx, r.k8sClient, s3tenant.Name, s3tenant.Namespace, oldS3Tenant.Spec.S3ObjectLock, s3tenant.Spec.S3ObjectLock); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type.
func (r *S3TenantValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	s3tenant, ok := obj.(*s3v1alpha1.S3Tenant)
	if !ok {
		return nil, fmt.Errorf("object is not an S3Tenant")
	}

	s3tenantlog.Info("validate delete", "name", s3tenant.Name)

	// Opt-in deletion protection: block deletion only if the protection annotation is explicitly set.
	if val, ok := s3tenant.Annotations[s3v1alpha1.AnnotationDeletionProtection]; ok && val == "true" {
		return nil, fmt.Errorf("deletion of S3Tenant %s is protected by annotation %s=true. To remove protection run: kubectl annotate s3tenant %s %s- -n %s",
			s3tenant.Name, s3v1alpha1.AnnotationDeletionProtection, s3tenant.Name, s3v1alpha1.AnnotationDeletionProtection, s3tenant.Namespace)
	}

	// Warn (but do not block) if S3Buckets still reference this tenant.
	return r.warnOnLinkedBuckets(ctx, s3tenant), nil
}

// warnOnLinkedBuckets returns a warning if any S3Buckets still reference this tenant.
// Uses the field indexer registered by the S3Tenant controller for efficient lookup.
//
// This deliberately warns instead of denying the DELETE. Denying it would leave the
// namespace controller retrying a call that admission keeps rejecting, so a
// `kubectl delete namespace` holding such a tenant would hang in Terminating forever
// with no finalizer to force-remove. The actual protection lives in the finalizer:
// S3TenantReconciler.finalize refuses to complete while linked buckets exist, so the
// tenant is accepted for deletion and then held in the Deleting phase until it is safe.
func (r *S3TenantValidator) warnOnLinkedBuckets(ctx context.Context, s3tenant *s3v1alpha1.S3Tenant) admission.Warnings {
	buckets := &s3v1alpha1.S3BucketList{}
	if err := r.k8sClient.List(ctx, buckets,
		client.MatchingFields{"spec.s3TenantRef.namespacedName": s3tenant.Namespace + "/" + s3tenant.Name}); err != nil {
		s3tenantlog.Error(err, "Unable to list S3Buckets referencing tenant", "tenant", s3tenant.Name)
		return nil
	}

	if len(buckets.Items) == 0 {
		return nil
	}

	linked := make([]string, 0, len(buckets.Items))
	for i := range buckets.Items {
		b := &buckets.Items[i]
		linked = append(linked, fmt.Sprintf("%s/%s", b.Namespace, b.Name))
	}

	return admission.Warnings{fmt.Sprintf(
		"S3Tenant %s/%s is still referenced by %d S3Bucket(s): [%s]. "+
			"Deletion is accepted but the tenant will remain in the Deleting phase until those buckets are gone.",
		s3tenant.Namespace, s3tenant.Name, len(linked), strings.Join(linked, ", "))}
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

// validateObjectLockGridAvailability rejects mode=Compliance when the parent grid does not have S3 Object Lock enabled.
// Other modes (Disabled, Governance) are unconditionally allowed at this layer; the bucket webhook re-validates per-bucket.
func (r *S3TenantValidator) validateObjectLockGridAvailability(ctx context.Context, storageGridName string, spec *s3v1alpha1.S3ObjectLockTenantSpec) error {
	if effectiveTenantObjectLockMode(spec) != s3v1alpha1.S3ObjectLockModeCompliance {
		return nil
	}
	available, err := gridObjectLockAvailable(ctx, r.k8sClient, storageGridName)
	if err != nil {
		return err
	}
	if !available {
		return fmt.Errorf("spec.s3ObjectLock.mode=Compliance requires StorageGrid %s to have S3 Object Lock enabled grid-wide", storageGridName)
	}
	return nil
}
