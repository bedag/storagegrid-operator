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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var s3bucketlog = logf.Log.WithName("s3bucket-webhook")

type S3BucketValidator struct {
	// +kubebuilder:object:generate=false
	k8sClient client.Client `json:"-"`
	// Scheme    *runtime.Scheme `json:"-"`.
}

// SetupWebhookWithManager will setup the manager to manage the webhooks.
func (r *S3BucketValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	// nonWatchClient, err := client.New(mgr.GetConfig(), client.Options{}).
	// if err != nil {.
	// 	return fmt.Errorf("failed to create non watch client: %v", err).
	// }

	// validator := &S3BucketValidator{.
	// 	Client: nonWatchClient,.
	// }

	r.k8sClient = mgr.GetClient()
	// r.Scheme = mgr.GetScheme().

	return ctrl.NewWebhookManagedBy(mgr).
		For(&s3v1alpha1.S3Bucket{}).
		WithValidator(r).
		Complete()
}

// change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-s3-bedag-ch-v1alpha1-s3bucket,mutating=false,failurePolicy=fail,sideEffects=None,groups=s3.bedag.ch,resources=s3buckets,verbs=create;update;delete,versions=v1alpha1,name=vs3bucket.kb.io,admissionReviewVersions=v1

var _ webhook.CustomValidator = &S3BucketValidator{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type.
func (r *S3BucketValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	s3bucket, ok := obj.(*s3v1alpha1.S3Bucket)
	if !ok {
		return nil, fmt.Errorf("object is not an S3Bucket")
	}

	s3bucketlog.Info("validate create", "name", s3bucket.Name)

	// check if the namespace is allowed to create buckets in the tenant.
	s3Teant, err := r.getTenant(ctx, s3bucket)
	if err != nil {
		return nil, err
	}

	allowed := r.isNamespaceAllowed(s3Teant, s3bucket.Namespace)

	if !allowed {
		return nil, fmt.Errorf("namespace %s is not allowed to create buckets in tenant %s", s3bucket.Namespace, s3Teant.Name)
	}

	if err := r.validateBucketObjectLock(ctx, s3bucket, s3Teant); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type.
func (r *S3BucketValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	s3bucketNew, ok := newObj.(*s3v1alpha1.S3Bucket)

	if !ok {
		return nil, fmt.Errorf("object is not an S3Bucket")
	}

	s3bucketOld, ok := oldObj.(*s3v1alpha1.S3Bucket)

	if !ok {
		return nil, fmt.Errorf("object is not an S3Bucket")
	}

	s3bucketlog.Info("validate update", "name", s3bucketNew.Name)

	s3Teant, err := r.getTenant(ctx, s3bucketNew)
	if err != nil {
		return nil, err
	}

	allowed := r.isNamespaceAllowed(s3Teant, s3bucketNew.Namespace)

	if !allowed {
		return nil, fmt.Errorf("namespace %s is not allowed to create buckets in tenant %s", s3bucketNew.Namespace, s3Teant.Name)
	}

	// spec.bucketName is immutable once the bucket has been successfully created on the backend.
	// Gate on ConditionTypeCreated=True rather than phase, so the user can still fix the name
	// when creation failed (e.g. BucketNameConflict) while phase is Pending.
	createdCondition := meta.FindStatusCondition(s3bucketOld.Status.Conditions, s3v1alpha1.ConditionTypeCreated)
	if createdCondition != nil && createdCondition.Status == metav1.ConditionTrue {
		if s3bucketNew.Spec.BucketName != nil && *s3bucketNew.Spec.BucketName != s3bucketOld.Status.BucketName {
			return nil, fmt.Errorf("spec.bucketName cannot be changed after the bucket has been created")
		}
	}

	if err := r.validateBucketObjectLock(ctx, s3bucketNew, s3Teant); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type.
func (r *S3BucketValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	s3bucket, ok := obj.(*s3v1alpha1.S3Bucket)
	if !ok {
		return nil, fmt.Errorf("object is not an S3Bucket")
	}

	s3bucketlog.Info("validate delete", "name", s3bucket.Name)

	// Check if bucket has objects and provide helpful guidance
	if s3bucket.Status.BucketUsage.ObjectCount > 0 {
		warning := fmt.Sprintf(
			"Bucket contains %d objects. "+
				"Deletion will be blocked by finalizer until bucket is empty. "+
				"To drain objects automatically, add annotation: kubectl annotate s3bucket %s %s=true",
			s3bucket.Status.BucketUsage.ObjectCount,
			s3bucket.Name,
			s3v1alpha1.AnnotationDrainBucket,
		)
		return admission.Warnings{warning}, nil
	}

	return nil, nil
}

func (r *S3BucketValidator) getTenant(ctx context.Context, s3Bucket *s3v1alpha1.S3Bucket) (*s3v1alpha1.S3Tenant, error) {
	s3Tenant := &s3v1alpha1.S3Tenant{}

	namespaceToLookup := s3Bucket.Spec.S3TenantRef.Namespace
	if namespaceToLookup == "" {
		namespaceToLookup = s3Bucket.Namespace
	}

	if err := r.k8sClient.Get(ctx, client.ObjectKey{Name: s3Bucket.Spec.S3TenantRef.Name, Namespace: namespaceToLookup}, s3Tenant); err != nil {
		s3bucketlog.Info("Unable to retrieve tenant, failing!")
		return nil, err
	}
	s3bucketlog.Info("Successfully retrieved tenant")

	return s3Tenant, nil
}

func (r *S3BucketValidator) isNamespaceAllowed(s3tenant *s3v1alpha1.S3Tenant, bucketNamespace string) bool {
	// if no allowed namespaces are specified ony the namespace of the tenant is allowed.
	if len(s3tenant.Spec.AllowedNamespaces) == 0 {
		return s3tenant.Namespace == bucketNamespace
	}

	for _, pattern := range s3tenant.Spec.AllowedNamespaces {
		if matchesWildcard(bucketNamespace, pattern) {
			return true
		}
	}

	return false
}

func matchesWildcard(bucketNamespace string, pattern string) bool {
	if pattern == "*" {
		return true
	}

	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		if strings.HasPrefix(bucketNamespace, prefix) {
			return true
		}
	}

	return false
}

// validateBucketObjectLock enforces grid-availability, tenant-mode-ceiling and retention-cap rules
// for S3Bucket spec.s3ObjectLock. Called from both ValidateCreate and ValidateUpdate.
func (r *S3BucketValidator) validateBucketObjectLock(ctx context.Context, bucket *s3v1alpha1.S3Bucket, tenant *s3v1alpha1.S3Tenant) error {
	bucketMode := effectiveBucketObjectLockMode(bucket.Spec.S3ObjectLock)
	if bucketMode == s3v1alpha1.S3ObjectLockModeDisabled {
		return nil
	}

	// Required: retentionInDays > 0 when mode != Disabled.
	if bucket.Spec.S3ObjectLock == nil || bucket.Spec.S3ObjectLock.RetentionInDays <= 0 {
		return fmt.Errorf("spec.s3ObjectLock.retentionInDays must be greater than 0 when mode is %s", bucketMode)
	}

	// Grid must support S3 Object Lock.
	gridName := tenant.Spec.StorageGridRef.Name
	available, err := gridObjectLockAvailable(ctx, r.k8sClient, gridName)
	if err != nil {
		return err
	}
	if !available {
		return fmt.Errorf("StorageGrid %s does not have S3 Object Lock enabled grid-wide", gridName)
	}

	// Tenant mode acts as ceiling.
	tenantMode := effectiveTenantObjectLockMode(tenant.Spec.S3ObjectLock)
	if objectLockModeRank(bucketMode) > objectLockModeRank(tenantMode) {
		return fmt.Errorf("spec.s3ObjectLock.mode=%s is not allowed: parent tenant %s/%s permits at most %s", bucketMode, tenant.Namespace, tenant.Name, tenantMode)
	}

	// Tenant maxRetentionInDays caps the bucket retention.
	if tenant.Spec.S3ObjectLock != nil && tenant.Spec.S3ObjectLock.MaxRetentionInDays > 0 {
		if bucket.Spec.S3ObjectLock.RetentionInDays > tenant.Spec.S3ObjectLock.MaxRetentionInDays {
			return fmt.Errorf("spec.s3ObjectLock.retentionInDays=%d exceeds parent tenant %s/%s maxRetentionInDays=%d",
				bucket.Spec.S3ObjectLock.RetentionInDays, tenant.Namespace, tenant.Name, tenant.Spec.S3ObjectLock.MaxRetentionInDays)
		}
	}

	return nil
}
