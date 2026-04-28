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
	"sigs.k8s.io/controller-runtime/pkg/client"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

// effectiveBucketObjectLockMode returns the configured bucket object-lock mode, defaulting to Disabled.
func effectiveBucketObjectLockMode(spec *s3v1alpha1.S3ObjectLockBucketSpec) s3v1alpha1.S3ObjectLockMode {
	if spec == nil || spec.Mode == "" {
		return s3v1alpha1.S3ObjectLockModeDisabled
	}
	return spec.Mode
}

// effectiveTenantObjectLockMode returns the configured tenant object-lock mode, defaulting to Disabled.
func effectiveTenantObjectLockMode(spec *s3v1alpha1.S3ObjectLockTenantSpec) s3v1alpha1.S3ObjectLockMode {
	if spec == nil || spec.Mode == "" {
		return s3v1alpha1.S3ObjectLockModeDisabled
	}
	return spec.Mode
}

// objectLockModeRank returns the ordering rank of an object-lock mode (Disabled < Governance < Compliance).
func objectLockModeRank(mode s3v1alpha1.S3ObjectLockMode) int {
	switch mode {
	case s3v1alpha1.S3ObjectLockModeCompliance:
		return 2
	case s3v1alpha1.S3ObjectLockModeGovernance:
		return 1
	default:
		return 0
	}
}

// gridObjectLockAvailable reports whether the named StorageGrid currently has S3 Object Lock enabled.
// A nil/unreported status flag is treated as false.
func gridObjectLockAvailable(ctx context.Context, c client.Client, storageGridName string) (bool, error) {
	if storageGridName == "" {
		return false, fmt.Errorf("storageGridRef.name is empty")
	}
	sg := &s3v1alpha1.StorageGrid{}
	if err := c.Get(ctx, client.ObjectKey{Name: storageGridName}, sg); err != nil {
		if apierrors.IsNotFound(err) {
			return false, fmt.Errorf("StorageGrid %s not found", storageGridName)
		}
		return false, fmt.Errorf("failed to get StorageGrid %s: %w", storageGridName, err)
	}
	if sg.Status.S3ObjectLockAvailable == nil {
		return false, nil
	}
	return *sg.Status.S3ObjectLockAvailable, nil
}

// listBucketsForTenant returns all S3Buckets across the cluster that reference the given tenant by name.
// The tenant ref namespace is honoured when set, otherwise the tenant's own namespace is used.
func listBucketsForTenant(ctx context.Context, c client.Client, tenantName, tenantNamespace string) ([]s3v1alpha1.S3Bucket, error) {
	bucketList := &s3v1alpha1.S3BucketList{}
	if err := c.List(ctx, bucketList); err != nil {
		return nil, fmt.Errorf("failed to list S3Buckets: %w", err)
	}
	var matched []s3v1alpha1.S3Bucket
	for _, b := range bucketList.Items {
		if b.Spec.S3TenantRef.Name != tenantName {
			continue
		}
		ns := b.Spec.S3TenantRef.Namespace
		if ns == "" {
			ns = b.Namespace
		}
		if ns == tenantNamespace {
			matched = append(matched, b)
		}
	}
	return matched, nil
}

// validateTenantObjectLockTransition enforces that lowering the tenant mode or maxRetentionInDays
// is only allowed when no owned bucket would be left in violation.
func validateTenantObjectLockTransition(ctx context.Context, c client.Client, tenantName, tenantNamespace string, oldSpec, newSpec *s3v1alpha1.S3ObjectLockTenantSpec) error {
	oldMode := effectiveTenantObjectLockMode(oldSpec)
	newMode := effectiveTenantObjectLockMode(newSpec)

	var newMax int32
	if newSpec != nil {
		newMax = newSpec.MaxRetentionInDays
	}
	var oldMax int32
	if oldSpec != nil {
		oldMax = oldSpec.MaxRetentionInDays
	}

	modeLowered := objectLockModeRank(newMode) < objectLockModeRank(oldMode)
	maxLowered := newMode != s3v1alpha1.S3ObjectLockModeDisabled && newMax > 0 && newMax < oldMax

	if !modeLowered && !maxLowered {
		return nil
	}

	// if the mode was lowered, or maxRetentionInDays was reduced, then check all buckets for compliance with the new tenant-level ceiling.
	buckets, err := listBucketsForTenant(ctx, c, tenantName, tenantNamespace)
	if err != nil {
		return err
	}

	for _, b := range buckets {
		bMode := effectiveBucketObjectLockMode(b.Spec.S3ObjectLock)
		if modeLowered && objectLockModeRank(bMode) > objectLockModeRank(newMode) {
			return fmt.Errorf("cannot lower tenant s3ObjectLock.mode to %s: bucket %s/%s currently uses mode %s",
				newMode, b.Namespace, b.Name, bMode)
		}
		if maxLowered && b.Spec.S3ObjectLock != nil && b.Spec.S3ObjectLock.RetentionInDays > newMax {
			return fmt.Errorf("cannot reduce tenant s3ObjectLock.maxRetentionInDays to %d: bucket %s/%s currently uses retentionInDays=%d",
				newMax, b.Namespace, b.Name, b.Spec.S3ObjectLock.RetentionInDays)
		}
	}
	return nil
}
