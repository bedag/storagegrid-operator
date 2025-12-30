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

	"sigs.k8s.io/controller-runtime/pkg/client"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

// ValidateAccountClaim validates whether an S3Tenant can claim an S3TenantAccount.
// This function is shared between the webhook and controller to ensure consistent validation.
//
// Validation rules:
// 1. Account must not be bound to a different tenant (name+namespace check)
// 2. If account has pre-binding (spec.s3TenantRef), it must match the claiming tenant
// 3. Both must reference the same StorageGrid
// 4. Tenant quota must be >= account quota (minimum requirement)
// 5. TenantClass must match exactly
func ValidateAccountClaim(ctx context.Context, k8sClient client.Client, tenant *s3v1alpha1.S3Tenant, account *s3v1alpha1.S3TenantAccount) error {
	// Rule 1: Check if account is already bound to a different tenant
	if account.Status.S3TenantRef != nil {
		// Check if it's bound to a different tenant (using name+namespace for stability)
		if account.Status.S3TenantRef.Name != tenant.Name ||
			account.Status.S3TenantRef.Namespace != tenant.Namespace {
			return fmt.Errorf("account %s is already bound to tenant %s/%s",
				account.Name, account.Status.S3TenantRef.Namespace, account.Status.S3TenantRef.Name)
		}
	}

	// Rule 2: Check pre-binding match (if account has spec.s3TenantRef set)
	if account.Spec.S3TenantRef != nil {
		// Account is pre-bound to a specific tenant, must match
		if account.Spec.S3TenantRef.Name != tenant.Name ||
			account.Spec.S3TenantRef.Namespace != tenant.Namespace {
			return fmt.Errorf("account %s is pre-bound to tenant %s/%s, cannot be claimed by %s/%s",
				account.Name,
				account.Spec.S3TenantRef.Namespace, account.Spec.S3TenantRef.Name,
				tenant.Namespace, tenant.Name)
		}
	}

	// Rule 3: Both must reference the same StorageGrid
	if tenant.Spec.StorageGridRef.Name != account.Spec.StorageGridRef.Name {
		return fmt.Errorf("tenant references StorageGrid %s but account references %s",
			tenant.Spec.StorageGridRef.Name, account.Spec.StorageGridRef.Name)
	}

	// Rule 4: Tenant quota must be >= account quota (minimum requirement)
	// If tenant has no quota limit (nil), it's unlimited and can claim any account
	if tenant.Spec.StorageQuota != nil {
		tenantQuota := tenant.Spec.StorageQuota.Value()

		// If account has a quota, tenant quota must be at least as large
		if account.Spec.StorageQuota != nil {
			accountQuota := account.Spec.StorageQuota.Value()
			if tenantQuota < accountQuota {
				return fmt.Errorf("tenant quota (%s) is less than account quota (%s)",
					tenant.Spec.StorageQuota.String(), account.Spec.StorageQuota.String())
			}
		}
	}

	// Rule 5: TenantClass must match exactly
	if tenant.Spec.S3TenantClassName != account.Spec.S3TenantClassName {
		return fmt.Errorf("tenant references TenantClass %s but account references %s",
			tenant.Spec.S3TenantClassName, account.Spec.S3TenantClassName)
	}

	return nil
}
