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

package grid

import (
	"context"
	"fmt"

	"github.com/bedag/storagegrid-sdk-go/models"
	"golang.org/x/exp/rand"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type Tenant = models.Tenant
type TenantUsage = models.TenantUsage

const (
	ErrTenantNotFound = "API error: 404 Not Found (code: 404)"
	adminUserName     = "admin"
	adminGroupName    = "admin"
	letterBytes       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	specialBytes      = "!@#$%&*()_+-[]{}"
	numBytes          = "0123456789"
)

func generatePassword(length int, useLetters bool, useSpecial bool, useNum bool) string {
	b := make([]byte, length)
	for i := range b {
		if useLetters {
			b[i] = letterBytes[rand.Intn(len(letterBytes))]
		} else if useSpecial {
			b[i] = specialBytes[rand.Intn(len(specialBytes))]
		} else if useNum {
			b[i] = numBytes[rand.Intn(len(numBytes))]
		}
	}
	return string(b)
}

func CreateTenant(ctx context.Context, name string, description string, quota int64, allowComplianceMode bool, maxRetentionInDays *int, gridClient *GridClient) (string, string, error) {
	log := log.FromContext(ctx).WithValues("func", "CreateTenant")
	log.V(1).Info(fmt.Sprintf("Creating tenant: name=%s, description=%s, quota=%d, allowComplianceMode=%v, maxRetentionInDays=%v", name, description, quota, allowComplianceMode, maxRetentionInDays))

	pw := generatePassword(12, true, true, true)

	allow := allowComplianceMode
	tenant := models.Tenant{
		Name:         &name,
		Description:  &description,
		Capabilities: []string{"s3", "management"},
		Policy: &models.TenantPolicy{
			QuotaObjectBytes:    &quota,
			AllowComplianceMode: &allow,
			MaxRetentionDays:    maxRetentionInDays,
		},
		Password: &pw,
	}

	createdTenant, err := gridClient.Tenant().Create(ctx, &tenant)
	if err != nil {
		log.Error(err, "Failed to create tenant")
		return "", "", err
	}

	if createdTenant.Id == "" {
		return "", "", fmt.Errorf("backend returned empty tenant ID after creation")
	}

	log.V(1).Info(fmt.Sprintf("Tenant created: id=%s", createdTenant.Id))
	return createdTenant.Id, pw, nil
}

func FetchTenant(ctx context.Context, tenantID string, gridClient *GridClient) (*Tenant, error) {
	log := log.FromContext(ctx).WithValues("func", "FetchTenant")

	tenant, err := gridClient.Tenant().GetById(ctx, tenantID)
	if err != nil {
		log.Error(err, "Failed to fetch tenant")
		return nil, err
	}

	log.V(1).Info("Tenant fetched")
	return tenant, nil
}

func CreateTenantAdminUser(ctx context.Context, tenantClient *TenantClient) (string, string, error) {
	log := log.FromContext(ctx).WithValues("func", "CreateTenantAdminUser")

	// make sure the group doesn't already exist.
	log.V(1).Info("Creating tenant admin group")
	groupID, err := createGroupIfNotExists(ctx, adminGroupName, generateAdminGroupPolicy(ctx, "admin-policy"), tenantClient)
	if err != nil {
		log.Error(err, "Failed to create tenant admin group")
		return "", "", err
	}

	log.V(1).Info("Creating tenant admin user and assigning to admin group")
	_, err = createUserIfNotExists(ctx, adminUserName, groupID, tenantClient)
	if err != nil {
		log.Error(err, "Failed to create tenant admin user")
		return "", "", err
	}
	log.V(1).Info("Tenant admin user created and assigned to admin group")

	log.V(1).Info("Setting password for tenant admin user")
	_, pw, err := SetTenantAdminPassword(ctx, tenantClient)
	if err != nil {
		log.Error(err, "Failed to set password for tenant admin user")
		return "", "", err
	}

	log.V(1).Info("Tenant admin user created successfully")
	return adminUserName, pw, nil
}

func SetTenantAdminPassword(ctx context.Context, tenantClient *TenantClient) (string, string, error) {
	log := log.FromContext(ctx).WithValues("func", "SetTenantAdminPassword")
	log.V(1).Info("Setting password for tenant admin user")

	// generate a new password.
	newPassword := generatePassword(12, true, true, true)

	// get userid by name.
	user, err := tenantClient.Users().GetByName(ctx, adminUserName)
	if err != nil {
		log.Error(err, "Failed to get tenant admin user ID")
		return "", "", err
	}

	err = tenantClient.Users().SetPassword(ctx, *user.Id, newPassword)
	if err != nil {
		log.Error(err, "Failed to set password for tenant admin user")
		return "", "", err
	}

	log.V(1).Info("Tenant admin password updated successfully")
	return adminUserName, newPassword, nil
}

func generateAdminGroupPolicy(ctx context.Context, policyName string) *models.TenantGroupPolicies {
	log := log.FromContext(ctx).WithValues("func", "generateAdminGroupPolicy")
	log.V(1).Info("Generating admin group policy")

	trueVal := true

	// allow all s3 actions.
	actions := []string{"s3:*"}

	// on all resources.
	resource := []string{"arn:aws:s3:::*"}

	return &models.TenantGroupPolicies{
		Management: &models.TenantGroupManagementPolicy{
			ManageAllContainers:       &trueVal,
			ManageEndpoints:           &trueVal,
			ManageOwnS3Credentials:    &trueVal,
			ManageOwnContainerObjects: &trueVal,
			ViewAllContainers:         &trueVal,
			RootAccess:                &trueVal,
		},
		S3: &models.S3Policy{
			ID: &policyName,
			Statement: []models.S3Statement{
				{
					Effect:   "Allow",
					Action:   &actions,
					Resource: resource,
				},
			},
		},
	}
}

// FetchTenantUsage should only ever be executed once per reconciliation.
func FetchTenantUsage(ctx context.Context, tenantID string, gridClient *GridClient) (*TenantUsage, error) {
	log := log.FromContext(ctx).WithValues("func", "FetchTenantUsage")
	log.V(1).Info(fmt.Sprintf("Fetching tenant usage for tenant with id %s", tenantID))

	tenantUsage, err := gridClient.Tenant().GetUsage(ctx, tenantID)
	if err != nil {
		log.Error(err, "Failed to fetch tenant usage")
		return nil, err
	}

	log.V(1).Info("Tenant usage fetched")
	return tenantUsage, nil
}

// Get the different values from the tenant and tenantUsage structs.

func GetTenantObjectCount(tenantUsage *TenantUsage) int64 {
	if tenantUsage == nil || tenantUsage.ObjectCount == nil {
		return 0
	}

	count := *tenantUsage.ObjectCount
	return count
}

func GetTenantUsedBytes(tenantUsage *TenantUsage) int64 {
	if tenantUsage == nil || tenantUsage.DataBytes == nil {
		return 0
	}

	bytes := *tenantUsage.DataBytes
	return bytes
}

func GetBucketCount(tenantUsage *TenantUsage) int {
	if tenantUsage == nil {
		return 0
	}

	count := len(tenantUsage.Buckets)
	return count
}

func GetConfiguredQuota(tenant *Tenant) int64 {
	return *tenant.Policy.QuotaObjectBytes
}

func GetConfiguredName(tenant *Tenant) string {
	return *tenant.Name
}

func GetConfiguredDescription(tenant *Tenant) string {
	return *tenant.Description
}

// update func to reflect user updates in the backend.

// helper method to update the tenant in the backend.
func updateTenant(ctx context.Context, tenant *Tenant, gridClient *GridClient) error {
	log := log.FromContext(ctx).WithValues("func", "updateTenant")
	log.V(1).Info("Updating tenant")

	_, err := gridClient.Tenant().Update(ctx, tenant)
	if err != nil {
		log.Error(err, "Failed to update tenant")
		return err
	}

	log.V(1).Info("Tenant updated successfully")
	return nil
}

func UpdateQuota(ctx context.Context, quota int64, tenant *Tenant, gridClient *GridClient) error {
	log := log.FromContext(ctx).WithValues("func", "UpdateQuota")
	log.V(1).Info(fmt.Sprintf("Updating quota to %d", quota))

	// only allow an increase in quota.
	// if quota < *tenant.Policy.QuotaObjectBytes {.
	// 	log.V(1).Info(fmt.printf("Quota can only be increased, %s is less than the current quota %s, failing", strconv.FormatInt(quota, 10), strconv.FormatInt(*tenant.Policy.QuotaObjectBytes, 10))).
	// 	return fmt.Errorf("quota can only be increased, %s is less than the current quota %s", strconv.FormatInt(quota, 10), strconv.FormatInt(*tenant.Policy.QuotaObjectBytes, 10)).
	// }

	tenant.Policy.QuotaObjectBytes = &quota
	return updateTenant(ctx, tenant, gridClient)
}

func UpdateDescription(ctx context.Context, description string, tenant *Tenant, gridClient *GridClient) error {
	log := log.FromContext(ctx).WithValues("func", "UpdateDescription")
	log.V(1).Info(fmt.Sprintf("Updating description to %s", description))

	tenant.Description = &description
	return updateTenant(ctx, tenant, gridClient)
}

func UpdateName(ctx context.Context, name string, tenant *Tenant, gridClient *GridClient) error {
	log := log.FromContext(ctx).WithValues("func", "UpdateName")
	log.V(1).Info(fmt.Sprintf("Updating name to %s", name))

	tenant.Name = &name
	return updateTenant(ctx, tenant, gridClient)
}

// UpdateTenantObjectLockPolicy synchronizes the tenant's S3 Object Lock policy fields
// (AllowComplianceMode and MaxRetentionDays). Because the SDK only exposes a full PUT,
// the supplied tenant must be a freshly-fetched object so all other Policy fields are
// preserved. maxRetentionInDays may be nil to clear the cap.
func UpdateTenantObjectLockPolicy(ctx context.Context, allowComplianceMode bool, maxRetentionInDays *int, tenant *Tenant, gridClient *GridClient) error {
	log := log.FromContext(ctx).WithValues("func", "UpdateTenantObjectLockPolicy")
	log.V(1).Info(fmt.Sprintf("Updating object lock policy: allowComplianceMode=%v, maxRetentionInDays=%v", allowComplianceMode, maxRetentionInDays))

	if tenant.Policy == nil {
		tenant.Policy = &models.TenantPolicy{}
	}
	allow := allowComplianceMode
	tenant.Policy.AllowComplianceMode = &allow
	tenant.Policy.MaxRetentionDays = maxRetentionInDays
	return updateTenant(ctx, tenant, gridClient)
}

// GetConfiguredAllowComplianceMode returns whether the tenant currently allows compliance mode.
// A nil pointer is treated as false (server default).
func GetConfiguredAllowComplianceMode(tenant *Tenant) bool {
	if tenant == nil || tenant.Policy == nil || tenant.Policy.AllowComplianceMode == nil {
		return false
	}
	return *tenant.Policy.AllowComplianceMode
}

// GetConfiguredMaxRetentionDays returns the tenant's configured maximum retention in days.
// A nil pointer means no cap is enforced server-side.
func GetConfiguredMaxRetentionDays(tenant *Tenant) *int {
	if tenant == nil || tenant.Policy == nil {
		return nil
	}
	return tenant.Policy.MaxRetentionDays
}

// delete tenant in the backend.
func DeleteTenant(ctx context.Context, tenantId string, gridClient *GridClient) error {
	log := log.FromContext(ctx).WithValues("func", "DeleteTenant")
	log.V(1).Info(fmt.Sprintf("Deleting tenant with id %s", tenantId))

	// check if the tenant still exists on the backend.
	// we need to do this, because otherwise the deletion will fail.
	_, err := gridClient.Tenant().GetById(ctx, tenantId)
	if err != nil {
		log.Error(err, "Unable to fetch tenant, it might have been deleted already, removing from kubernetes")
		return nil
	}

	return gridClient.Tenant().Delete(ctx, tenantId)
}
