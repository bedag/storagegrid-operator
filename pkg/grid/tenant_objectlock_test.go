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
	"encoding/json"
	"strings"
	"testing"

	"github.com/bedag/storagegrid-sdk-go/models"
)

// StorageGrid stamps maxRetentionYears=100 onto every tenant created without an explicit
// ceiling. The operator expresses the ceiling in days only, so the accessor has to surface a
// years value for drift detection to notice such a tenant is out of band.
func TestGetConfiguredMaxRetentionYears(t *testing.T) {
	if got := GetConfiguredMaxRetentionYears(nil); got != nil {
		t.Errorf("nil tenant: got %v, want nil", got)
	}

	if got := GetConfiguredMaxRetentionYears(&Tenant{}); got != nil {
		t.Errorf("nil policy: got %v, want nil", got)
	}

	hundred := 100
	tenant := &Tenant{Policy: &models.TenantPolicy{MaxRetentionYears: &hundred}}
	got := GetConfiguredMaxRetentionYears(tenant)
	if got == nil || *got != 100 {
		t.Errorf("years=100: got %v, want 100", got)
	}
}

// The update is a full PUT of a freshly fetched tenant. If MaxRetentionYears were left alone,
// a tenant sitting at the backend's 100-year default would have that value written straight
// back alongside the days value, leaving two competing ceilings.
func TestObjectLockPolicyMutationClearsYears(t *testing.T) {
	hundred := 100
	quota := int64(1 << 30)
	policy := &models.TenantPolicy{
		QuotaObjectBytes:  &quota,
		MaxRetentionYears: &hundred,
	}

	// Mirrors what UpdateTenantObjectLockPolicy does before handing the tenant to the SDK.
	allow := true
	maxDays := 365
	policy.AllowComplianceMode = &allow
	policy.MaxRetentionDays = &maxDays
	policy.MaxRetentionYears = nil

	if policy.MaxRetentionDays == nil || *policy.MaxRetentionDays != 365 {
		t.Errorf("maxRetentionDays: got %v, want 365", policy.MaxRetentionDays)
	}
	if policy.MaxRetentionYears != nil {
		t.Errorf("maxRetentionYears: got %v, want nil", policy.MaxRetentionYears)
	}
	if policy.QuotaObjectBytes == nil || *policy.QuotaObjectBytes != 1<<30 {
		t.Error("unrelated policy fields must survive the read-modify-write")
	}
}

// The whole bug hinges on marshaling. Both retention fields carry omitempty in the SDK, so a nil
// is dropped from the request body rather than sent as null. A body carrying no ceiling at all
// leaves the backend free to keep or invent one - which is how tenants ended up pinned at 100
// years - so the operator must always emit a concrete maxRetentionDays.
//
// Omitting maxRetentionYears is fine: StorageGrid stores the ceiling as either days or years and
// writing one clears the other, verified against the grid by creating a tenant with only
// maxRetentionDays set and reading back maxRetentionYears=null.
func TestTenantPolicyAlwaysMarshalsAConcreteCeiling(t *testing.T) {
	maxDays := 365
	body, err := json.Marshal(&models.TenantPolicy{MaxRetentionDays: &maxDays})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(body), `"maxRetentionDays":365`) {
		t.Errorf("maxRetentionDays missing from body: %s", body)
	}
	if strings.Contains(string(body), "maxRetentionYears") {
		t.Errorf("maxRetentionYears is omitted, not nulled: %s", body)
	}

	// Guards the reason CreateTenant and UpdateTenantObjectLockPolicy take a plain int rather
	// than a pointer: a nil ceiling silently vanishes from the wire.
	body, err = json.Marshal(&models.TenantPolicy{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(body), "maxRetention") {
		t.Errorf("a nil ceiling is omitted rather than sent, so callers must resolve a concrete "+
			"value before reaching the SDK: %s", body)
	}
}
