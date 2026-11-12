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

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

func boolPtr(b bool) *bool { return &b }

func TestObjectLockToSDK(t *testing.T) {
	tests := []struct {
		name     string
		spec     *s3v1alpha1.S3ObjectLockBucketSpec
		wantNil  bool
		wantJSON string
	}{
		{name: "nil spec", spec: nil, wantNil: true},
		{name: "disabled", spec: &s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeDisabled, RetentionInDays: 30}, wantNil: true},
		{
			name:     "governance with retention",
			spec:     &s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeGovernance, RetentionInDays: 30},
			wantJSON: `{"enabled":true,"defaultRetentionSetting":{"mode":"governance","days":30}}`,
		},
		{
			name:     "governance without default retention",
			spec:     &s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeGovernance, RetentionInDays: s3v1alpha1.S3ObjectLockRetentionNoDefault},
			wantJSON: `{"enabled":true}`,
		},
		{
			name:     "compliance without default retention",
			spec:     &s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeCompliance, RetentionInDays: s3v1alpha1.S3ObjectLockRetentionNoDefault},
			wantJSON: `{"enabled":true}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := objectLockToSDK(tt.spec)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("expected nil, got %+v", got)
				}
				return
			}
			raw, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if strings.TrimSpace(string(raw)) != tt.wantJSON {
				t.Fatalf("request body = %s, want %s", raw, tt.wantJSON)
			}
		})
	}
}

// A bucket that already has Object Lock without default retention must not be updated again,
// while one that still carries a default retention must be updated when the spec moves to -1.
func TestDesiredBucketObjectLockNoDefaultRetentionDrift(t *testing.T) {
	spec := &s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeGovernance, RetentionInDays: s3v1alpha1.S3ObjectLockRetentionNoDefault}

	inSync := &models.BucketS3ObjectLockSettings{Enabled: boolPtr(true)}
	if desired := DesiredBucketObjectLock(spec, inSync); !ObjectLockSettingsEqual(inSync, desired) {
		t.Fatalf("expected no drift for bucket without default retention, desired=%+v", desired)
	}

	withRetention := &models.BucketS3ObjectLockSettings{
		Enabled: boolPtr(true),
		DefaultRetentionSetting: &models.BucketS3ObjectLockDefaultRetentionSettings{
			Mode: "governance",
			Days: json.Number("30"),
		},
	}
	desired := DesiredBucketObjectLock(spec, withRetention)
	if ObjectLockSettingsEqual(withRetention, desired) {
		t.Fatal("expected drift when the bucket still has a default retention")
	}
	if desired.Enabled == nil || !*desired.Enabled || desired.DefaultRetentionSetting != nil {
		t.Fatalf("desired = %+v, want enabled with no default retention", desired)
	}

	notLocked := &models.BucketS3ObjectLockSettings{Enabled: boolPtr(false)}
	if ObjectLockSettingsEqual(notLocked, DesiredBucketObjectLock(spec, notLocked)) {
		t.Fatal("expected drift when Object Lock is not enabled on the bucket")
	}
}
