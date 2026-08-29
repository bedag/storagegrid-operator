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
	"testing"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

func gridWithDefault(days int32) *s3v1alpha1.StorageGrid {
	return &s3v1alpha1.StorageGrid{
		Spec: s3v1alpha1.StorageGridSpec{DefaultMaxRetentionInDays: days},
	}
}

// StorageGrid rejects an empty S3 Object Lock ceiling and silently substitutes 100 years when a
// request carries none, so this resolution must never yield zero no matter how sparse the input.
func TestEffectiveMaxRetentionInDays(t *testing.T) {
	tests := []struct {
		name string
		spec *s3v1alpha1.S3ObjectLockTenantSpec
		sg   *s3v1alpha1.StorageGrid
		want int32
	}{
		{
			name: "tenant ceiling wins over the grid default",
			spec: &s3v1alpha1.S3ObjectLockTenantSpec{Mode: s3v1alpha1.S3ObjectLockModeGovernance, MaxRetentionInDays: 90},
			sg:   gridWithDefault(365),
			want: 90,
		},
		{
			name: "zero is the inherit sentinel",
			spec: &s3v1alpha1.S3ObjectLockTenantSpec{Mode: s3v1alpha1.S3ObjectLockModeDisabled, MaxRetentionInDays: 0},
			sg:   gridWithDefault(365),
			want: 365,
		},
		{
			name: "absent object lock config inherits the grid default",
			spec: nil,
			sg:   gridWithDefault(730),
			want: 730,
		},
		{
			// The ceiling also caps Governance retention requested straight over the S3 API,
			// which the S3Bucket mode gate never sees, so mode must not suppress it.
			name: "a ceiling applies even when the mode is Disabled",
			spec: &s3v1alpha1.S3ObjectLockTenantSpec{Mode: s3v1alpha1.S3ObjectLockModeDisabled, MaxRetentionInDays: 30},
			sg:   gridWithDefault(365),
			want: 30,
		},
		{
			name: "an unconfigured grid falls back to one year",
			spec: nil,
			sg:   gridWithDefault(0),
			want: s3v1alpha1.DefaultMaxRetentionInDays,
		},
		{
			name: "a nil grid still resolves",
			spec: &s3v1alpha1.S3ObjectLockTenantSpec{},
			sg:   nil,
			want: s3v1alpha1.DefaultMaxRetentionInDays,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EffectiveMaxRetentionInDays(tt.spec, tt.sg)
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
			if got <= 0 {
				t.Errorf("resolution must never yield a non-positive ceiling, got %d", got)
			}
			if d := desiredMaxRetentionDays(tt.spec, tt.sg); d != int(tt.want) {
				t.Errorf("desiredMaxRetentionDays: got %d, want %d", d, tt.want)
			}
		})
	}
}
