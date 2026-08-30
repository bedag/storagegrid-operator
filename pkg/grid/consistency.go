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

	"sigs.k8s.io/controller-runtime/pkg/log"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"

	models "github.com/bedag/storagegrid-sdk-go/models"
)

// Consistency is the StorageGRID wire value of a bucket consistency setting, as accepted and
// returned by GET/PUT /org/containers/{name}/consistency.
type Consistency string

const (
	// ConsistencyAll requires all nodes to receive the data immediately, or the request fails.
	ConsistencyAll Consistency = "all"
	// ConsistencyStrongGlobal guarantees read-after-write for all client requests across all sites.
	ConsistencyStrongGlobal Consistency = "strong-global"
	// ConsistencyStrongSite guarantees read-after-write for all client requests within a site.
	ConsistencyStrongSite Consistency = "strong-site"
	// ConsistencyReadAfterNewWrite guarantees read-after-write for new objects and eventual
	// consistency for object updates.
	ConsistencyReadAfterNewWrite Consistency = "read-after-new-write"
	// ConsistencyAvailable provides eventual consistency for both new objects and updates.
	ConsistencyAvailable Consistency = "available"
)

// ConsistencyDefault is the consistency StorageGRID applies to a newly created bucket.
// Reverting a bucket the operator previously managed means putting this value back.
const ConsistencyDefault = ConsistencyReadAfterNewWrite

// consistencyBySpec maps the API-level (PascalCase) consistency values onto their StorageGRID
// wire values. A map rather than a switch, so that adding an enum value without extending the
// mapping surfaces as a clear runtime error rather than a silently missing case.
var consistencyBySpec = map[s3v1alpha1.BucketConsistency]Consistency{
	s3v1alpha1.BucketConsistencyAll:               ConsistencyAll,
	s3v1alpha1.BucketConsistencyStrongGlobal:      ConsistencyStrongGlobal,
	s3v1alpha1.BucketConsistencyStrongSite:        ConsistencyStrongSite,
	s3v1alpha1.BucketConsistencyReadAfterNewWrite: ConsistencyReadAfterNewWrite,
	s3v1alpha1.BucketConsistencyAvailable:         ConsistencyAvailable,
}

// ConsistencyFromSpec translates an API-level consistency value into its StorageGRID wire form.
func ConsistencyFromSpec(spec s3v1alpha1.BucketConsistency) (Consistency, error) {
	wire, ok := consistencyBySpec[spec]
	if !ok {
		return "", fmt.Errorf("unknown bucket consistency %q", spec)
	}
	return wire, nil
}

// GetBucketConsistency fetches the current consistency setting for a bucket.
func GetBucketConsistency(ctx context.Context, bucketName string, tenantClient *TenantClient) (Consistency, error) {
	log := log.FromContext(ctx).WithValues("func", "GetBucketConsistency")
	log.V(1).Info(fmt.Sprintf("Fetching consistency for bucket %s", bucketName))

	setting, err := tenantClient.Bucket().GetConsistency(ctx, bucketName)
	if err != nil {
		return "", fmt.Errorf("failed to fetch consistency for bucket %s: %w", bucketName, err)
	}
	if setting == nil || setting.Consistency == "" {
		return "", fmt.Errorf("backend returned no consistency for bucket %s", bucketName)
	}
	return Consistency(setting.Consistency), nil
}

// UpdateBucketConsistency sets the consistency for an existing bucket.
// Note: StorageGRID applies the change to objects ingested after it; objects already in the
// bucket keep their prior behavior.
func UpdateBucketConsistency(ctx context.Context, bucketName string, desired Consistency, tenantClient *TenantClient) error {
	log := log.FromContext(ctx).WithValues("func", "UpdateBucketConsistency")
	log.V(1).Info(fmt.Sprintf("Updating consistency for bucket %s to %s", bucketName, desired))

	setting := &models.BucketConsistencySetting{Consistency: string(desired)}
	if _, err := tenantClient.Bucket().UpdateConsistency(ctx, bucketName, setting); err != nil {
		return fmt.Errorf("failed to update consistency for bucket %s: %w", bucketName, err)
	}
	return nil
}
