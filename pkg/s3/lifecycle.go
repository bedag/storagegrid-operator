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

package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Stable rule IDs for the operator-managed lifecycle configuration.
// Kept as constants so the diff against the backend is deterministic and idempotent.
const (
	LifecycleRuleIDExpiration                = "operator-managed-expiration"
	LifecycleRuleIDNoncurrentVersion         = "operator-managed-noncurrent-version-expiration"
	LifecycleRuleIDExpiredObjectDeleteMarker = "operator-managed-expired-delete-marker-cleanup"
)

// Default for NoncurrentVersionExpiration.NoncurrentDays. Set to 1 so that on a
// versioned bucket the noncurrent version created by the current-version Expiration
// (which only places a delete-marker on versioned buckets) is reaped almost immediately
// — keeping the user's intended retention close to ExpirationInDays rather than ~2x.
// Inert no-op on unversioned buckets.
const lifecycleNoncurrentDays int32 = 1

// LifecycleSpec is a backend-agnostic description of the lifecycle configuration the
// operator wants to apply to a bucket. It mirrors api/v1alpha1.LifecycleManagementSpec
// but lives here to keep pkg/s3 free of a dependency on the API package.
type LifecycleSpec struct {
	// ExpirationInDays maps to S3 Expiration.Days. 0 means "no current-version expiration rule".
	ExpirationInDays int32
}

// IsEmpty reports whether the spec describes no lifecycle rules at all.
// When true, the operator should ensure the bucket has no managed lifecycle configuration.
func (s LifecycleSpec) IsEmpty() bool {
	return s.ExpirationInDays <= 0
}

// BuildLifecycleConfiguration translates the operator-level spec into the AWS SDK type
// expected by PutBucketLifecycleConfiguration. Returns nil when the spec is empty.
//
// Design intent: deliver the user's intuitive expectation — "objects are gone N days
// after creation" — regardless of whether the bucket is versioned now, becomes
// versioned later, or was imported with versioning already enabled. To achieve this
// the configuration emits three separate rules (StorageGRID requires
// ExpiredObjectDeleteMarker to live in its own rule; it cannot share an Expiration
// block with Days):
//
//   - Rule 1 (Expiration.Days = N): on unversioned buckets the object is deleted
//     outright; on versioned buckets a delete-marker is placed and the prior version
//     becomes noncurrent.
//   - Rule 2 (NoncurrentVersionExpiration.NoncurrentDays = 1): reaps the noncurrent
//     version produced by rule 1 on versioned buckets almost immediately, keeping
//     total retention close to N rather than ~2N. Inert no-op on unversioned buckets.
//   - Rule 3 (Expiration.ExpiredObjectDeleteMarker = true): cleans up stranded
//     delete-markers once their underlying versions are gone. Inert on unversioned
//     buckets.
//
// AbortIncompleteMultipartUpload is intentionally omitted — StorageGRID rejects it
// with `MalformedXML: AbortIncompleteMultipartUpload rules are not supported`.
//
// Documented support reference:
// https://docs.netapp.com/us-en/storagegrid-enable/examples/bucket-lifecycle-examples.html
//
// Net effect: the same configuration is correct under all versioning states and
// self-heals if versioning is toggled on out-of-band.
func BuildLifecycleConfiguration(spec LifecycleSpec) *s3types.BucketLifecycleConfiguration {
	if spec.IsEmpty() {
		return nil
	}

	rules := []s3types.LifecycleRule{
		{
			ID:     aws.String(LifecycleRuleIDExpiration),
			Status: s3types.ExpirationStatusEnabled,
			// An empty Filter (no Prefix, no Tags) targets all objects in the bucket.
			Filter: &s3types.LifecycleRuleFilter{},
			Expiration: &s3types.LifecycleExpiration{
				Days: aws.Int32(spec.ExpirationInDays),
			},
		},
		{
			ID:     aws.String(LifecycleRuleIDNoncurrentVersion),
			Status: s3types.ExpirationStatusEnabled,
			Filter: &s3types.LifecycleRuleFilter{},
			NoncurrentVersionExpiration: &s3types.NoncurrentVersionExpiration{
				NoncurrentDays: aws.Int32(lifecycleNoncurrentDays),
			},
		},
		{
			ID:     aws.String(LifecycleRuleIDExpiredObjectDeleteMarker),
			Status: s3types.ExpirationStatusEnabled,
			Filter: &s3types.LifecycleRuleFilter{},
			Expiration: &s3types.LifecycleExpiration{
				ExpiredObjectDeleteMarker: aws.Bool(true),
			},
		},
	}

	return &s3types.BucketLifecycleConfiguration{Rules: rules}
}

// LifecycleFingerprint returns a stable, comparable JSON representation of the lifecycle
// configuration. It is used by the controller to detect drift versus status.LastAppliedLifecycle.
// Returns the empty string when cfg is nil or contains no rules.
func LifecycleFingerprint(cfg *s3types.BucketLifecycleConfiguration) string {
	if cfg == nil || len(cfg.Rules) == 0 {
		return ""
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		// JSON marshaling of SDK structs cannot realistically fail; on the off-chance
		// it does, fall back to a degenerate fingerprint that still differs from "".
		return fmt.Sprintf("error:%v", err)
	}
	return string(b)
}

// PutLifecycle applies the given lifecycle configuration to the bucket.
func PutLifecycle(ctx context.Context, bucket string, cfg *s3types.BucketLifecycleConfiguration, s3Client *S3Client) error {
	log := log.FromContext(ctx).WithValues("func", "putLifecycle", "bucket", bucket)
	log.V(1).Info("Applying lifecycle configuration")

	_, err := s3Client.PutBucketLifecycleConfiguration(ctx, &awss3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucket),
		LifecycleConfiguration: cfg,
	})
	if err != nil {
		log.Error(err, "Failed to apply lifecycle configuration")
		return err
	}

	log.V(1).Info("Lifecycle configuration applied")
	return nil
}

// GetLifecycle returns the current lifecycle configuration of the bucket.
// Returns (nil, nil) when no lifecycle configuration is set on the bucket.
func GetLifecycle(ctx context.Context, bucket string, s3Client *S3Client) (*s3types.BucketLifecycleConfiguration, error) {
	log := log.FromContext(ctx).WithValues("func", "getLifecycle", "bucket", bucket)
	log.V(1).Info("Fetching lifecycle configuration")

	out, err := s3Client.GetBucketLifecycleConfiguration(ctx, &awss3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		if isNoSuchLifecycleConfiguration(err) {
			log.V(1).Info("No lifecycle configuration set on bucket")
			return nil, nil
		}
		log.Error(err, "Failed to get lifecycle configuration")
		return nil, err
	}

	return &s3types.BucketLifecycleConfiguration{Rules: out.Rules}, nil
}

// DeleteLifecycle removes any lifecycle configuration from the bucket.
// Treats a missing configuration as a no-op (idempotent).
func DeleteLifecycle(ctx context.Context, bucket string, s3Client *S3Client) error {
	log := log.FromContext(ctx).WithValues("func", "deleteLifecycle", "bucket", bucket)
	log.V(1).Info("Deleting lifecycle configuration")

	_, err := s3Client.DeleteBucketLifecycle(ctx, &awss3.DeleteBucketLifecycleInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		if isNoSuchLifecycleConfiguration(err) {
			log.V(1).Info("No lifecycle configuration to delete")
			return nil
		}
		log.Error(err, "Failed to delete lifecycle configuration")
		return err
	}

	log.V(1).Info("Lifecycle configuration deleted")
	return nil
}

// isNoSuchLifecycleConfiguration reports whether err signals that the bucket simply has
// no lifecycle configuration. StorageGRID and AWS both surface this as the
// `NoSuchLifecycleConfiguration` API error code.
func isNoSuchLifecycleConfiguration(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchLifecycleConfiguration" {
		return true
	}
	// Fallback for backends that return the code in the error string only.
	return strings.Contains(err.Error(), "NoSuchLifecycleConfiguration")
}
