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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

// These cases exercise the CEL rule on S3ObjectLockBucketSpec against the real CRD schema;
// no webhook runs in this suite, so only the API server's own validation is in play.
var _ = Describe("S3Bucket s3ObjectLock CEL validation", func() {
	ctx := context.Background()

	newBucket := func(name string, lock *s3v1alpha1.S3ObjectLockBucketSpec) *s3v1alpha1.S3Bucket {
		return &s3v1alpha1.S3Bucket{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: s3v1alpha1.S3BucketSpec{
				S3TenantRef:  corev1.ObjectReference{Name: "cel-tenant"},
				S3ObjectLock: lock,
			},
		}
	}

	DescribeTable("admission",
		func(name string, lock *s3v1alpha1.S3ObjectLockBucketSpec, wantErr string) {
			b := newBucket(name, lock)
			err := k8sClient.Create(ctx, b)
			if wantErr == "" {
				Expect(err).NotTo(HaveOccurred())
				Expect(k8sClient.Delete(ctx, b)).To(Succeed())
				return
			}
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(wantErr))
		},
		Entry("governance with positive retention", "cel-gov-30",
			&s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeGovernance, RetentionInDays: 30}, ""),
		Entry("governance without default retention (-1)", "cel-gov-nodefault",
			&s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeGovernance, RetentionInDays: -1}, ""),
		Entry("compliance without default retention (-1)", "cel-comp-nodefault",
			&s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeCompliance, RetentionInDays: -1}, ""),
		Entry("disabled without retention", "cel-disabled",
			&s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeDisabled}, ""),
		Entry("governance missing retention", "cel-gov-missing",
			&s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeGovernance}, "retentionInDays is required"),
		Entry("compliance missing retention", "cel-comp-missing",
			&s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeCompliance}, "retentionInDays is required"),
		Entry("retention below -1", "cel-gov-minus2",
			&s3v1alpha1.S3ObjectLockBucketSpec{Mode: s3v1alpha1.S3ObjectLockModeGovernance, RetentionInDays: -2}, "retentionInDays"),
	)
})
