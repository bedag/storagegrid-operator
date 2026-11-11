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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

var _ = Describe("StorageGrid metrics validation", func() {
	ctx := context.Background()
	v := &StorageGridValidator{}

	grid := func(m *s3v1alpha1.StorageGridMetrics) *s3v1alpha1.StorageGrid {
		sg := &s3v1alpha1.StorageGrid{}
		sg.Name = "grid"
		sg.Spec.Metrics = m
		return sg
	}

	It("rejects a reserved allowed label", func() {
		_, err := v.ValidateCreate(ctx, grid(&s3v1alpha1.StorageGridMetrics{
			Grid: &s3v1alpha1.GridMetrics{Enabled: true, AllowedLabels: []string{"namespace"}},
		}))
		Expect(err).To(MatchError(ContainSubstring("namespace")))
	})

	It("rejects duplicate query names on update", func() {
		sg := grid(&s3v1alpha1.StorageGridMetrics{
			Grid: &s3v1alpha1.GridMetrics{Enabled: true, Queries: []s3v1alpha1.GridMetricQuery{
				{Name: "a", Query: "q1"}, {Name: "a", Query: "q2"},
			}},
		})
		_, err := v.ValidateUpdate(ctx, grid(nil), sg)
		Expect(err).To(MatchError(ContainSubstring("Duplicate value")))
	})

	It("accepts a non-reserved allowed label", func() {
		_, err := v.ValidateCreate(ctx, grid(&s3v1alpha1.StorageGridMetrics{
			Grid: &s3v1alpha1.GridMetrics{Enabled: true, AllowedLabels: []string{"site_name"}},
		}))
		Expect(err).NotTo(HaveOccurred())
	})
})
