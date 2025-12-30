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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

var _ = Describe("S3Tenant Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		s3tenant := &s3v1alpha1.S3Tenant{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind S3Tenant")
			err := k8sClient.Get(ctx, typeNamespacedName, s3tenant)
			if err != nil && errors.IsNotFound(err) {
				resource := &s3v1alpha1.S3Tenant{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: s3v1alpha1.S3TenantSpec{
						CommonTenantSpec: s3v1alpha1.CommonTenantSpec{
							StorageGridRef: corev1.LocalObjectReference{
								Name: "test-storagegrid",
							},
							S3TenantClassName: "default",
							StorageQuota:      &[]resource.Quantity{resource.MustParse("1Gi")}[0],
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &s3v1alpha1.S3Tenant{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance S3Tenant")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully create the resource", func() {
			By("Verifying the created resource exists")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, s3tenant)
				return err == nil
			}).Should(BeTrue())
		})
	})
})
