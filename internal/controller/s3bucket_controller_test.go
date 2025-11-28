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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

var _ = Describe("S3Bucket Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"
		const tenantName = "test-tenant"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		tenantNamespacedName := types.NamespacedName{
			Name:      tenantName,
			Namespace: "default",
		}
		s3bucket := &s3v1alpha1.S3Bucket{}

		BeforeEach(func() {
			// Create S3Tenant dependency first
			By("creating the S3Tenant dependency")
			tenant := &s3v1alpha1.S3Tenant{}
			err := k8sClient.Get(ctx, tenantNamespacedName, tenant)
			if err != nil && errors.IsNotFound(err) {
				tenant = &s3v1alpha1.S3Tenant{
					ObjectMeta: metav1.ObjectMeta{
						Name:      tenantName,
						Namespace: "default",
					},
					Spec: s3v1alpha1.S3TenantSpec{},
				}
				Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
				// Update status to Bound phase
				tenant.Status.Phase = s3v1alpha1.PhaseBound
				Expect(k8sClient.Status().Update(ctx, tenant)).To(Succeed())
			}

			By("creating the custom resource for the Kind S3Bucket")
			err = k8sClient.Get(ctx, typeNamespacedName, s3bucket)
			if err != nil && errors.IsNotFound(err) {
				resource := &s3v1alpha1.S3Bucket{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: s3v1alpha1.S3BucketSpec{
						S3TenantRef: corev1.ObjectReference{
							Name:      tenantName,
							Namespace: "default",
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &s3v1alpha1.S3Bucket{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance S3Bucket")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			// Cleanup tenant
			tenant := &s3v1alpha1.S3Tenant{}
			err = k8sClient.Get(ctx, tenantNamespacedName, tenant)
			if err == nil {
				By("Cleanup the S3Tenant dependency")
				Expect(k8sClient.Delete(ctx, tenant)).To(Succeed())
			}
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &S3BucketReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			// Without a real StorageGrid backend and bound tenant, we expect an error
			// The important thing is that it doesn't panic and handles dependencies
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("tenant"))
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})
