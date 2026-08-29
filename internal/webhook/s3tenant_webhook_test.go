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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachineryruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

var _ = Describe("S3Tenant Webhook", func() {

	Context("When creating S3Tenant under Defaulting Webhook", func() {
		It("Should fill in the default value if a required field is empty", func() {

			// TODO(user): Add your logic here.

		})
	})

	Context("When creating S3Tenant under Validating Webhook", func() {
		It("Should deny if a required field is empty", func() {

			// TODO(user): Add your logic here.

		})

		It("Should admit if all required fields are provided", func() {

			// TODO(user): Add your logic here.

		})
	})

})

var _ = Describe("S3Tenant deletion validation", func() {
	ctx := context.Background()

	// Mirrors the index registered by S3TenantReconciler.SetupWithManager.
	indexKey := func(obj client.Object) []string {
		bucket := obj.(*s3v1alpha1.S3Bucket)
		ns := bucket.Spec.S3TenantRef.Namespace
		if ns == "" {
			ns = bucket.Namespace
		}
		return []string{ns + "/" + bucket.Spec.S3TenantRef.Name}
	}

	testScheme := apimachineryruntime.NewScheme()
	Expect(s3v1alpha1.AddToScheme(testScheme)).To(Succeed())

	newValidator := func(objs ...client.Object) *S3TenantValidator {
		return &S3TenantValidator{
			k8sClient: fake.NewClientBuilder().
				WithScheme(testScheme).
				WithIndex(&s3v1alpha1.S3Bucket{}, "spec.s3TenantRef.namespacedName", indexKey).
				WithObjects(objs...).
				Build(),
		}
	}

	tenant := func() *s3v1alpha1.S3Tenant {
		return &s3v1alpha1.S3Tenant{
			ObjectMeta: metav1.ObjectMeta{Name: "my-tenant", Namespace: "default"},
		}
	}

	bucket := func(name string) *s3v1alpha1.S3Bucket {
		return &s3v1alpha1.S3Bucket{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: s3v1alpha1.S3BucketSpec{
				S3TenantRef: corev1.ObjectReference{Name: "my-tenant"},
			},
		}
	}

	Context("when S3Buckets still reference the tenant", func() {
		// Denying the DELETE would leave the namespace controller retrying a call that
		// admission keeps rejecting, hanging `kubectl delete namespace` in Terminating with
		// no finalizer to force-remove. The finalizer in S3TenantReconciler.finalize is what
		// actually holds the tenant, so admission only warns.
		It("warns but does not deny", func() {
			v := newValidator(bucket("logs"), bucket("backups"))

			warnings, err := v.ValidateDelete(ctx, tenant())

			Expect(err).NotTo(HaveOccurred(), "deletion must be accepted so namespaces can drain")
			Expect(warnings).To(HaveLen(1))
			Expect(warnings[0]).To(ContainSubstring("logs"))
			Expect(warnings[0]).To(ContainSubstring("backups"))
			Expect(warnings[0]).To(ContainSubstring("Deleting phase"))
		})
	})

	Context("when no buckets reference the tenant", func() {
		It("admits without a warning", func() {
			v := newValidator()

			warnings, err := v.ValidateDelete(ctx, tenant())

			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})
	})

	Context("when the opt-in deletion protection annotation is set", func() {
		// This one stays a hard denial: it is explicit, per-object and user-removable, so a
		// namespace cannot wander into it by accident.
		It("denies the deletion", func() {
			v := newValidator()

			protected := tenant()
			protected.Annotations = map[string]string{s3v1alpha1.AnnotationDeletionProtection: "true"}

			_, err := v.ValidateDelete(ctx, protected)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("protected by annotation"))
		})
	})
})
