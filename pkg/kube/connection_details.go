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

package kube

import (
	"context"
	"errors"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
)

// ErrSecretNotOwned is returned by ReconcileOwnedSecret when the target Secret already
// exists and is controlled by a different owner.
var ErrSecretNotOwned = errors.New("secret exists and is controlled by a different owner")

// ConnectionDetailsInputs holds the data points needed to render a connection-details
type ConnectionDetailsInputs struct {
	AccessKeyID     string
	SecretAccessKey string
	EndpointURL     string
	Region          string
	BucketName      string
}

// BuildConnectionDetailsData renders the standard connection-details Secret payload
// using the AWS env-var conventions.
func BuildConnectionDetailsData(in ConnectionDetailsInputs) map[string][]byte {
	return map[string][]byte{
		s3v1alpha1.ConnectionDetailKeyAccessKeyID:     []byte(in.AccessKeyID),
		s3v1alpha1.ConnectionDetailKeySecretAccessKey: []byte(in.SecretAccessKey),
		s3v1alpha1.ConnectionDetailKeyEndpointURL:     []byte(in.EndpointURL),
		s3v1alpha1.ConnectionDetailKeyRegion:          []byte(in.Region),
		s3v1alpha1.ConnectionDetailKeyBucket:          []byte(in.BucketName),
	}
}

// ReconcileOwnedSecret creates or updates a connection-details Secret owned by `owner`.
// Standard labels (managed-by, part-of, instance, secret-type) are derived from the
// owner's GVK and name. Secrets with no controller reference are adopted (the owner ref
// is re-established) so that accidental removal of the OwnerReference by a user heals
// on the next reconcile. If the Secret already exists and is controlled by a different
// owner, it returns ErrSecretNotOwned without modifying the existing Secret.
//
// Returns true if the Secret was created or updated; false when no change was needed.
func ReconcileOwnedSecret(
	ctx context.Context,
	k8sClient client.Client,
	namespace string,
	name string,
	data map[string][]byte,
	owner client.Object,
) (bool, error) {
	logger := log.FromContext(ctx).WithValues("func", "ReconcileOwnedSecret", "name", name, "namespace", namespace)

	gvk, err := apiutil.GVKForObject(owner, k8sClient.Scheme())
	if err != nil {
		return false, err
	}
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "storagegrid-operator",
		"app.kubernetes.io/part-of":    strings.ToLower(gvk.Kind),
		"app.kubernetes.io/instance":   owner.GetName(),
		"s3.bedag.ch/secret-type":      "connection-details",
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Data: data,
	}

	existing := &corev1.Secret{}
	err = k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(owner, desired, k8sClient.Scheme()); err != nil {
			return false, err
		}
		if err := k8sClient.Create(ctx, desired); err != nil {
			return false, err
		}
		logger.V(1).Info("Created secret")
		return true, nil
	} else if err != nil {
		return false, err
	}

	adopted := false
	if !metav1.IsControlledBy(existing, owner) {
		// Refuse to overwrite a Secret controlled by a different owner. Secrets with no
		// controller (orphaned by a user editing OwnerReferences) are adopted instead.
		if metav1.GetControllerOf(existing) != nil {
			return false, ErrSecretNotOwned
		}
		if err := controllerutil.SetControllerReference(owner, existing, k8sClient.Scheme()); err != nil {
			return false, err
		}
		adopted = true
	}

	labelsChanged := mergeLabels(existing, labels)
	dataChanged := !reflect.DeepEqual(existing.Data, data)
	if dataChanged {
		existing.Data = data
	}
	if !adopted && !labelsChanged && !dataChanged {
		return false, nil
	}
	if err := k8sClient.Update(ctx, existing); err != nil {
		return false, err
	}
	if adopted {
		logger.V(1).Info("Adopted orphaned secret")
	} else {
		logger.V(1).Info("Updated secret")
	}
	return true, nil
}

// DeleteOwnedSecret deletes a Secret by name. Callers are expected to only call this
// for Secrets they previously created (tracked via status). Returns (deleted, err):
// deleted=true when a delete request was issued; err=nil with deleted=false means the
// Secret did not exist (treated as a no-op so removal is idempotent).
func DeleteOwnedSecret(
	ctx context.Context,
	k8sClient client.Client,
	namespace string,
	name string,
	owner metav1.Object,
) (bool, error) {
	existing := &corev1.Secret{}
	err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, existing)
	if apierrors.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if err := k8sClient.Delete(ctx, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
