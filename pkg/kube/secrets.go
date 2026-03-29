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
	"fmt"
	"reflect"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// SecretFinalizer is added to critical secrets to prevent premature deletion during namespace teardown.
	SecretFinalizer = "secret.s3.bedag.ch/finalizer"
)

// FetchCredentialsFromSecret fetches a Secret and returns the credentials as strings.
// assumes that the keys are always "username" and "password".
func FetchCredentialsFromSecret(ctx context.Context, k8sClient client.Client, namespace string, secretName string) (username string, password string, err error) {
	var secret corev1.Secret
	err = k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName}, &secret)
	if err != nil {
		return "", "", err
	}

	username = string(secret.Data["username"])
	password = string(secret.Data["password"])
	return username, password, nil
}

func FetchKeyPairFromSecret(ctx context.Context, k8sClient client.Client, namespace string, secretName string) (accessKeyId string, secretAccessKey string, err error) {
	var secret corev1.Secret
	err = k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName}, &secret)
	if err != nil {
		return "", "", err
	}

	accessKeyId = string(secret.Data["accessKey"])
	secretAccessKey = string(secret.Data["secretKey"])
	return accessKeyId, secretAccessKey, nil
}

// creates a secret with keys "username" and "password" in the specified namespace.
func CreateCredentialSecret(ctx context.Context, k8sClient client.Client, namespace string, secretName string, username string, password string, owner metav1.Object, ownerKind string) error {
	data := map[string][]byte{
		"username": []byte(username),
		"password": []byte(password),
	}

	return createSecret(ctx, k8sClient, secretName, namespace, data, owner, ownerKind)
}

// creates a secret with keys "accessKeyId" and "secretAccessKey" in the specified namespace.
func CreateKeyPairSecret(ctx context.Context, k8sClient client.Client, namespace string, secretName string, accessKeyId string, secretAccessKey string, owner metav1.Object, ownerKind string) error {
	data := map[string][]byte{
		"accessKey": []byte(accessKeyId),
		"secretKey": []byte(secretAccessKey),
	}

	return createSecret(ctx, k8sClient, secretName, namespace, data, owner, ownerKind)
}

func createSecret(ctx context.Context, k8sClient client.Client, secretName string, secretNamespace string, data map[string][]byte, owner metav1.Object, ownerKind string) error {
	log := log.FromContext(ctx).WithValues("func", "createSecret")
	log.V(1).Info("Creating or updating secret", "name", secretName, "namespace", secretNamespace)

	labels := secretLabels(ownerKind, owner.GetName())

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: secretNamespace,
			Labels:    labels,
		},
		Data: data,
	}

	existing := &corev1.Secret{}
	err := k8sClient.Get(ctx, client.ObjectKey{
		Name:      desired.Name,
		Namespace: desired.Namespace,
	}, existing)

	// Doesn't exist → create new secret.
	if apierrors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(owner, desired, k8sClient.Scheme()); err != nil {
			return err
		}
		if err := k8sClient.Create(ctx, desired); err != nil {
			return err
		}
		return nil
	} else if err != nil {
		// Any other error.
		return err
	}

	// Check ownership before updating.
	if !metav1.IsControlledBy(existing, owner) {
		return fmt.Errorf(
			"secret %s already exists and cannot be owned by %s/%s",
			existing.Name, owner.GetName(), owner.GetNamespace(),
		)
	}

	// Reconcile labels and data on existing secret.
	labelsChanged := mergeLabels(existing, labels)
	dataChanged := !reflect.DeepEqual(existing.Data, desired.Data)

	if dataChanged {
		existing.Data = desired.Data
	}

	if labelsChanged || dataChanged {
		if err := k8sClient.Update(ctx, existing); err != nil {
			return err
		}
	}

	log.V(1).Info("Secret created or updated successfully", "name", secretName, "namespace", secretNamespace)

	return nil
}

// secretLabels returns the standard labels for operator-managed secrets.
func secretLabels(ownerKind string, ownerName string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "storagegrid-operator",
		"app.kubernetes.io/part-of":    strings.ToLower(ownerKind),
		"app.kubernetes.io/instance":   ownerName,
	}
}

// mergeLabels adds missing labels to the existing object. Returns true if any labels were added or changed.
func mergeLabels(obj metav1.Object, desired map[string]string) bool {
	existing := obj.GetLabels()
	if existing == nil {
		obj.SetLabels(desired)
		return true
	}
	changed := false
	for k, v := range desired {
		if existing[k] != v {
			existing[k] = v
			changed = true
		}
	}
	return changed
}

// DeleteSecret deletes a secret from the specified namespace.
func DeleteSecret(ctx context.Context, k8sClient client.Client, namespace string, secretName string) error {
	log := log.FromContext(ctx).WithValues("func", "DeleteSecret")
	log.V(1).Info("Deleting secret", "name", secretName, "namespace", namespace)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
		},
	}

	if err := k8sClient.Delete(ctx, secret); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("Secret already deleted", "name", secretName, "namespace", namespace)
			return nil
		}
		log.Error(err, "Failed to delete secret", "name", secretName, "namespace", namespace)
		return err
	}

	log.V(1).Info("Secret deleted successfully", "name", secretName, "namespace", namespace)
	return nil
}

// AddSecretFinalizer adds the protective finalizer to a secret to prevent premature deletion during namespace teardown.
func AddSecretFinalizer(ctx context.Context, k8sClient client.Client, namespace string, secretName string) error {
	log := log.FromContext(ctx).WithValues("func", "AddSecretFinalizer")

	secret := &corev1.Secret{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName}, secret); err != nil {
		return fmt.Errorf("failed to get secret %s/%s for adding finalizer: %w", namespace, secretName, err)
	}

	if controllerutil.ContainsFinalizer(secret, SecretFinalizer) {
		return nil
	}

	controllerutil.AddFinalizer(secret, SecretFinalizer)
	if err := k8sClient.Update(ctx, secret); err != nil {
		return fmt.Errorf("failed to add finalizer to secret %s/%s: %w", namespace, secretName, err)
	}

	log.V(1).Info("Added finalizer to secret", "name", secretName, "namespace", namespace)
	return nil
}

// RemoveSecretFinalizer removes the protective finalizer from a secret to allow deletion.
func RemoveSecretFinalizer(ctx context.Context, k8sClient client.Client, namespace string, secretName string) error {
	log := log.FromContext(ctx).WithValues("func", "RemoveSecretFinalizer")

	secret := &corev1.Secret{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(1).Info("Secret already deleted, no finalizer to remove", "name", secretName, "namespace", namespace)
			return nil
		}
		return fmt.Errorf("failed to get secret %s/%s for removing finalizer: %w", namespace, secretName, err)
	}

	if !controllerutil.ContainsFinalizer(secret, SecretFinalizer) {
		return nil
	}

	controllerutil.RemoveFinalizer(secret, SecretFinalizer)
	if err := k8sClient.Update(ctx, secret); err != nil {
		return fmt.Errorf("failed to remove finalizer from secret %s/%s: %w", namespace, secretName, err)
	}

	log.V(1).Info("Removed finalizer from secret", "name", secretName, "namespace", namespace)
	return nil
}
