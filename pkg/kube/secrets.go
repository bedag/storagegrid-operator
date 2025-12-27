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

	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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

	accessKeyId = string(secret.Data["accessKeyId"])
	secretAccessKey = string(secret.Data["secretAccessKey"])
	return accessKeyId, secretAccessKey, nil
}

// creates a secret with keys "username" and "password" in the specified namespace.
func CreateCredentialSecret(ctx context.Context, k8sClient client.Client, namespace string, secretName string, username string, password string, owner metav1.Object) error {
	data := map[string][]byte{
		"username": []byte(username),
		"password": []byte(password),
	}

	return createSecret(ctx, k8sClient, secretName, namespace, data, owner)
}

// creates a secret with keys "accessKeyId" and "secretAccessKey" in the specified namespace.
func CreateKeyPairSecret(ctx context.Context, k8sClient client.Client, namespace string, secretName string, accessKeyId string, secretAccessKey string, owner metav1.Object) error {
	data := map[string][]byte{
		"accessKeyId":     []byte(accessKeyId),
		"secretAccessKey": []byte(secretAccessKey),
	}

	return createSecret(ctx, k8sClient, secretName, namespace, data, owner)
}

func createSecret(ctx context.Context, k8sClient client.Client, secretName string, secretNamespace string, data map[string][]byte, owner metav1.Object) error {
	log := log.FromContext(ctx).WithValues("func", "createSecret")
	log.V(1).Info("Creating or updating secret", "name", secretName, "namespace", secretNamespace)

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: secretNamespace,
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

	// Update existing if necessary.
	if !reflect.DeepEqual(existing.Data, desired.Data) {
		existing.Data = desired.Data
		if err := k8sClient.Update(ctx, existing); err != nil {
			return err
		}
	}

	log.V(1).Info("Secret created or updated successfully", "name", secretName, "namespace", secretNamespace)

	return nil
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
