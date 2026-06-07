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

package s3policies

import (
	"context"
	"fmt"

	s3v1alpha1 "github.com/bedag/storagegrid-operator/api/v1alpha1"
	grid "github.com/bedag/storagegrid-operator/pkg/grid"
	"sigs.k8s.io/controller-runtime/pkg/client"
	log "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	PolicyFinalizer = "policy.s3.bedag.ch/finalizer"
)

func renderPolicy(policy s3v1alpha1.CommonPolicySpec) (string, error) {
	statements := make([]grid.PolicyStatement, 0, len(policy.Rules))
	for _, rule := range policy.Rules {
		resource := resourceFromScope(rule.Scope)
		if resource == "" {
			return "", fmt.Errorf("invalid scope: %s", rule.Scope)
		}
		statements = append(statements, grid.PolicyStatement{
			Effect:   rule.Effect,
			Action:   rule.Actions,
			Resource: []string{resource},
		})
	}

	policyDocument := grid.Policy{
		Version:   policy.Version,
		Statement: statements,
	}
	// convert rules to statements
	return grid.GeneratePolicyDocument(policyDocument), nil
}

func resourceFromScope(scope string) string {
	switch scope {
	case "Bucket":
		return "arn:aws:s3:::BUCKET_NAME"
	case "Objects":
		return "arn:aws:s3:::BUCKET_NAME/*"
	default:
		return ""
	}
}

func policyInUse(ctx context.Context, k8sclient client.Client, policyName string, policyNamespace string, policyKind string) (bool, []string) {
	// returns error if the policy is still in use
	log := log.FromContext(ctx).WithValues("function", "policyInUse")

	key := BuildPolicyKey(policyKind, policyNamespace, policyName)

	// Check S3Access references.
	accessList := &s3v1alpha1.S3AccessList{}
	err := k8sclient.List(ctx, accessList,
		client.MatchingFields{".spec.policyRefs": key},
	)
	if client.IgnoreNotFound(err) != nil {
		log.Error(err, "Failed to list S3Accesses for policy", "policyKey", key)
		return true, []string{}
	}

	consumers := []string{}
	for _, access := range accessList.Items {
		consumers = append(consumers, fmt.Sprintf("S3Access/%s/%s", access.Namespace, access.Name))
	}

	// Check S3Bucket.spec.bucketPolicies references.
	bucketList := &s3v1alpha1.S3BucketList{}
	listOpts := []client.ListOption{}
	if policyKind == "S3Policy" && policyNamespace != "" {
		listOpts = append(listOpts, client.InNamespace(policyNamespace))
	}
	err = k8sclient.List(ctx, bucketList, listOpts...)
	if client.IgnoreNotFound(err) != nil {
		log.Error(err, "Failed to list S3Buckets for policy", "policyKey", key)
		return true, []string{}
	}

	for i := range bucketList.Items {
		for _, binding := range bucketList.Items[i].Spec.BucketPolicies {
			refKind := binding.PolicyRef.Kind
			if refKind == "" {
				refKind = "GlobalS3Policy"
			}
			if refKind == policyKind && binding.PolicyRef.Name == policyName {
				consumers = append(consumers, fmt.Sprintf("S3Bucket/%s/%s", bucketList.Items[i].Namespace, bucketList.Items[i].Name))
				break
			}
		}
	}

	return len(consumers) > 0, consumers
}

func BuildPolicyKey(kind, namespace, name string) string {
	switch kind {
	case "GlobalS3Policy":
		return "GlobalS3Policy/" + name
	case "S3Policy":
		return "S3Policy/" + namespace + "/" + name
	default:
		return ""
	}
}
