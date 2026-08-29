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

package v1alpha1

// condition types.
const (
	ConditionTypeReady                    = "Ready"
	ConditionTypeReachable                = "Reachable"
	ConditionTypeQuotaSufficient          = "QuotaSufficient"
	ConditionTypeCreated                  = "Created"
	ConditionTypeReconcileSucceeded       = "ReconcileSucceeded"
	ContitionTypeBackingResourceReady     = "BackingResourceReady"
	ConditionTypeAccountReady             = "AccountReady"
	ConditionTypeBound                    = "Bound"
	ConditionTypePending                  = "Pending"
	ConditionTypeRetained                 = "Retained"
	ConditionTypeRetainThenDelete         = "RetainThenDelete"
	ConditionTypeDeletionTimestampReached = "DeletionTimestampReached"
	ConditionTypeConfigurationSynced      = "ConfigurationSynced"
	ConditionTypeOwnershipConflict        = "OwnershipConflict"
	ConditionTypeS3ObjectLockSupported    = "S3ObjectLockSupported"
	ConditionTypeLifecycleSynced          = "LifecycleSynced"
	ConditionTypeConsistencySynced        = "ConsistencySynced"

	// ConditionTypeDeleting is set while a resource is being deleted. It carries the
	// reason the deletion has not completed yet (waiting for linked buckets, waiting
	// for backend confirmation, ...) so that a slow deletion is legible as progress
	// rather than as a failure. Deletion waits deliberately leave
	// ConditionTypeReconcileSucceeded true - they are waits, not errors.
	ConditionTypeDeleting = "Deleting"

	// ConditionTypeDraining is set while a bucket drain is in progress. The counts and
	// timing live in S3Bucket.status.drainStatus; this condition carries the state.
	ConditionTypeDraining = "Draining"
)
