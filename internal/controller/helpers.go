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
	"reflect"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

type HasManagementPolicy interface {
	GetManagementPolicy() string
}

// PredicateWithoutStatusChange creates a predicate that triggers reconciliation.
// for changes in generation, annotations, or labels.
func PredicateWithoutStatusChange() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Ensure neither object is nil.
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}

			// Check if the generation has changed.
			if e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration() {
				return true
			}

			// Check if annotations have changed.
			if !reflect.DeepEqual(e.ObjectNew.GetAnnotations(), e.ObjectOld.GetAnnotations()) {
				return true
			}

			// Check if labels have changed.
			if !reflect.DeepEqual(e.ObjectNew.GetLabels(), e.ObjectOld.GetLabels()) {
				return true
			}

			// No meaningful changes detected.
			return false
		},
	}
}

// Ignore reconciliation if managementPolicy is "Reference".
func IgnoreReferencePolicyPredicate() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			obj, ok := e.ObjectNew.(HasManagementPolicy)
			if !ok {
				// reconcile if the object does not have a management policy.
				return false
			}

			// If managementPolicy is "Reference", ignore updates.
			// eg if its "Managed" or "Full", this will return true and thus reconcile.
			return obj.GetManagementPolicy() != "Reference"
		},

		// Create and Delete events are always allowed.
	}
}
