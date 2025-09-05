package controller

import (
	"reflect"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

type HasManagementPolicy interface {
	GetManagementPolicy() string
}

// PredicateWithoutStatusChange creates a predicate that triggers reconciliation
// for changes in generation, annotations, or labels.
func PredicateWithoutStatusChange() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Ensure neither object is nil
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}

			// Check if the generation has changed
			if e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration() {
				return true
			}

			// Check if annotations have changed
			if !reflect.DeepEqual(e.ObjectNew.GetAnnotations(), e.ObjectOld.GetAnnotations()) {
				return true
			}

			// Check if labels have changed
			if !reflect.DeepEqual(e.ObjectNew.GetLabels(), e.ObjectOld.GetLabels()) {
				return true
			}

			// No meaningful changes detected
			return false
		},
	}
}

// Ignore reconciliation if managementPolicy is "Reference"
func IgnoreReferencePolicyPredicate() predicate.Funcs {

	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {

			obj, ok := e.ObjectNew.(HasManagementPolicy)
			if !ok {
				// reconcile if the object does not have a management policy
				return false
			}

			// If managementPolicy is "Reference", ignore updates
			// eg if its "Managed" or "Full", this will return true and thus reconcile
			return obj.GetManagementPolicy() != "Reference"
		},

		// Create and Delete events are always allowed
	}
}
