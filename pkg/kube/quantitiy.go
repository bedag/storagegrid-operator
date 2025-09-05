package kube

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

// ParseBytes parses an int64 byte value into a string like "1Gi".
func ParseBytes(bytes int64) *resource.Quantity {
	quantity := resource.NewQuantity(bytes, resource.BinarySI)
	return quantity
}
