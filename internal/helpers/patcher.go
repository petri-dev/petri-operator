package helpers

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type DeepCopyObject[T any] interface {
	client.Object
	DeepCopyObject() runtime.Object
	DeepCopy() T
}

type StatusPatcher[T DeepCopyObject[T]] struct {
	client   client.Client
	original T
}

func NewStatusPatcher[T DeepCopyObject[T]](c client.Client, obj T) *StatusPatcher[T] {
	return &StatusPatcher[T]{
		client:   c,
		original: obj.DeepCopy(),
	}
}

func (p *StatusPatcher[T]) Patch(ctx context.Context, obj T) error {
	if apiequality.Semantic.DeepEqual(p.original, obj) {
		return nil
	}

	return p.client.Status().Patch(ctx, obj, client.MergeFrom(p.original))
}
