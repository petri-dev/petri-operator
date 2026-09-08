package controller

import (
	"time"

	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// retryBackoffMax caps the per-item exponential backoff applied on reconcile errors. Starts at 1s and doubles up to this ceiling.
const retryBackoffMax = 5 * time.Minute

type RateLimitOptions struct {
	MaxConcurrentReconciles int
	QPS                     float64
	Burst                   int
}

func DefaultRateLimitOptions() RateLimitOptions {
	return RateLimitOptions{MaxConcurrentReconciles: 4, QPS: 10, Burst: 100}
}

func (o RateLimitOptions) controllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: o.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedMaxOfRateLimiter(
			// per-item exponential backoff on errors: 1s to retryBackoffMax
			workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, retryBackoffMax),
			// overall queue cap
			&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(o.QPS), o.Burst)},
		),
	}
}
