package utils

import (
	"context"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Antrakos/provider-http-async/apis/interfaces"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
)

const (
	ErrFailedToSetStatus = "failed to update status"
)

// SetRequestStatusFunc is a function that sets the status of a resource.
type SetRequestStatusFunc func()

// RequestResource is a struct that holds the status writer, resource object, request context, http response, http request, and local client.
// StatusWriter and Resource must reference the same underlying CR: conflict-retry in
// SetRequestResourceStatus refreshes Resource and re-applies the setters through StatusWriter.
type RequestResource struct {
	StatusWriter   interfaces.BaseStatusWriter // Common status writer interface
	Resource       client.Object               // Underlying resource for status updates
	RequestContext context.Context
	HttpResponse   httpClient.HttpResponse
	HttpRequest    httpClient.HttpRequest
	LocalClient    client.Client
}

func (rr *RequestResource) SetStatusCode() SetRequestStatusFunc {
	return func() {
		if rr.HttpResponse.StatusCode != 0 {
			rr.StatusWriter.SetStatusCode(rr.HttpResponse.StatusCode)
		}
	}
}

func (rr *RequestResource) SetHeaders() SetRequestStatusFunc {
	return func() {
		if rr.HttpResponse.Headers != nil {
			rr.StatusWriter.SetHeaders(rr.HttpResponse.Headers)
		}
	}
}

func (rr *RequestResource) SetBody() SetRequestStatusFunc {
	return func() {
		if rr.HttpResponse.Body != "" {
			rr.StatusWriter.SetBody(rr.HttpResponse.Body)
		}
	}
}

func (rr *RequestResource) SetRequestDetails() SetRequestStatusFunc {
	return func() {
		if rr.HttpRequest.Method != "" {
			rr.StatusWriter.SetRequestDetails(rr.HttpRequest.URL, rr.HttpRequest.Method, rr.HttpRequest.Body, rr.HttpRequest.Headers)
		}
	}
}

func (rr *RequestResource) SetSynced() SetRequestStatusFunc {
	return func() {
		if synced, ok := rr.StatusWriter.(interfaces.DisposableRequestStatusWriter); ok {
			synced.SetSynced(true)
		}
	}
}

func (rr *RequestResource) SetLastReconcileTime() SetRequestStatusFunc {
	return func() {
		if lastReconcileTimeSetter, ok := rr.StatusWriter.(interfaces.DisposableRequestStatusWriter); ok {
			lastReconcileTimeSetter.SetLastReconcileTime()
		}
	}
}

func (rr *RequestResource) SetCache() SetRequestStatusFunc {
	return func() {
		if cached, ok := rr.StatusWriter.(interfaces.RequestStatusWriter); ok {
			cached.SetCache(rr.HttpResponse.StatusCode, rr.HttpResponse.Headers, rr.HttpResponse.Body)
		}
	}
}

func (rr *RequestResource) SetError(err error) SetRequestStatusFunc {
	return func() {
		rr.StatusWriter.SetError(err)
	}
}

func (rr *RequestResource) ResetFailures() SetRequestStatusFunc {
	return func() {
		if resetter, ok := rr.StatusWriter.(interfaces.RequestStatusWriter); ok {
			resetter.ResetFailures()
		}
	}
}

// SetRequestResourceStatus sets the status of a resource, retrying on optimistic-lock
// conflicts instead of failing the reconcile (the external call has typically already
// succeeded by this point).
//
// Callers are expected to Get the latest resource before calling; the first attempt
// writes against that state. On a conflict the resource is re-fetched and the setters
// re-apply against server state — caller mutations applied after the caller's Get and
// not expressed as setters (e.g. conditions marked in the Observe path) are dropped
// from that write and re-applied by the next reconcile.
func SetRequestResourceStatus(rr RequestResource, statusFuncs ...SetRequestStatusFunc) error {
	nn := types.NamespacedName{Name: rr.Resource.GetName(), Namespace: rr.Resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		for _, updateStatusFunc := range statusFuncs {
			updateStatusFunc()
		}

		err := rr.LocalClient.Status().Update(rr.RequestContext, rr.Resource)
		if !apierrors.IsConflict(err) {
			return err
		}

		// Re-fetch into the same pointer so the setters re-apply against server state
		// on the next attempt — relative setters (e.g. Status.Failed++) must not stack
		// on top of the failed attempt. The refresh also runs on the final (exhausted)
		// attempt deliberately: it leaves the shared object with server state and a
		// fresh resourceVersion for the reconciler's subsequent status write.
		if getErr := rr.LocalClient.Get(rr.RequestContext, nn, rr.Resource); getErr != nil {
			return errors.Wrap(getErr, "failed to re-get resource after status update conflict")
		}
		return err
	})
}
