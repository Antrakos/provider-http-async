package utils

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1_request "github.com/Antrakos/provider-http-async/apis/cluster/request/v1alpha2"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"

	"github.com/crossplane/crossplane-runtime/v2/pkg/test"
	"github.com/google/go-cmp/cmp"
)

var (
	errBoom = errors.New("boom")
)

var (
	testPostMapping = v1alpha1_request.Mapping{
		Method: "POST",
		Body:   "{ username: .payload.body.username, email: .payload.body.email }",
		URL:    ".payload.baseUrl",
	}

	testPutMapping = v1alpha1_request.Mapping{
		Method: "PUT",
		Body:   "{ username: \"john_doe_new_username\" }",
		URL:    "(.payload.baseUrl + \"/\" + .response.body.id)",
	}

	testGetMapping = v1alpha1_request.Mapping{
		Method: "GET",
		URL:    "(.payload.baseUrl + \"/\" + .response.body.id)",
	}

	testDeleteMapping = v1alpha1_request.Mapping{
		Method: "DELETE",
		URL:    "(.payload.baseUrl + \"/\" + .response.body.id)",
	}
)

var (
	testRequestForProvider = v1alpha1_request.AsyncRequestParameters{
		Payload: v1alpha1_request.Payload{
			Body:    "{\"username\": \"john_doe\", \"email\": \"john.doe@example.com\"}",
			BaseUrl: "https://api.example.com/users",
		},
		Mappings: []v1alpha1_request.Mapping{
			testPostMapping,
			testGetMapping,
			testPutMapping,
			testDeleteMapping,
		},
	}

	testRequestCr = &v1alpha1_request.AsyncRequest{
		Spec: v1alpha1_request.AsyncRequestSpec{
			ForProvider: testRequestForProvider,
		},
		Status: v1alpha1_request.AsyncRequestStatus{
			Failed: int32(3),
		},
	}

	testRequestResource = RequestResource{
		StatusWriter:   testRequestCr,
		Resource:       testRequestCr,
		RequestContext: context.Background(),
		HttpResponse: httpClient.HttpResponse{
			StatusCode: 200,
			Body:       `{"ids":"123","username":"john_doe"}`,
		},
		HttpRequest: httpClient.HttpRequest{
			Method: "GET",
			URL:    "https://example",
		},
		LocalClient: &test.MockClient{
			MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
		},
	}
)

func Test_SetRequestResourceStatus(t *testing.T) {
	type args struct {
		rr          RequestResource
		statusFuncs []SetRequestStatusFunc
	}
	type want struct {
		failures int32
		err      error
	}
	cases := map[string]struct {
		args args
		want want
	}{
		"Success": {
			args: args{
				rr: testRequestResource,
				statusFuncs: []SetRequestStatusFunc{
					testRequestResource.SetBody(),
					testRequestResource.SetRequestDetails(),
					testRequestResource.SetHeaders(),
					testRequestResource.SetStatusCode(),
					testRequestResource.ResetFailures(),
					testRequestResource.SetCache(),
				},
			},
			want: want{
				failures: 0,
				err:      nil,
			},
		},
		"SetError": {
			args: args{
				rr: testRequestResource,
				statusFuncs: []SetRequestStatusFunc{
					testRequestResource.SetBody(),
					testRequestResource.SetRequestDetails(),
					testRequestResource.SetHeaders(),
					testRequestResource.SetStatusCode(),
					testRequestResource.ResetFailures(),
					testRequestResource.SetCache(),
					testRequestResource.SetError(errBoom),
				},
			},
			want: want{
				failures: 1,
				err:      nil,
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gotErr := SetRequestResourceStatus(tc.args.rr, tc.args.statusFuncs...)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("SetRequestResourceStatus(...): -want error, +got error: %s", diff)
			}

			if diff := cmp.Diff(tc.args.rr.HttpResponse.Body, testRequestCr.Status.Response.Body); diff != "" {
				t.Fatalf("SetRequestResourceStatus(...): -want response body, +got response body: %s", diff)
			}

			if diff := cmp.Diff(tc.args.rr.HttpResponse.Headers, testRequestCr.Status.Response.Headers); diff != "" {
				t.Fatalf("SetRequestResourceStatus(...): -want response headers, +got response headers: %s", diff)
			}

			if diff := cmp.Diff(tc.args.rr.HttpResponse.StatusCode, testRequestCr.Status.Response.StatusCode); diff != "" {
				t.Fatalf("SetRequestResourceStatus(...): -want response status code, +got response status code: %s", diff)
			}

			if diff := cmp.Diff(tc.args.rr.HttpResponse.StatusCode, testRequestCr.Status.Cache.Response.StatusCode); diff != "" {
				t.Fatalf("SetRequestResourceStatus(...): -want cache status code, +got cache status code: %s", diff)
			}

			if diff := cmp.Diff(tc.args.rr.HttpResponse.Body, testRequestCr.Status.Cache.Response.Body); diff != "" {
				t.Fatalf("SetRequestResourceStatus(...): -want cache body, +got cache body: %s", diff)
			}

			if diff := cmp.Diff(tc.args.rr.HttpResponse.Headers, testRequestCr.Status.Cache.Response.Headers); diff != "" {
				t.Fatalf("SetRequestResourceStatus(...): -want cache headers, +got cache headers: %s", diff)
			}

			if diff := cmp.Diff(tc.want.failures, testRequestCr.Status.Failed); diff != "" {
				t.Fatalf("SetRequestResourceStatus(...): -want failures amount, +got failures amount: %s", diff)
			}

			if diff := cmp.Diff(tc.args.rr.HttpRequest.Method, testRequestCr.Status.RequestDetails.Method); diff != "" {
				t.Fatalf("SetRequestResourceStatus(...): -want request method, +got request method: %s", diff)
			}

			if diff := cmp.Diff(tc.args.rr.HttpRequest.URL, testRequestCr.Status.RequestDetails.URL); diff != "" {
				t.Fatalf("SetRequestResourceStatus(...): -want request url, +got request url: %s", diff)
			}
		})
	}
}

// Test_SetRequestResourceStatus_ConflictRetried verifies a status write that hits an
// optimistic-lock conflict is retried after re-fetching the resource, and that relative
// setters (SetError increments Status.Failed) land exactly once against the refreshed
// object rather than stacking on the failed attempt.
func Test_SetRequestResourceStatus_ConflictRetried(t *testing.T) {
	conflictErr := apierrors.NewConflict(schema.GroupResource{Resource: "asyncrequests"}, "test-request", errBoom)
	updateAttempts := 0

	cr := &v1alpha1_request.AsyncRequest{
		Status: v1alpha1_request.AsyncRequestStatus{Failed: int32(5)},
	}
	rr := RequestResource{
		StatusWriter:   cr,
		Resource:       cr,
		RequestContext: context.Background(),
		HttpResponse: httpClient.HttpResponse{
			StatusCode: 200,
			Body:       `{"id":"123"}`,
		},
		HttpRequest: httpClient.HttpRequest{
			Method: "POST",
			URL:    "https://example",
		},
		LocalClient: &test.MockClient{
			MockStatusUpdate: func(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
				updateAttempts++
				if updateAttempts == 1 {
					return conflictErr
				}
				return nil
			},
			// Simulate the server overwriting the shared object on re-fetch: the failed
			// attempt's Failed++ never persisted, so the baseline is still 5.
			MockGet: test.NewMockGetFn(nil, func(obj client.Object) error {
				obj.(*v1alpha1_request.AsyncRequest).Status.Failed = int32(5)
				return nil
			}),
		},
	}

	gotErr := SetRequestResourceStatus(rr, rr.SetBody(), rr.SetError(errBoom))
	if diff := cmp.Diff(nil, gotErr, test.EquateErrors()); diff != "" {
		t.Fatalf("SetRequestResourceStatus(...): -want error, +got error: %s", diff)
	}

	if updateAttempts != 2 {
		t.Fatalf("SetRequestResourceStatus(...): want 2 status update attempts (conflict then success), got %d", updateAttempts)
	}
	// Exactly one increment on top of the re-fetched baseline — not two.
	if diff := cmp.Diff(int32(6), cr.Status.Failed); diff != "" {
		t.Fatalf("SetRequestResourceStatus(...): -want failures, +got failures: %s", diff)
	}
	if diff := cmp.Diff(rr.HttpResponse.Body, cr.Status.Response.Body); diff != "" {
		t.Fatalf("SetRequestResourceStatus(...): -want response body, +got response body: %s", diff)
	}
}

// Test_SetRequestResourceStatus_ConflictRetriedFakeClient verifies the same conflict
// retry path against a real fake client, where the conflict arises from an actual
// resourceVersion mismatch after a simulated concurrent writer.
func Test_SetRequestResourceStatus_ConflictRetriedFakeClient(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1alpha1_request.SchemeBuilder.AddToScheme(scheme)

	cr := &v1alpha1_request.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-request", Namespace: "default"},
		Status:     v1alpha1_request.AsyncRequestStatus{Failed: int32(5)},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(cr).
		WithObjects(cr).
		Build()

	nn := types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}
	// Populate the shared object with the tracker-assigned resourceVersion.
	if err := fakeClient.Get(context.Background(), nn, cr); err != nil {
		t.Fatalf("failed to get initial resource: %v", err)
	}

	// Simulate a concurrent writer bumping the resourceVersion after rr was built.
	concurrent := &v1alpha1_request.AsyncRequest{}
	if err := fakeClient.Get(context.Background(), nn, concurrent); err != nil {
		t.Fatalf("failed to get resource for concurrent writer: %v", err)
	}
	concurrent.Status.Error = "concurrent-writer"
	if err := fakeClient.Status().Update(context.Background(), concurrent); err != nil {
		t.Fatalf("failed to simulate concurrent writer: %v", err)
	}

	rr := RequestResource{
		StatusWriter:   cr,
		Resource:       cr,
		RequestContext: context.Background(),
		HttpResponse: httpClient.HttpResponse{
			StatusCode: 200,
			Body:       `{"id":"123"}`,
		},
		HttpRequest: httpClient.HttpRequest{
			Method: "POST",
			URL:    "https://example",
		},
		LocalClient: fakeClient,
	}

	gotErr := SetRequestResourceStatus(rr, rr.SetBody(), rr.SetError(errBoom))
	if gotErr != nil {
		t.Fatalf("SetRequestResourceStatus(...): want no error after conflict retry, got: %v", gotErr)
	}

	server := &v1alpha1_request.AsyncRequest{}
	if err := fakeClient.Get(context.Background(), nn, server); err != nil {
		t.Fatalf("failed to get resource after retry: %v", err)
	}
	// Exactly one increment on top of the re-fetched baseline — not two.
	if diff := cmp.Diff(int32(6), server.Status.Failed); diff != "" {
		t.Fatalf("SetRequestResourceStatus(...): -want failures, +got failures: %s", diff)
	}
	if diff := cmp.Diff(rr.HttpResponse.Body, server.Status.Response.Body); diff != "" {
		t.Fatalf("SetRequestResourceStatus(...): -want response body, +got response body: %s", diff)
	}
}

// Test_SetRequestResourceStatus_ReGetAfterConflictFails verifies that when the resource
// cannot be re-fetched after a conflict, the re-get error is surfaced instead of being
// retried or swallowed.
func Test_SetRequestResourceStatus_ReGetAfterConflictFails(t *testing.T) {
	conflictErr := apierrors.NewConflict(schema.GroupResource{Resource: "asyncrequests"}, "test-request", errBoom)
	cr := &v1alpha1_request.AsyncRequest{}
	rr := RequestResource{
		StatusWriter:   cr,
		Resource:       cr,
		RequestContext: context.Background(),
		HttpResponse: httpClient.HttpResponse{
			StatusCode: 200,
			Body:       `{"id":"123"}`,
		},
		LocalClient: &test.MockClient{
			MockStatusUpdate: test.NewMockSubResourceUpdateFn(conflictErr),
			MockGet:          test.NewMockGetFn(errBoom),
		},
	}

	gotErr := SetRequestResourceStatus(rr, rr.SetBody())
	wantErr := errors.Wrap(errBoom, "failed to re-get resource after status update conflict")
	if diff := cmp.Diff(wantErr, gotErr, test.EquateErrors()); diff != "" {
		t.Fatalf("SetRequestResourceStatus(...): -want error, +got error: %s", diff)
	}
}

// Test_SetRequestResourceStatus_ConflictExhausted verifies that a conflict persisting
// through all retry attempts surfaces as a conflict error, and that the resource is
// re-fetched after every conflict (including the exhausted final attempt).
func Test_SetRequestResourceStatus_ConflictExhausted(t *testing.T) {
	conflictErr := apierrors.NewConflict(schema.GroupResource{Resource: "asyncrequests"}, "test-request", errBoom)
	updateAttempts := 0
	getCalls := 0

	cr := &v1alpha1_request.AsyncRequest{}
	rr := RequestResource{
		StatusWriter:   cr,
		Resource:       cr,
		RequestContext: context.Background(),
		HttpResponse: httpClient.HttpResponse{
			StatusCode: 200,
			Body:       `{"id":"123"}`,
		},
		LocalClient: &test.MockClient{
			MockStatusUpdate: func(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
				updateAttempts++
				return conflictErr
			},
			MockGet: test.NewMockGetFn(nil, func(_ client.Object) error {
				getCalls++
				return nil
			}),
		},
	}

	gotErr := SetRequestResourceStatus(rr, rr.SetBody())
	if !apierrors.IsConflict(gotErr) {
		t.Fatalf("SetRequestResourceStatus(...): want conflict error after exhausting retries, got: %v", gotErr)
	}
	if updateAttempts != 5 {
		t.Fatalf("SetRequestResourceStatus(...): want 5 update attempts (retry.DefaultRetry), got %d", updateAttempts)
	}
	if getCalls != 5 {
		t.Fatalf("SetRequestResourceStatus(...): want 5 re-gets (one per conflict), got %d", getCalls)
	}
}

// Test_SetRequestResourceStatus_NonConflictNotRetried verifies that a non-conflict
// status write error surfaces immediately: one update attempt and no re-fetch —
// pinning the apierrors.IsConflict gate.
func Test_SetRequestResourceStatus_NonConflictNotRetried(t *testing.T) {
	notFoundErr := apierrors.NewNotFound(schema.GroupResource{Resource: "asyncrequests"}, "test-request")
	updateAttempts := 0
	getCalls := 0

	cr := &v1alpha1_request.AsyncRequest{}
	rr := RequestResource{
		StatusWriter:   cr,
		Resource:       cr,
		RequestContext: context.Background(),
		HttpResponse: httpClient.HttpResponse{
			StatusCode: 200,
			Body:       `{"id":"123"}`,
		},
		LocalClient: &test.MockClient{
			MockStatusUpdate: func(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
				updateAttempts++
				return notFoundErr
			},
			MockGet: test.NewMockGetFn(nil, func(_ client.Object) error {
				getCalls++
				return nil
			}),
		},
	}

	gotErr := SetRequestResourceStatus(rr, rr.SetBody())
	if diff := cmp.Diff(notFoundErr, gotErr, test.EquateErrors()); diff != "" {
		t.Fatalf("SetRequestResourceStatus(...): -want error, +got error: %s", diff)
	}
	if updateAttempts != 1 {
		t.Fatalf("SetRequestResourceStatus(...): want 1 update attempt for non-conflict error, got %d", updateAttempts)
	}
	if getCalls != 0 {
		t.Fatalf("SetRequestResourceStatus(...): want 0 re-gets for non-conflict error, got %d", getCalls)
	}
}
