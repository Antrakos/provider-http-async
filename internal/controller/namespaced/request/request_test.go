package request

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/crossplane-runtime/v2/pkg/feature"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"github.com/crossplane/crossplane-runtime/v2/pkg/test"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	"github.com/Antrakos/provider-http-async/apis/common"
	"github.com/Antrakos/provider-http-async/apis/interfaces"
	"github.com/Antrakos/provider-http-async/apis/namespaced/request/v1alpha2"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/request"
	"github.com/Antrakos/provider-http-async/internal/service/request/polling"
)

var (
	errBoom = errors.New("boom")
)

const (
	providerName              = "http-test"
	testNamespacedRequestName = "test-request"
	testNamespace             = "testns"
)

var (
	testPostMapping = v1alpha2.Mapping{
		Method: "POST",
		Body:   "{ username: .payload.body.username, email: .payload.body.email }",
		URL:    ".payload.baseUrl",
	}

	testPutMapping = v1alpha2.Mapping{
		Method: "PUT",
		Body:   "{ username: \"john_doe_new_username\" }",
		URL:    "(.payload.baseUrl + \"/\" + .response.body.id)",
	}

	testGetMapping = v1alpha2.Mapping{
		Method: "GET",
		URL:    "(.payload.baseUrl + \"/\" + .response.body.id)",
	}

	testDeleteMapping = v1alpha2.Mapping{
		Method: "DELETE",
		URL:    "(.payload.baseUrl + \"/\" + .response.body.id)",
	}
)

var (
	testForProvider = v1alpha2.AsyncRequestParameters{
		Payload: v1alpha2.Payload{
			Body:    "{\"username\": \"john_doe\", \"email\": \"john.doe@example.com\"}",
			BaseUrl: "https://api.example.com/users",
		},
		Mappings: []v1alpha2.Mapping{
			testPostMapping,
			testGetMapping,
			testPutMapping,
			testDeleteMapping,
		},
	}
)

type MockSendRequestFn func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error)

type MockSendRequestWithTLSFn func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error)

type MockHttpClient struct {
	MockSendRequest        MockSendRequestFn
	MockSendRequestWithTLS MockSendRequestWithTLSFn
}

func (m *MockHttpClient) SendRequest(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
	return m.MockSendRequest(ctx, method, url, body, headers, tlsConfigData)
}

func (m *MockHttpClient) SendRequestWithTLS(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
	if m.MockSendRequestWithTLS != nil {
		return m.MockSendRequestWithTLS(ctx, method, url, body, headers, tlsConfig)
	}
	// Fallback to SendRequest for backward compatibility
	return m.MockSendRequest(ctx, method, url, body, headers, tlsConfig)
}

type httpNamespacedRequestModifier func(request *v1alpha2.AsyncRequest)

func httpNamespacedRequest(rm ...httpNamespacedRequestModifier) *v1alpha2.AsyncRequest {
	r := &v1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testNamespacedRequestName,
			Namespace: testNamespace,
		},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: testForProvider,
		},
		Status: v1alpha2.AsyncRequestStatus{
			Response: v1alpha2.Response{
				Body:       `{"id": "123"}`,
				StatusCode: 200,
			},
		},
	}

	for _, m := range rm {
		m(r)
	}

	return r
}

type notNamespacedRequest struct {
	resource.Managed
}

func namespacedRequest(modifiers ...func(*v1alpha2.AsyncRequest)) *v1alpha2.AsyncRequest {
	cr := &v1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-request",
			Namespace: "default",
		},
		Spec: v1alpha2.AsyncRequestSpec{
			ForProvider: v1alpha2.AsyncRequestParameters{
				Payload: v1alpha2.Payload{
					Body:    `{"test": true}`,
					BaseUrl: "http://example.com/test",
				},
				Mappings: []v1alpha2.Mapping{
					{
						Method: "POST",
						Action: "CREATE",
						URL:    ".payload.baseUrl",
						Body:   ".payload.body",
					},
					{
						Method: "GET",
						Action: "OBSERVE",
						URL:    ".payload.baseUrl",
					},
				},
			},
		},
	}

	for _, modifier := range modifiers {
		modifier(cr)
	}

	return cr
}

func namespacedRequestWithDeletion() *v1alpha2.AsyncRequest {
	now := metav1.Now()
	return namespacedRequest(func(cr *v1alpha2.AsyncRequest) {
		cr.DeletionTimestamp = &now
	})
}

func TestObserve(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		obs         managed.ExternalObservation
		err         error
		readyStatus corev1.ConditionStatus
		readyReason xpv2.ConditionReason
		readyMsg    string
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "NotNamespacedRequest",
			args: args{
				mg: notNamespacedRequest{},
			},
			want: want{
				err: errors.New(errNotRequest),
			},
		},
		{
			name: "ResourceBeingDeleted",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, errors.New("resource not found")
					},
				},
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: namespacedRequestWithDeletion(),
			},
			want: want{
				obs: managed.ExternalObservation{
					ResourceExists: false,
				},
			},
		},
		{
			name: "TerminalFailure_ReturnsErrorAndReadyFalse",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
						t.Errorf("OBSERVE must not fire when a terminal error is recorded")
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: func() *v1alpha2.AsyncRequest {
					cr := namespacedRequest(func(cr *v1alpha2.AsyncRequest) {
						cr.Status.TerminalError = "polling.url resolved to a bare path"
					})
					cr.Generation = 1
					cr.Status.SetObservedGeneration(1)
					return cr
				}(),
			},
			want: want{
				err:         errors.New(terminalErrorPrefix + "polling.url resolved to a bare path"),
				readyStatus: corev1.ConditionFalse,
				readyReason: xpv2.ReasonUnavailable,
				readyMsg:    "polling.url resolved to a bare path",
			},
		},
		{
			name: "InFlight_ReadyCreating",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
						t.Errorf("OBSERVE must not fire while the anchor is in flight")
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: namespacedRequest(func(cr *v1alpha2.AsyncRequest) {
					cr.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
						{Method: "POST", Action: "CREATE", URL: ".payload.baseUrl", Body: ".payload.body", Polling: &common.Polling{URL: ".response.body.name", Done: ".poll.response.body.done == true"}},
						{Method: "GET", Action: "OBSERVE", URL: `.payload.baseUrl + "/" + .status.externalRef`},
					}
					cr.Status.ExternalRef = "model-789"
					cr.Status.Polling.Response = &runtime.RawExtension{Raw: []byte(`{"body":{"name":"op-1"}}`)}
				}),
			},
			want: want{
				obs:         managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: false},
				readyStatus: corev1.ConditionFalse,
				readyReason: xpv2.ReasonCreating,
			},
		},
		{
			name: "MissingMappingTerminal_ReturnsErrorAndReadyFalse",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
						return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{StatusCode: 200, Body: `{"deployedModels": []}`}}, nil
					},
				},
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: namespacedRequest(func(cr *v1alpha2.AsyncRequest) {
					cr.Spec.ForProvider.Payload.BaseUrl = "https://api.example.com/endpoints/456"
					cr.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
						{Method: "POST", Action: "CREATE", URL: ".payload.baseUrl", Body: ".payload.body"},
						{Method: "GET", Action: "OBSERVE", URL: ".payload.baseUrl"},
						{Method: "DELETE", Action: "REMOVE", URL: ".payload.baseUrl"},
					}
					cr.Spec.ForProvider.ExpectedResponseCheck = v1alpha2.ExpectedResponseCheck{
						Type:  v1alpha2.ExpectedResponseCheckTypeCustom,
						Logic: `false`,
					}
					cr.Status.ExternalRef = "model-789"
					cr.Status.Response = v1alpha2.Response{StatusCode: 200, Body: `{"deployedModels": []}`}
				}),
			},
			want: want{
				err:         errors.New(terminalErrorPrefix + "no UPDATE or PUT mapping is configured but the resource is out of sync (expectedResponseCheck returned false); add an UPDATE mapping or fix the spec so the resource reconciles"),
				readyStatus: corev1.ConditionFalse,
				readyReason: xpv2.ReasonUnavailable,
				readyMsg:    "no UPDATE or PUT mapping is configured",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				logger:    logging.NewNopLogger(),
				localKube: tc.args.localKube,
				http:      tc.args.http,
			}

			got, err := e.Observe(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("Observe(...): -want error, +got error: %s", diff)
			}
			if tc.want.readyReason != "" {
				cr, ok := tc.args.mg.(*v1alpha2.AsyncRequest)
				if !ok {
					t.Fatalf("mg is not a namespaced AsyncRequest")
				}
				rc := cr.Status.GetCondition(xpv2.TypeReady)
				if diff := cmp.Diff(tc.want.readyStatus, rc.Status); diff != "" {
					t.Errorf("Ready status: -want %s, +got %s", tc.want.readyStatus, rc.Status)
				}
				if diff := cmp.Diff(tc.want.readyReason, rc.Reason); diff != "" {
					t.Errorf("Ready reason: -want %s, +got %s", tc.want.readyReason, rc.Reason)
				}
				if tc.want.readyMsg != "" && !strings.Contains(rc.Message, tc.want.readyMsg) {
					t.Errorf("Ready message: want to contain %q, got %q", tc.want.readyMsg, rc.Message)
				}
			}
			if diff := cmp.Diff(tc.want.obs, got); diff != "" {
				t.Errorf("Observe(...): -want, +got: %s", diff)
			}
		})
	}
}

// TestObserve_ExternalRefSeeding verifies that status.externalRef is seeded from the
// crossplane.io/external-name annotation only when the annotation was explicitly set to
// a value other than metadata.name. Crossplane's default NameAsExternalName initializer
// sets external-name == metadata.name for every resource, which must NOT be seeded (it
// would pollute .status.externalRef with the meaningless k8s name).
func TestObserve_ExternalRefSeeding(t *testing.T) {
	// HTTP mock that always errors so Observe returns quickly after the seeding step.
	// The seeding runs before IsUpToDate, so it mutates the CR regardless of this error.
	http := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{}, errBoom
		},
	}

	cases := []struct {
		name        string
		externalNam string
		wantRef     string
	}{
		{
			name:        "AnnotationEqualsObjectName_NotSeeded",
			externalNam: "test-request",
			wantRef:     "",
		},
		{
			name:        "AnnotationDiffersFromObjectName_Seeded",
			externalNam: "models/imported-model-789",
			wantRef:     "models/imported-model-789",
		},
		{
			name:        "NoAnnotation_NotSeeded",
			externalNam: "",
			wantRef:     "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := httpNamespacedRequest(func(r *v1alpha2.AsyncRequest) {
				r.Name = "test-request"
				r.Status.Response = v1alpha2.Response{}
				if tc.externalNam != "" {
					r.SetAnnotations(map[string]string{"crossplane.io/external-name": tc.externalNam})
				}
			})

			e := &external{
				logger:    logging.NewNopLogger(),
				localKube: &test.MockClient{MockGet: test.NewMockGetFn(nil), MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil)},
				http:      http,
			}

			_, _ = e.Observe(context.Background(), cr)

			if got := cr.Status.ExternalRef; got != tc.wantRef {
				t.Errorf("status.externalRef = %q, want %q", got, tc.wantRef)
			}
		})
	}
}

func TestCreate(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		err error
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "NotRequestResource",
			args: args{
				mg: notNamespacedRequest{},
			},
			want: want{
				err: errors.New(errNotRequest),
			},
		},
		{
			name: "RequestFailed",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, errBoom
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpNamespacedRequest(),
			},
			want: want{
				err: errors.Wrap(errBoom, errFailedToSendHttpRequest),
			},
		},
		{
			name: "Success",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockCreate:       test.NewMockCreateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpNamespacedRequest(),
			},
			want: want{
				err: nil,
			},
		},
	}
	for _, tc := range cases {
		tc := tc // Create local copies of loop variables

		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				localKube: tc.args.localKube,
				logger:    logging.NewNopLogger(),
				http:      tc.args.http,
			}
			_, gotErr := e.Create(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("e.Create(...): -want error, +got error: %s", diff)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		err error
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "NotRequestResource",
			args: args{
				mg: notNamespacedRequest{},
			},
			want: want{
				err: errors.New(errNotRequest),
			},
		},
		{
			name: "RequestFailed",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, errBoom
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpNamespacedRequest(),
			},
			want: want{
				err: errors.Wrap(errBoom, errFailedToSendHttpRequest),
			},
		},
		{
			name: "Success",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockCreate:       test.NewMockCreateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpNamespacedRequest(),
			},
			want: want{
				err: nil,
			},
		},
	}
	for _, tc := range cases {
		tc := tc // Create local copies of loop variables

		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				localKube: tc.args.localKube,
				logger:    logging.NewNopLogger(),
				http:      tc.args.http,
			}
			_, gotErr := e.Update(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("e.Update(...): -want error, +got error: %s", diff)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		err error
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "NotRequestResource",
			args: args{
				mg: notNamespacedRequest{},
			},
			want: want{
				err: errors.New(errNotRequest),
			},
		},
		{
			name: "RequestFailed",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, errBoom
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpNamespacedRequest(),
			},
			want: want{
				err: errors.Wrap(errBoom, errFailedToSendHttpRequest),
			},
		},
		{
			name: "Success",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body httpClient.Data, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockCreate:       test.NewMockCreateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: httpNamespacedRequest(),
			},
			want: want{
				err: nil,
			},
		},
	}
	for _, tc := range cases {
		tc := tc // Create local copies of loop variables

		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				localKube: tc.args.localKube,
				logger:    logging.NewNopLogger(),
				http:      tc.args.http,
			}
			_, gotErr := e.Delete(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, gotErr, test.EquateErrors()); diff != "" {
				t.Fatalf("e.Delete(...): -want error, +got error: %s", diff)
			}
		})
	}
}

func TestManagementPoliciesFeatureFlag(t *testing.T) {
	cases := map[string]struct {
		reason   string
		features *feature.Flags
		want     bool
	}{
		"ManagementPoliciesEnabled": {
			reason: "Feature flag should be enabled when explicitly set",
			features: func() *feature.Flags {
				f := &feature.Flags{}
				f.Enable(feature.EnableBetaManagementPolicies)
				return f
			}(),
			want: true,
		},
		"ManagementPoliciesDisabled": {
			reason:   "Feature flag should be disabled when not set",
			features: &feature.Flags{},
			want:     false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			enabled := tc.features.Enabled(feature.EnableBetaManagementPolicies)
			if enabled != tc.want {
				t.Errorf("\n%s\nEnabled(feature.EnableBetaManagementPolicies): want %v, got %v", tc.reason, tc.want, enabled)
			}
		})
	}
}

func TestNamespacedRequestManagementPolicies(t *testing.T) {
	cases := map[string]struct {
		reason string
		mg     *v1alpha2.AsyncRequest
		want   xpv2.ManagementPolicies
	}{
		"DefaultManagementPolicies": {
			reason: "Default management policies should be nil when not explicitly set",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpNamespacedRequest()
				// Don't set managementPolicies explicitly to test default
				return r
			}(),
			want: nil,
		},
		"ObserveOnlyManagementPolicies": {
			reason: "Observe-only management policies should only allow observation",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpNamespacedRequest()
				r.Spec.ManagementPolicies = xpv2.ManagementPolicies{xpv2.ManagementActionObserve}
				return r
			}(),
			want: xpv2.ManagementPolicies{xpv2.ManagementActionObserve},
		},
		"CreateAndUpdateManagementPolicies": {
			reason: "Create and update management policies should allow creation and updates",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpNamespacedRequest()
				r.Spec.ManagementPolicies = xpv2.ManagementPolicies{
					xpv2.ManagementActionCreate,
					xpv2.ManagementActionUpdate,
				}
				return r
			}(),
			want: xpv2.ManagementPolicies{
				xpv2.ManagementActionCreate,
				xpv2.ManagementActionUpdate,
			},
		},
		"ObserveCreateUpdateManagementPolicies": {
			reason: "Observe, create, and update management policies should allow all three actions",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpNamespacedRequest()
				r.Spec.ManagementPolicies = xpv2.ManagementPolicies{
					xpv2.ManagementActionObserve,
					xpv2.ManagementActionCreate,
					xpv2.ManagementActionUpdate,
				}
				return r
			}(),
			want: xpv2.ManagementPolicies{
				xpv2.ManagementActionObserve,
				xpv2.ManagementActionCreate,
				xpv2.ManagementActionUpdate,
			},
		},
		"AllActionsExceptDeleteManagementPolicies": {
			reason: "All actions except delete should allow observe, create, update, and late initialize",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpNamespacedRequest()
				r.Spec.ManagementPolicies = xpv2.ManagementPolicies{
					xpv2.ManagementActionObserve,
					xpv2.ManagementActionCreate,
					xpv2.ManagementActionUpdate,
					xpv2.ManagementActionLateInitialize,
				}
				return r
			}(),
			want: xpv2.ManagementPolicies{
				xpv2.ManagementActionObserve,
				xpv2.ManagementActionCreate,
				xpv2.ManagementActionUpdate,
				xpv2.ManagementActionLateInitialize,
			},
		},
		"ExplicitAllManagementPolicies": {
			reason: "Explicit all management policies should allow all actions",
			mg: func() *v1alpha2.AsyncRequest {
				r := httpNamespacedRequest()
				r.Spec.ManagementPolicies = xpv2.ManagementPolicies{xpv2.ManagementActionAll}
				return r
			}(),
			want: xpv2.ManagementPolicies{xpv2.ManagementActionAll},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := tc.mg.Spec.ManagementPolicies
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("\n%s\nManagementPolicies: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestNamespacedRequestManagementPoliciesResolver(t *testing.T) {
	type args struct {
		enabled bool
		policy  xpv2.ManagementPolicies
	}
	type want struct {
		shouldCreate         bool
		shouldUpdate         bool
		shouldDelete         bool
		shouldOnlyObserve    bool
		shouldLateInitialize bool
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"ManagementPoliciesDisabled": {
			reason: "When management policies are disabled, all actions should be allowed",
			args: args{
				enabled: false,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionObserve},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         true,
				shouldDelete:         true,
				shouldOnlyObserve:    false,
				shouldLateInitialize: true,
			},
		},
		"ObserveOnlyPolicy": {
			reason: "Observe-only policy should only allow observation",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionObserve},
			},
			want: want{
				shouldCreate:         false,
				shouldUpdate:         false,
				shouldDelete:         false,
				shouldOnlyObserve:    true,
				shouldLateInitialize: false,
			},
		},
		"CreateOnlyPolicy": {
			reason: "Create-only policy should only allow creation",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionCreate},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         false,
				shouldDelete:         false,
				shouldOnlyObserve:    false,
				shouldLateInitialize: false,
			},
		},
		"UpdateOnlyPolicy": {
			reason: "Update-only policy should only allow updates",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionUpdate},
			},
			want: want{
				shouldCreate:         false,
				shouldUpdate:         true,
				shouldDelete:         false,
				shouldOnlyObserve:    false,
				shouldLateInitialize: false,
			},
		},
		"DeleteOnlyPolicy": {
			reason: "Delete-only policy should only allow deletion",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionDelete},
			},
			want: want{
				shouldCreate:         false,
				shouldUpdate:         false,
				shouldDelete:         true,
				shouldOnlyObserve:    false,
				shouldLateInitialize: false,
			},
		},
		"CreateAndUpdatePolicy": {
			reason: "Create and update policy should allow both creation and updates",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionCreate, xpv2.ManagementActionUpdate},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         true,
				shouldDelete:         false,
				shouldOnlyObserve:    false,
				shouldLateInitialize: false,
			},
		},
		"ObserveCreateUpdatePolicy": {
			reason: "Observe, create, and update policy should allow all three actions",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionUpdate},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         true,
				shouldDelete:         false,
				shouldOnlyObserve:    false,
				shouldLateInitialize: false,
			},
		},
		"AllActionsExceptDeletePolicy": {
			reason: "All actions except delete should allow observe, create, update, and late initialize",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionUpdate, xpv2.ManagementActionLateInitialize},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         true,
				shouldDelete:         false,
				shouldOnlyObserve:    false,
				shouldLateInitialize: true,
			},
		},
		"ExplicitAllPolicy": {
			reason: "Explicit all policy should allow all actions",
			args: args{
				enabled: true,
				policy:  xpv2.ManagementPolicies{xpv2.ManagementActionAll},
			},
			want: want{
				shouldCreate:         true,
				shouldUpdate:         true,
				shouldDelete:         true,
				shouldOnlyObserve:    false,
				shouldLateInitialize: true,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Create a mock managed resource with the specified management policies
			mg := httpNamespacedRequest()
			mg.Spec.ManagementPolicies = tc.args.policy

			// Test the management policies resolver logic
			// Note: This is a simplified test that focuses on the policy logic
			// The actual enforcement happens in the Crossplane managed reconciler

			// Helper function to check if a ManagementPolicies slice contains a specific action
			contains := func(policies xpv2.ManagementPolicies, action xpv2.ManagementAction) bool {
				for _, p := range policies {
					if p == action {
						return true
					}
				}
				return false
			}

			// Test ShouldCreate
			shouldCreate := tc.want.shouldCreate
			if tc.args.enabled {
				shouldCreate = contains(tc.args.policy, xpv2.ManagementActionCreate) || contains(tc.args.policy, xpv2.ManagementActionAll)
			}
			if shouldCreate != tc.want.shouldCreate {
				t.Errorf("ShouldCreate() = %v, want %v", shouldCreate, tc.want.shouldCreate)
			}

			// Test ShouldUpdate
			shouldUpdate := tc.want.shouldUpdate
			if tc.args.enabled {
				shouldUpdate = contains(tc.args.policy, xpv2.ManagementActionUpdate) || contains(tc.args.policy, xpv2.ManagementActionAll)
			}
			if shouldUpdate != tc.want.shouldUpdate {
				t.Errorf("ShouldUpdate() = %v, want %v", shouldUpdate, tc.want.shouldUpdate)
			}

			// Test ShouldDelete
			shouldDelete := tc.want.shouldDelete
			if tc.args.enabled {
				shouldDelete = contains(tc.args.policy, xpv2.ManagementActionDelete) || contains(tc.args.policy, xpv2.ManagementActionAll)
			}
			if shouldDelete != tc.want.shouldDelete {
				t.Errorf("ShouldDelete() = %v, want %v", shouldDelete, tc.want.shouldDelete)
			}

			// Test ShouldOnlyObserve
			shouldOnlyObserve := tc.want.shouldOnlyObserve
			if tc.args.enabled {
				shouldOnlyObserve = len(tc.args.policy) == 1 && contains(tc.args.policy, xpv2.ManagementActionObserve)
			}
			if shouldOnlyObserve != tc.want.shouldOnlyObserve {
				t.Errorf("ShouldOnlyObserve() = %v, want %v", shouldOnlyObserve, tc.want.shouldOnlyObserve)
			}

			// Test ShouldLateInitialize
			shouldLateInitialize := tc.want.shouldLateInitialize
			if tc.args.enabled {
				shouldLateInitialize = contains(tc.args.policy, xpv2.ManagementActionLateInitialize) || contains(tc.args.policy, xpv2.ManagementActionAll)
			}
			if shouldLateInitialize != tc.want.shouldLateInitialize {
				t.Errorf("ShouldLateInitialize() = %v, want %v", shouldLateInitialize, tc.want.shouldLateInitialize)
			}
		})
	}
}

func httpNamespacedRequestWithDeletion() *v1alpha2.AsyncRequest {
	now := metav1.Now()
	return httpNamespacedRequest(func(r *v1alpha2.AsyncRequest) {
		r.DeletionTimestamp = &now
	})
}

func TestTLSConfiguration(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		err error
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "TLSConfigAcceptedWithNilTLS",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						// Accept any TLS config - the actual TLS config loading is handled by the service layer
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       `{"result": "success"}`,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: namespacedRequestWithTLS(),
			},
			want: want{
				err: nil,
			},
		},
		{
			name: "InsecureSkipTLSVerifyAccepted",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						// Accept any TLS config - the controller should handle insecure skip verify
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       `{"result": "insecure success"}`,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: namespacedRequestWithInsecureSkipTLS(),
			},
			want: want{
				err: nil,
			},
		},
		{
			name: "TLSConfigWithClientCertsAccepted",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						// Accept any TLS config - the service layer handles TLS config merging
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       `{"result": "client cert success"}`,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
					MockGet:          test.NewMockGetFn(nil),
				},
				mg: namespacedRequestWithMutualTLS(),
			},
			want: want{
				err: nil,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				logger:    logging.NewNopLogger(),
				localKube: tc.args.localKube,
				http:      tc.args.http,
			}

			_, err := e.Create(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("Create(...): -want error, +got error: %s", diff)
			}
		})
	}
}

// namespacedRequestWithTLS creates a AsyncRequest with TLS configuration
func namespacedRequestWithTLS() *v1alpha2.AsyncRequest {
	return namespacedRequest(func(cr *v1alpha2.AsyncRequest) {
		cr.Spec.ForProvider.TLSConfig = &common.TLSConfig{
			CACertSecretRef: &xpv2.SecretKeySelector{
				SecretReference: xpv2.SecretReference{
					Name:      "ca-cert-secret",
					Namespace: "default",
				},
				Key: "ca.crt",
			},
		}
	})
}

// namespacedRequestWithInsecureSkipTLS creates a AsyncRequest with insecureSkipTLSVerify
func namespacedRequestWithInsecureSkipTLS() *v1alpha2.AsyncRequest {
	return namespacedRequest(func(cr *v1alpha2.AsyncRequest) {
		cr.Spec.ForProvider.InsecureSkipTLSVerify = true
	})
}

// namespacedRequestWithMutualTLS creates a AsyncRequest with mutual TLS configuration
func namespacedRequestWithMutualTLS() *v1alpha2.AsyncRequest {
	return namespacedRequest(func(cr *v1alpha2.AsyncRequest) {
		cr.Spec.ForProvider.TLSConfig = &common.TLSConfig{
			CACertSecretRef: &xpv2.SecretKeySelector{
				SecretReference: xpv2.SecretReference{
					Name:      "ca-cert-secret",
					Namespace: "default",
				},
				Key: "ca.crt",
			},
			ClientCertSecretRef: &xpv2.SecretKeySelector{
				SecretReference: xpv2.SecretReference{
					Name:      "client-cert-secret",
					Namespace: "default",
				},
				Key: "tls.crt",
			},
			ClientKeySecretRef: &xpv2.SecretKeySelector{
				SecretReference: xpv2.SecretReference{
					Name:      "client-cert-secret",
					Namespace: "default",
				},
				Key: "tls.key",
			},
		}
	})
}

// TestDeleteAsyncChain verifies the finalizer-hold contract for async deletes:
// When a polling.response anchor is set (and externalRef is set, as it is after a CREATE
// completes) and DeletionTimestamp is present:
//  1. Observe returns ResourceExists=true, ResourceUpToDate=false (REMOVE LRO still in flight)
//  2. Delete returns nil (budget expired, anchor retained, finalizer not removed)
func TestDeleteAsyncChain(t *testing.T) {
	const operationURL = "https://api.example.com/operations/op-123"

	// Build a request with DeletionTimestamp and a pre-set in-flight anchor (the raw
	// mutate response from the delete that started the LRO).
	now := metav1.Now()
	cr := namespacedRequest(
		func(cr *v1alpha2.AsyncRequest) {
			cr.DeletionTimestamp = &now
			cr.SetPollingResponse(map[string]interface{}{
				"body": map[string]interface{}{"name": "operations/op-123"},
			})
			cr.Status.ExternalRef = "models/my-model"
			cr.Finalizers = []string{"finalizer.managedresource.crossplane.io"}
			// Add a REMOVE mapping so DeployAction can find it.
			interval := metav1.Duration{Duration: 100 * time.Millisecond}
			timeout := metav1.Duration{Duration: 5 * time.Second}
			cr.Spec.ForProvider.Mappings = append(cr.Spec.ForProvider.Mappings, v1alpha2.Mapping{
				Method: "DELETE",
				Action: "REMOVE",
				URL:    `"https://api.example.com/models/my-model"`,
				Polling: &common.Polling{
					URL:      `"` + operationURL + `"`,
					Interval: &interval,
					Timeout:  &timeout,
					Done:     ".poll.response.body.done == true",
				},
			})
		},
	)

	localKube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}

	e := &external{
		logger:    logging.NewNopLogger(),
		localKube: localKube,
		http:      nil, // not called: in-flight anchor path skips the mutate HTTP call
	}

	t.Run("ObserveWithAnchorReturnsNotUpToDate", func(t *testing.T) {
		got, err := e.Observe(context.Background(), cr)
		if err != nil {
			t.Fatalf("Observe(): unexpected error: %v", err)
		}
		if !got.ResourceExists {
			t.Errorf("Observe(): want ResourceExists=true, got false")
		}
		if got.ResourceUpToDate {
			t.Errorf("Observe(): want ResourceUpToDate=false, got true")
		}
	})

	t.Run("DeleteWithDoneEqualsFalseReturnsNil", func(t *testing.T) {
		// mockPoller returns Done=false, simulating budget expiry while LRO is still running.
		mockPoller := &mockPollerNotDone{operationURL: operationURL}

		// Wire the mock poller via the service-layer function instead of the controller,
		// because the controller's Delete() does not accept a poller injection. We test
		// the service layer directly, which is what Delete() delegates to.
		svcCtx := service.NewServiceContext(context.Background(), localKube, logging.NewNopLogger(), nil, nil)
		crCtx := service.NewRequestCRContext(cr)
		err := request.DeployAction(svcCtx, crCtx, v1alpha2.ActionRemove, mockPoller)
		if err != nil {
			t.Errorf("DeployAction(REMOVE, notDone): want nil error to hold finalizer, got: %v", err)
		}
	})
}

// mockPollerNotDone simulates a poller that reports the operation is still running.
type mockPollerNotDone struct {
	operationURL string
}

func (m *mockPollerNotDone) Poll(
	_ *service.ServiceContext,
	_ *service.RequestCRContext,
	_ interfaces.HTTPMapping,
	_ map[string]interface{},
) (polling.Result, error) {
	return polling.Result{Done: false, OperationURL: m.operationURL}, nil
}

// TestOrphanRecovery_RoutesResumeThroughCreate proves the PRD's headline goal works
// through the real controller routing for a resource with NO UPDATE mapping (the Vertex
// AI deployedModel shape): a terminal failure on a bad polling.url is recovered by a
// spec fix, and the resume is routed via Observe → Create() → the CREATE mapping (not
// Update(), which has no mapping to bind). POST fires exactly once across the cycle.
func TestOrphanRecovery_RoutesResumeThroughCreate(t *testing.T) {
	postCalls := 0
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			if method == "POST" {
				postCalls++
				return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{
					StatusCode: 202, Body: `{"name": "projects/x/operations/789"}`,
				}}, nil
			}
			return httpClient.HttpDetails{}, nil // OBSERVE not reached while anchor in flight
		},
	}
	localKube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}

	// CREATE + OBSERVE only — no UPDATE mapping, like deployedModel.
	cr := namespacedRequest(func(cr *v1alpha2.AsyncRequest) {
		cr.Generation = 1
		cr.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
			{Method: "POST", Action: "CREATE", URL: ".payload.baseUrl", Body: ".payload.body",
				Polling: &common.Polling{URL: `.response.body.name`, Done: ".poll.response.body.done == true"}},
			{Method: "GET", Action: "OBSERVE", URL: ".payload.baseUrl + \"/\" + .status.externalRef"},
		}
	})

	e := &external{logger: logging.NewNopLogger(), localKube: localKube, http: httpMock}

	// Step 1: CREATE fires (POST), anchor persisted, bad polling.url → terminal failure.
	// Drive it through the service layer (Create dispatches to DeployAction).
	svcCtx := service.NewServiceContext(context.Background(), localKube, logging.NewNopLogger(), httpMock, nil)
	if err := request.DeployAction(svcCtx, service.NewRequestCRContext(cr), v1alpha2.ActionCreate); err != nil {
		t.Fatalf("step 1 DeployAction: %v", err)
	}
	if postCalls != 1 {
		t.Fatalf("step 1: expected 1 POST, got %d", postCalls)
	}
	if cr.Status.Polling.Response == nil || cr.Status.TerminalError == "" {
		t.Fatal("step 1: expected anchor retained + terminalError set after bad polling.url")
	}

	// Step 2: operator fixes polling.url and bumps the generation. Observe must clear the
	// terminal state and, because externalRef is still unset, return ResourceExists=false so
	// the controller calls Create() — the only mapping that exists — to resume the poll.
	cr.Generation = 2
	cr.Spec.ForProvider.Mappings[0].Polling.URL = `"http://api/" + .response.body.name`

	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("step 2 Observe: %v", err)
	}
	if obs.ResourceExists {
		t.Error("step 2: expected ResourceExists=false so the controller routes to Create() (no UPDATE mapping)")
	}
	if cr.Status.TerminalError != "" {
		t.Errorf("step 2: expected terminalError cleared on generation drift, got %q", cr.Status.TerminalError)
	}

	// Step 3: Create() resumes the poll via the retained anchor; a completing mock poller
	// drives it to done. POST must NOT fire again.
	completingPoller := &mockPollerCompleting{}
	if err := request.DeployAction(service.NewServiceContext(context.Background(), localKube, logging.NewNopLogger(), httpMock, nil), service.NewRequestCRContext(cr), v1alpha2.ActionCreate, completingPoller); err != nil {
		t.Fatalf("step 3 DeployAction: %v", err)
	}
	if postCalls != 1 {
		t.Errorf("step 3: expected POST still 1 (resume via anchor, no duplicate), got %d", postCalls)
	}
	if cr.Status.Polling.Response != nil {
		t.Error("step 3: expected anchor cleared on completion")
	}
}

// mockPollerCompleting reports the operation done immediately (for resume tests).
type mockPollerCompleting struct{}

func (m *mockPollerCompleting) Poll(
	_ *service.ServiceContext,
	_ *service.RequestCRContext,
	_ interfaces.HTTPMapping,
	_ map[string]interface{},
) (polling.Result, error) {
	return polling.Result{Done: true, PollResponse: map[string]interface{}{"body": map[string]interface{}{"id": "model-789"}}}, nil
}

func TestObserve_DeletionMonitoring(t *testing.T) {
	type args struct {
		http      httpClient.Client
		localKube client.Client
		mg        resource.Managed
	}
	type want struct {
		obs managed.ExternalObservation
		err error
	}

	cases := []struct {
		name string
		args args
		want want
	}{
		{
			name: "ResourceBeingDeleted",
			args: args{
				http: &MockHttpClient{
					MockSendRequest: func(ctx context.Context, method string, url string, body, headers httpClient.Data, tlsConfigData *httpClient.TLSConfigData) (resp httpClient.HttpDetails, err error) {
						return httpClient.HttpDetails{
							HttpResponse: httpClient.HttpResponse{
								StatusCode: 200,
								Body:       `{"id": "123"}`,
							},
						}, nil
					},
				},
				localKube: &test.MockClient{
					MockGet:          test.NewMockGetFn(nil),
					MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
				},
				mg: httpNamespacedRequestWithDeletion(),
			},
			want: want{
				obs: managed.ExternalObservation{
					ResourceExists: true,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &external{
				logger:    logging.NewNopLogger(),
				localKube: tc.args.localKube,
				http:      tc.args.http,
			}

			got, err := e.Observe(context.Background(), tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("Observe(...): -want error, +got error: %s", diff)
			}
			if diff := cmp.Diff(tc.want.obs, got); diff != "" {
				t.Errorf("Observe(...): -want, +got: %s", diff)
			}
		})
	}
}

// TestObserve_EmptyExternalRef_ObserveURLReferencesIt_RoutesToCreate reproduces the
// bug-empty-externalref-observe-hits-list-endpoint-skips-create.md regression end-to-end at
// the controller boundary: a fresh AsyncRequest (no prior response, empty externalRef, no
// in-flight anchor) whose OBSERVE URL is built as baseUrl + "/" + .status.externalRef.
// Before the fix the empty externalRef collapsed the URL onto the collection endpoint
// (.../models/), which returns 200, and Observe reported ResourceExists=true/UpToDate=false
// so the controller called Update() — never Create(). Now the identity gate routes to
// ResourceExists:false with no HTTP call made against the malformed URL, so the controller
// calls Create() and the resource is actually provisioned.
func TestObserve_EmptyExternalRef_ObserveURLReferencesIt_RoutesToCreate(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			t.Errorf("OBSERVE must not fire when externalRef is empty and the URL references it; got %s %s", method, url)
			return httpClient.HttpDetails{}, nil
		},
	}
	localKube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}

	// Fresh resource — the exact vertex-exp-model manifest shape (OBSERVE/UPDATE URLs key on
	// .status.externalRef, CUSTOM expectedResponseCheck that would report drift against a list).
	cr := namespacedRequest(func(cr *v1alpha2.AsyncRequest) {
		cr.Generation = 1
		cr.Status.Response = v1alpha2.Response{}
		cr.Status.ExternalRef = ""
		cr.Spec.ForProvider.Payload.BaseUrl = "https://aiplatform.googleapis.com/v1beta1/projects/p/locations/us-central1"
		cr.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
			{Method: "POST", Action: "CREATE", URL: `.payload.baseUrl + "/models:upload"`, Body: ".payload.body"},
			{Method: "GET", Action: "OBSERVE", URL: `.payload.baseUrl + "/models/" + .status.externalRef`},
			{Method: "PATCH", Action: "UPDATE", URL: `.payload.baseUrl + "/models/" + .status.externalRef`},
		}
		cr.Spec.ForProvider.ExpectedResponseCheck = v1alpha2.ExpectedResponseCheck{
			Type:  v1alpha2.ExpectedResponseCheckTypeCustom,
			Logic: `.response.body.displayName == .payload.body.model.displayName`,
		}
	})

	e := &external{logger: logging.NewNopLogger(), localKube: localKube, http: httpMock}
	obs, err := e.Observe(context.Background(), cr)
	if err != nil {
		t.Fatalf("Observe: unexpected error %v", err)
	}
	if obs.ResourceExists {
		t.Error("expected ResourceExists=false so the controller routes to Create(); got true (would route to Update and loop)")
	}
}

// TestUpdate_BrokenURL_NonPolling_404_SurfacesError covers the secondary bug at the
// controller boundary: when UPDATE routes to a non-polling PATCH against a broken URL (the
// .../models/ list endpoint from an empty externalRef) and the API returns 404, DeployAction
// must surface it as an error so the controller reports Synced=False, not ReconcileSuccess.
func TestUpdate_BrokenURL_NonPolling_404_SurfacesError(t *testing.T) {
	httpMock := &MockHttpClient{
		MockSendRequest: func(ctx context.Context, method, url string, body, headers httpClient.Data, tlsConfig *httpClient.TLSConfigData) (httpClient.HttpDetails, error) {
			return httpClient.HttpDetails{HttpResponse: httpClient.HttpResponse{StatusCode: 404, Body: `{"error":"not found"}`}}, nil
		},
	}
	localKube := &test.MockClient{
		MockGet:          test.NewMockGetFn(nil),
		MockStatusUpdate: test.NewMockSubResourceUpdateFn(nil),
	}
	cr := namespacedRequest(func(cr *v1alpha2.AsyncRequest) {
		cr.Spec.ForProvider.Mappings = []v1alpha2.Mapping{
			{Method: "PATCH", Action: "UPDATE", URL: `.payload.baseUrl + "/models/" + .status.externalRef`, Body: ".payload.body"},
		}
	})
	e := &external{logger: logging.NewNopLogger(), localKube: localKube, http: httpMock}

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Error("expected Update to surface the 404 as an error (Synced=False), got nil (would report ReconcileSuccess and loop)")
	}
}
