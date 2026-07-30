package polling

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha2 "github.com/Antrakos/provider-http-async/apis/cluster/request/v1alpha2"
	"github.com/Antrakos/provider-http-async/apis/common"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/service"
)

// operationServer returns a test server that reports done after n calls.
func operationServer(t *testing.T, doneAfter int) (*httptest.Server, *int) {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		done := calls >= doneAfter
		resp := map[string]interface{}{"done": done}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	return srv, &calls
}

func terminalErrorServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"done":  true,
			"error": map[string]interface{}{"code": 429, "message": "quota exceeded"},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
}

func buildMapping(srv *httptest.Server) *clusterv1alpha2.Mapping {
	interval := metav1.Duration{Duration: 1 * time.Millisecond}
	timeout := metav1.Duration{Duration: 30 * time.Minute}
	return &clusterv1alpha2.Mapping{
		Method: "POST",
		Action: common.ActionCreate,
		URL:    ".payload.baseUrl",
		Polling: &common.Polling{
			URL:      fmt.Sprintf(`"%s"`, srv.URL),
			Done:     ".poll.response.body.done == true",
			Error:    ".poll.response.body.error",
			Interval: &interval,
			Timeout:  &timeout,
		},
	}
}

func buildFakeKube(cr *clusterv1alpha2.AsyncRequest) client.Client {
	scheme := runtime.NewScheme()
	_ = clusterv1alpha2.SchemeBuilder.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cr).WithObjects(cr).Build()
}

func buildSvcCtx(t *testing.T, kubeClient client.Client, httpCli httpClient.Client) *service.ServiceContext {
	return service.NewServiceContext(context.Background(), kubeClient, logging.NewNopLogger(), httpCli, nil)
}

func buildCRCtx(cr *clusterv1alpha2.AsyncRequest) *service.RequestCRContext {
	return service.NewRequestCRContext(cr)
}

func buildHTTPClient(t *testing.T) httpClient.Client {
	c, err := httpClient.NewClient(logging.NewNopLogger(), 10*time.Second, "")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPoll_DoneAfterTwoIterations(t *testing.T) {
	srv, calls := operationServer(t, 2)
	defer srv.Close()

	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
	}
	cr.Spec.ForProvider = clusterv1alpha2.AsyncRequestParameters{
		Payload:  clusterv1alpha2.Payload{BaseUrl: "https://example.com"},
		Mappings: []clusterv1alpha2.Mapping{*buildMapping(srv)},
	}

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)
	mapping := buildMapping(srv)

	p := New()
	result, err := p.Poll(svcCtx, crCtx, mapping, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Done {
		t.Error("expected Done=true")
	}
	if result.TerminalErr != "" {
		t.Errorf("unexpected TerminalErr: %s", result.TerminalErr)
	}
	if *calls < 2 {
		t.Errorf("expected at least 2 poll calls, got %d", *calls)
	}
}

func TestPoll_TerminalError(t *testing.T) {
	srv := terminalErrorServer(t)
	defer srv.Close()

	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-terminal", Namespace: "default"},
	}
	cr.Spec.ForProvider = clusterv1alpha2.AsyncRequestParameters{
		Payload:  clusterv1alpha2.Payload{BaseUrl: "https://example.com"},
		Mappings: []clusterv1alpha2.Mapping{*buildMapping(srv)},
	}

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)
	mapping := buildMapping(srv)

	p := New()
	result, err := p.Poll(svcCtx, crCtx, mapping, nil)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	// An operation failure (polling.done + polling.error non-null) is no longer a poller-level
	// terminal: TerminalErr is reserved for provider-authored config/timeout terminals. The
	// poller reports the failure via FailingPollResponse + OperationErrorMessage and asks the
	// caller to clear the anchor (the operation is dead; a retry re-fires a fresh mutate).
	if result.FailingPollResponse == nil {
		t.Error("expected FailingPollResponse to be set on operation failure")
	}
	if result.OperationErrorMessage == "" {
		t.Error("expected OperationErrorMessage to be non-empty on operation failure")
	}
	if !result.ClearAnchor {
		t.Error("expected ClearAnchor=true on operation failure")
	}
	if result.TerminalErr != "" {
		t.Errorf("expected TerminalErr to be empty on operation failure, got %q", result.TerminalErr)
	}
	if result.Done {
		t.Error("Done should be false on operation failure")
	}
}

// TestPoll_TerminalError_StructuredResponse verifies that when polling.error evaluates to
// a structured object, Poll captures the FULL poll HTTP response in Result.FailingPollResponse
// (so a classifier can key off .body.error.code and the HTTP statusCode) while
// OperationErrorMessage carries the JSON projection of the polling.error value used as the
// default terminal message. Retaining only the jq value would have dropped the HTTP statusCode
// and any body fields the user's polling.error expression did not extract.
func TestPoll_TerminalError_StructuredResponse(t *testing.T) {
	srv := terminalErrorServer(t) // body: {"done":true,"error":{"code":429,"message":"quota exceeded"}}
	defer srv.Close()

	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-terminal-structured", Namespace: "default"},
	}
	cr.Spec.ForProvider = clusterv1alpha2.AsyncRequestParameters{
		Payload:  clusterv1alpha2.Payload{BaseUrl: "https://example.com"},
		Mappings: []clusterv1alpha2.Mapping{*buildMapping(srv)},
	}

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)
	mapping := buildMapping(srv)

	p := New()
	result, err := p.Poll(svcCtx, crCtx, mapping, nil)
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.OperationErrorMessage == "" {
		t.Fatal("expected OperationErrorMessage to be non-empty")
	}
	// The FULL poll response is retained as a typed HttpResponse: statusCode, body, and
	// headers — a classifier keys off these real fields, independent of what polling.error
	// extracted into OperationErrorMessage.
	resp := result.FailingPollResponse
	if resp == nil {
		t.Fatal("expected FailingPollResponse to be set")
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected FailingPollResponse .StatusCode == 200, got %d", resp.StatusCode)
	}
	// Body is the raw JSON string; a classifier parses it to reach .body.error.code.
	var body map[string]interface{}
	if err := json.Unmarshal([]byte(resp.Body), &body); err != nil {
		t.Fatalf("expected FailingPollResponse .Body to be JSON, got %q: %v", resp.Body, err)
	}
	errObj, _ := body["error"].(map[string]interface{})
	if code, _ := errObj["code"].(float64); int(code) != 429 {
		t.Errorf("expected FailingPollResponse .body.error.code == 429, got %v", errObj["code"])
	}
	if msg, _ := errObj["message"].(string); msg != "quota exceeded" {
		t.Errorf("expected FailingPollResponse .body.error.message == %q, got %q", "quota exceeded", msg)
	}
	// OperationErrorMessage is the JSON projection of the polling.error VALUE (not the full
	// response) — the default terminal message stays compact.
	if result.OperationErrorMessage != `{"code":429,"message":"quota exceeded"}` {
		t.Errorf("expected OperationErrorMessage to be the JSON projection, got %q", result.OperationErrorMessage)
	}
}

// TestTerminalMessageFromValue covers the string/number/object/array projections the
// poller derives from a raw polling.error value for the Ready condition message.
func TestTerminalMessageFromValue(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  interface{}
		want string
	}{
		{"string", "quota exceeded", "quota exceeded"},
		{"number", 429, "429"},
		{"boolean", false, "false"},
		{"object", map[string]interface{}{"code": 429, "message": "quota exceeded"},
			`{"code":429,"message":"quota exceeded"}`},
		{"array", []interface{}{1, 2, 3}, "[1,2,3]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := terminalMessageFromValue(tc.val, ".poll.response.body.error"); got != tc.want {
				t.Errorf("terminalMessageFromValue(%v) = %q, want %q", tc.val, got, tc.want)
			}
		})
	}
}

// TestPoll_Resume_RecomputesURL verifies that on a resume (anchor persisted) the poll
// URL is recomputed from the mutate response rather than frozen. The polling.url
// expression derives the URL from .response.body.name, and the persisted mutate response
// carries that name — so the same URL is re-derived and the operation completes.
func TestPoll_Resume_RecomputesURL(t *testing.T) {
	srv, calls := operationServer(t, 1)
	defer srv.Close()

	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-resume", Namespace: "default"},
	}
	cr.Spec.ForProvider = clusterv1alpha2.AsyncRequestParameters{
		Payload: clusterv1alpha2.Payload{BaseUrl: "https://example.com"},
		Mappings: []clusterv1alpha2.Mapping{{
			Method: "POST",
			Action: common.ActionCreate,
			URL:    ".payload.baseUrl",
			Polling: &common.Polling{
				// Derive the poll URL from the mutate response name — recomputed each call.
				// jq: "<srv.URL>" + "/" + .response.body.name
				URL:      fmt.Sprintf(`"%s" + "/" + .response.body.name`, srv.URL),
				Done:     ".poll.response.body.done == true",
				Interval: &metav1.Duration{Duration: 1 * time.Millisecond},
				Timeout:  &metav1.Duration{Duration: 30 * time.Minute},
			},
		}},
	}

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)
	mapping := &cr.Spec.ForProvider.Mappings[0]

	// The mutate response carries the operation name; the poll URL is derived from it.
	mutateResponse := map[string]interface{}{
		"body": map[string]interface{}{"name": "op-789"},
	}

	p := New()
	result, err := p.Poll(svcCtx, crCtx, mapping, mutateResponse)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Done {
		t.Error("expected Done=true")
	}
	if !strings.HasSuffix(result.OperationURL, "/op-789") {
		t.Errorf("expected OperationURL to be recomputed from .response.body.name (suffix /op-789), got %s", result.OperationURL)
	}
	_ = calls
}

func TestPoll_Timeout(t *testing.T) {
	// Server never reports done.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{"done": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	shortTimeout := metav1.Duration{Duration: 50 * time.Millisecond}
	shortInterval := metav1.Duration{Duration: 10 * time.Millisecond}
	mapping := &clusterv1alpha2.Mapping{
		Method: "POST",
		Action: common.ActionCreate,
		URL:    ".payload.baseUrl",
		Polling: &common.Polling{
			URL:      fmt.Sprintf(`"%s"`, srv.URL),
			Done:     ".poll.response.body.done == true",
			Timeout:  &shortTimeout,
			Interval: &shortInterval,
		},
	}

	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-timeout", Namespace: "default"},
	}
	cr.Spec.ForProvider = clusterv1alpha2.AsyncRequestParameters{
		Payload:  clusterv1alpha2.Payload{BaseUrl: "https://example.com"},
		Mappings: []clusterv1alpha2.Mapping{*mapping},
	}

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)

	p := New()
	result, err := p.Poll(svcCtx, crCtx, mapping, nil)
	if err != nil {
		t.Fatalf("unexpected Go error (timeout is now terminal, not a Go error): %v", err)
	}
	if result.TerminalErr == "" {
		t.Error("expected terminal timeout failure")
	}
}

// TestPoll_BarePathURL_TerminalError verifies that a polling.url resolving to a
// scheme-less path (the common GCP LRO misconfiguration) is reported as a terminal
// failure with an actionable message, and that no poll GET is attempted.
func TestPoll_BarePathURL_TerminalError(t *testing.T) {
	interval := metav1.Duration{Duration: 1 * time.Millisecond}
	timeout := metav1.Duration{Duration: 30 * time.Minute}
	mapping := &clusterv1alpha2.Mapping{
		Method: "POST",
		Action: common.ActionCreate,
		URL:    ".payload.baseUrl",
		Polling: &common.Polling{
			// jq expression that yields a bare GCP resource path (no scheme).
			URL:      `.response.body.name`,
			Done:     ".poll.response.body.done == true",
			Interval: &interval,
			Timeout:  &timeout,
		},
	}

	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-barepath", Namespace: "default"},
	}
	cr.Spec.ForProvider = clusterv1alpha2.AsyncRequestParameters{
		Payload:  clusterv1alpha2.Payload{BaseUrl: "https://example.com"},
		Mappings: []clusterv1alpha2.Mapping{*mapping},
	}

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)

	mutateResponse := map[string]interface{}{
		"body": map[string]interface{}{
			"name": "projects/123/locations/us-central1/operations/789",
		},
	}

	p := New()
	result, err := p.Poll(svcCtx, crCtx, mapping, mutateResponse)
	if err != nil {
		t.Fatalf("expected nil Go error (terminal failure), got: %v", err)
	}
	if result.TerminalErr == "" {
		t.Fatal("expected TerminalErr to be set for a scheme-less polling.url")
	}
	if result.Done {
		t.Error("Done should be false on terminal error")
	}
}

// TestPoll_EmptyURL_SyncOpTerminalError verifies that a polling.url resolving to an
// empty value (a `polling` block mis-attached to a synchronous operation whose response
// carries no LRO identifier) is reported as a terminal failure steering the user to
// remove the polling block.
func TestPoll_EmptyURL_SyncOpTerminalError(t *testing.T) {
	interval := metav1.Duration{Duration: 1 * time.Millisecond}
	timeout := metav1.Duration{Duration: 30 * time.Minute}
	mapping := &clusterv1alpha2.Mapping{
		Method: "POST",
		Action: common.ActionCreate,
		URL:    ".payload.baseUrl",
		Polling: &common.Polling{
			// A synchronous response has no operation name, so this yields empty.
			URL:      `(.response.body.name // "")`,
			Done:     ".poll.response.body.done == true",
			Interval: &interval,
			Timeout:  &timeout,
		},
	}

	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-emptyurl", Namespace: "default"},
	}
	cr.Spec.ForProvider = clusterv1alpha2.AsyncRequestParameters{
		Payload:  clusterv1alpha2.Payload{BaseUrl: "https://example.com"},
		Mappings: []clusterv1alpha2.Mapping{*mapping},
	}

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)

	// Synchronous response: no `name` field.
	mutateResponse := map[string]interface{}{
		"body": map[string]interface{}{"id": "model-1"},
	}

	p := New()
	result, err := p.Poll(svcCtx, crCtx, mapping, mutateResponse)
	if err != nil {
		t.Fatalf("expected nil Go error (terminal failure), got: %v", err)
	}
	if result.TerminalErr == "" {
		t.Fatal("expected TerminalErr for an empty polling.url")
	}
	if !strings.Contains(result.TerminalErr, "synchronous") {
		t.Errorf("expected terminal message to steer toward removing the polling block, got: %q", result.TerminalErr)
	}
}

func TestValidateOperationURL(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"absolute https", "https://api.example.com/v1/operations/1", false},
		{"absolute http", "http://api.example.com/op", false},
		{"bare path", "projects/123/operations/789", true},
		{"leading slash path", "/v1/operations/1", true},
		{"empty", "", true},
		{"ftp scheme", "ftp://host/path", true},
		{"scheme no host", "https:///op", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateOperationURL(tc.raw)
			if tc.wantErr && got == "" {
				t.Errorf("expected a validation error for %q, got none", tc.raw)
			}
			if !tc.wantErr && got != "" {
				t.Errorf("expected no validation error for %q, got %q", tc.raw, got)
			}
		})
	}
}

// TestPoll_Timeout_CrossReconcile verifies that a CR with OperationStartedAt already in
// the past (beyond timeout) times out immediately (as a terminal failure) without
// making any poll GET.
func TestPoll_Timeout_CrossReconcile(t *testing.T) {
	// Server that would be queried if the timeout check failed — we track calls.
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		resp := map[string]interface{}{"done": false}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	timeout := metav1.Duration{Duration: 30 * time.Second}
	interval := metav1.Duration{Duration: 1 * time.Millisecond}
	mapping := &clusterv1alpha2.Mapping{
		Method: "POST",
		Action: common.ActionCreate,
		URL:    ".payload.baseUrl",
		Polling: &common.Polling{
			URL:      fmt.Sprintf(`"%s"`, srv.URL),
			Done:     ".poll.response.body.done == true",
			Timeout:  &timeout,
			Interval: &interval,
		},
	}

	// Simulate a CR that started polling more than timeout ago.
	startedAt := metav1.NewTime(time.Now().Add(-(timeout.Duration + time.Second)))
	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-timeout-cross", Namespace: "default"},
	}
	cr.Spec.ForProvider = clusterv1alpha2.AsyncRequestParameters{
		Payload:  clusterv1alpha2.Payload{BaseUrl: "https://example.com"},
		Mappings: []clusterv1alpha2.Mapping{*mapping},
	}
	cr.Status.Polling.StartedAt = &startedAt

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)

	p := New()
	result, err := p.Poll(svcCtx, crCtx, mapping, nil)
	if err != nil {
		t.Fatalf("unexpected Go error (timeout is terminal): %v", err)
	}
	if result.TerminalErr == "" {
		t.Error("expected terminal timeout failure for already-expired operation")
	}
	if calls > 0 {
		t.Errorf("expected no poll GET calls before timeout check, got %d", calls)
	}
}

// TestSetTerminalFailure_RetainsAnchor verifies the clearAnchor=false policy (timeout /
// bad-polling.url terminal): SetTerminalFailure must PRESERVE the polling.response anchor
// (and StartedAt) so a corrected spec resumes the in-flight operation instead of re-creating
// it. The terminal message (now top-level status.terminalError) and observedGeneration are
// written.
func TestSetTerminalFailure_RetainsAnchor(t *testing.T) {
	startedAt := metav1.Now()
	anchor := map[string]interface{}{"body": map[string]interface{}{"name": "op-1"}}
	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-terminal-retain", Namespace: "default"},
	}
	cr.SetPollingResponse(anchor)
	cr.Status.Polling.StartedAt = &startedAt

	msg := "polling.timeout elapsed"

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)

	if err := SetTerminalFailure(svcCtx, crCtx, msg, false); err != nil {
		t.Fatalf("SetTerminalFailure failed: %v", err)
	}
	// The anchor must survive a timeout/config terminal (clearAnchor=false).
	if cr.Status.Polling.Response == nil {
		t.Fatal("expected polling.response anchor to be PRESERVED across timeout/config terminal")
	}
	if cr.Status.Polling.StartedAt == nil {
		t.Fatal("expected OperationStartedAt to be PRESERVED across timeout/config terminal")
	}
	// The fake client round-trips status through JSON, which drops the sub-second
	// and monotonic clock reading; compare wall time truncated to the second rather
	// than metav1.Time.Equal, which treats those differences as inequality.
	if !cr.Status.Polling.StartedAt.Truncate(time.Second).Equal(startedAt.Truncate(time.Second)) {
		t.Errorf("expected OperationStartedAt wall time unchanged, got %v want %v",
			cr.Status.Polling.StartedAt.Time, startedAt.Time)
	}
	// TerminalError relocated out of status.polling to top-level status.terminalError.
	if cr.Status.TerminalError != msg {
		t.Errorf("expected status.terminalError == %q, got %q", msg, cr.Status.TerminalError)
	}
}

// TestSetTerminalFailure_ClearsAnchor verifies the clearAnchor=true policy (operation
// failure, polling.error non-null): the operation is dead, so SetTerminalFailure must drop
// the polling.response anchor and StartedAt, so a spec change re-fires a fresh, duplicate-safe
// mutate rather than re-polling the dead operation.
func TestSetTerminalFailure_ClearsAnchor(t *testing.T) {
	startedAt := metav1.Now()
	anchor := map[string]interface{}{"body": map[string]interface{}{"name": "op-1"}}
	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-terminal-clear", Namespace: "default"},
	}
	cr.SetPollingResponse(anchor)
	cr.Status.Polling.StartedAt = &startedAt

	msg := "operation failed permanently"

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)

	if err := SetTerminalFailure(svcCtx, crCtx, msg, true); err != nil {
		t.Fatalf("SetTerminalFailure failed: %v", err)
	}
	if cr.Status.Polling.Response != nil {
		t.Fatal("expected polling.response anchor to be CLEARED on operation-failure terminal")
	}
	if cr.Status.Polling.StartedAt != nil {
		t.Fatal("expected OperationStartedAt to be CLEARED on operation-failure terminal")
	}
	if cr.Status.TerminalError != msg {
		t.Errorf("expected status.terminalError == %q, got %q", msg, cr.Status.TerminalError)
	}
}
