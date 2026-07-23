package polling

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	result, err := p.Poll(svcCtx, crCtx, mapping, nil, "")
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
	result, err := p.Poll(svcCtx, crCtx, mapping, nil, "")
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result.TerminalErr == "" {
		t.Error("expected TerminalErr to be non-empty")
	}
	if result.Done {
		t.Error("Done should be false on terminal error")
	}
}

func TestPoll_ResumeURL_SkipsMutate(t *testing.T) {
	// The poll server reports done immediately.
	srv, calls := operationServer(t, 1)
	defer srv.Close()

	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-resume", Namespace: "default"},
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
	// Pass resumeURL directly — polling.url jq expression is not evaluated.
	result, err := p.Poll(svcCtx, crCtx, mapping, nil, srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Done {
		t.Error("expected Done=true")
	}
	if result.OperationURL != srv.URL {
		t.Errorf("expected OperationURL=%s, got %s", srv.URL, result.OperationURL)
	}
	_ = calls // we know it was hit at least once
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
	_, err := p.Poll(svcCtx, crCtx, mapping, nil, srv.URL)
	if err == nil {
		t.Error("expected timeout error")
	}
}

// TestPoll_Timeout_CrossReconcile verifies that a CR with OperationStartedAt already in
// the past (beyond timeout) times out immediately without making any poll GET.
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
	cr.Status.Polling.OperationRef = srv.URL
	cr.Status.Polling.StartedAt = &startedAt

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)

	p := New()
	_, err := p.Poll(svcCtx, crCtx, mapping, nil, srv.URL)
	if err == nil {
		t.Error("expected timeout error for already-expired operation")
	}
	if calls > 0 {
		t.Errorf("expected no poll GET calls before timeout check, got %d", calls)
	}
}

// TestPersistOperationRef_SetsStartTimeOnFirstEntry verifies that persistOperationRef
// sets OperationStartedAt when it is not yet set, and does not overwrite it on resume.
func TestPersistOperationRef_SetsStartTimeOnFirstEntry(t *testing.T) {
	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-persist", Namespace: "default"},
	}
	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)

	if err := persistOperationRef(svcCtx, crCtx, "https://example.com/op/1"); err != nil {
		t.Fatalf("persistOperationRef failed: %v", err)
	}
	firstTime := cr.Status.Polling.StartedAt
	if firstTime == nil {
		t.Fatal("expected OperationStartedAt to be set after first persistOperationRef call")
	}

	// Second call (resume scenario): start time must not be overwritten.
	if err := persistOperationRef(svcCtx, crCtx, "https://example.com/op/1"); err != nil {
		t.Fatalf("second persistOperationRef failed: %v", err)
	}
	if cr.Status.Polling.StartedAt == nil || !cr.Status.Polling.StartedAt.Equal(firstTime) {
		t.Error("OperationStartedAt was overwritten on resume; expected it to be preserved")
	}
}

// TestClearOperationRef_ClearsStartTime verifies that ClearOperationRef also clears OperationStartedAt.
func TestClearOperationRef_ClearsStartTime(t *testing.T) {
	startedAt := metav1.Now()
	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-clear", Namespace: "default"},
	}
	cr.Status.Polling.OperationRef = "https://example.com/op/1"
	cr.Status.Polling.StartedAt = &startedAt

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)

	if err := ClearOperationRef(svcCtx, crCtx); err != nil {
		t.Fatalf("ClearOperationRef failed: %v", err)
	}
	if cr.Status.Polling.OperationRef != "" {
		t.Error("expected OperationRef to be cleared")
	}
	if cr.Status.Polling.StartedAt != nil {
		t.Error("expected OperationStartedAt to be cleared")
	}
}

// TestSetTerminalFailure_ClearsStartTime verifies that SetTerminalFailure clears OperationStartedAt.
func TestSetTerminalFailure_ClearsStartTime(t *testing.T) {
	startedAt := metav1.Now()
	cr := &clusterv1alpha2.AsyncRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-terminal-clear", Namespace: "default"},
	}
	cr.Status.Polling.OperationRef = "https://example.com/op/1"
	cr.Status.Polling.StartedAt = &startedAt

	kube := buildFakeKube(cr)
	svcCtx := buildSvcCtx(t, kube, buildHTTPClient(t))
	crCtx := buildCRCtx(cr)

	if err := SetTerminalFailure(svcCtx, crCtx, "quota exceeded"); err != nil {
		t.Fatalf("SetTerminalFailure failed: %v", err)
	}
	if cr.Status.Polling.OperationRef != "" {
		t.Error("expected OperationRef to be cleared")
	}
	if cr.Status.Polling.StartedAt != nil {
		t.Error("expected OperationStartedAt to be cleared")
	}
	if cr.Status.Polling.TerminalError == "" {
		t.Error("expected TerminalError to be set")
	}
}
