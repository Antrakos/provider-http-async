/*
Copyright 2024 The Crossplane Authors.

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

package disposablerequest

import (
	"net/url"
	"time"

	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Antrakos/provider-http-async/apis/common"
	"github.com/Antrakos/provider-http-async/apis/interfaces"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	datapatcher "github.com/Antrakos/provider-http-async/internal/data-patcher"
	"github.com/Antrakos/provider-http-async/internal/jq"
	json_util "github.com/Antrakos/provider-http-async/internal/json"
	"github.com/Antrakos/provider-http-async/internal/service"
)

// defaultReconcileBudget caps how long a single reconcile spends in the foreground
// poll loop before returning so Crossplane requeues. The next reconcile resumes via
// the persisted status.polling.response anchor.
const defaultReconcileBudget = 2 * time.Minute

// PollingAware is implemented by AsyncDisposableRequest CRs (both cluster and
// namespaced) so the scope-agnostic service layer can drive the long-running
// operation poll loop and persist its anchor state.
type PollingAware interface {
	GetPolling() *common.Polling
	GetPollingResponse() map[string]interface{}
	SetPollingResponse(m map[string]interface{})
	GetOperationStartedAt() *metav1.Time
	SetOperationStartedAt(t *metav1.Time)
}

// asPollingAware returns the CR as a PollingAware, or (nil, false) when the CR type
// does not support polling.
func asPollingAware(crCtx *service.DisposableRequestCRContext) (PollingAware, bool) {
	pa, ok := crCtx.GetCR().(PollingAware)
	return pa, ok
}

// beginPolling persists the trigger response as the poll anchor, records the request
// details, then drives the poll loop. It is called after a successful trigger request.
func beginPolling(svcCtx *service.ServiceContext, crCtx *service.DisposableRequestCRContext, pa PollingAware, triggerResp httpClient.HttpDetails) error {
	triggerMap := responseToMap(&triggerResp.HttpResponse)

	pa.SetPollingResponse(triggerMap)
	if pa.GetOperationStartedAt() == nil {
		now := metav1.Now()
		pa.SetOperationStartedAt(&now)
	}

	writer := crCtx.StatusWriter()
	writer.SetRequestDetails(triggerResp.HttpRequest.URL, triggerResp.HttpRequest.Method, triggerResp.HttpRequest.Body, triggerResp.HttpRequest.Headers)
	writer.SetLastReconcileTime()
	if err := persistStatus(svcCtx, crCtx); err != nil {
		return err
	}

	return drivePolling(svcCtx, crCtx, pa, triggerMap)
}

// resumePolling continues an in-flight operation using the persisted anchor without
// re-firing the trigger request.
func resumePolling(svcCtx *service.ServiceContext, crCtx *service.DisposableRequestCRContext, pa PollingAware) error {
	triggerMap := pa.GetPollingResponse()
	return drivePolling(svcCtx, crCtx, pa, triggerMap)
}

// drivePolling runs the budget-bounded foreground poll loop against the operation URL
// derived from the trigger response. It returns nil while the operation is still
// in-flight (Crossplane requeues), finalizes the resource on completion, and returns a
// Go error only for transient transport failures (so the controller requeues with backoff).
//
//gocyclo:ignore
func drivePolling(svcCtx *service.ServiceContext, crCtx *service.DisposableRequestCRContext, pa PollingAware, triggerMap map[string]interface{}) error {
	polling := interfaces.NewPollingAdapter(pa.GetPolling())

	operationURL, err := jq.ParseString(polling.GetURL(), buildJQContext(crCtx, triggerMap, nil))
	if err != nil {
		return errors.Wrap(err, "failed to evaluate polling.url")
	}
	if reason := validateOperationURL(operationURL); reason != "" {
		return finalizeError(svcCtx, crCtx, pa, errors.Errorf("polling.url %q resolved to %q, which is not a valid absolute URL: %s", polling.GetURL(), operationURL, reason), nil)
	}

	startedAt := pa.GetOperationStartedAt()
	if startedAt == nil {
		now := metav1.Now()
		startedAt = &now
	}
	deadline := startedAt.Add(polling.GetTimeout())
	budgetEnd := time.Now().Add(defaultReconcileBudget)

	for {
		if time.Now().After(deadline) {
			return finalizeError(svcCtx, crCtx, pa, errors.Errorf("polling timeout after %s for operation %s", polling.GetTimeout(), operationURL), nil)
		}

		if time.Now().After(budgetEnd) {
			// Still in flight: anchor already persisted, requeue and resume next reconcile.
			crCtx.StatusWriter().SetLastReconcileTime()
			return persistStatus(svcCtx, crCtx)
		}

		pollResp, err := svcCtx.HTTP.SendRequest(
			svcCtx.Ctx, "GET", operationURL,
			httpClient.Data{Encrypted: "", Decrypted: ""},
			httpClient.Data{Encrypted: map[string][]string{}, Decrypted: map[string][]string{}},
			svcCtx.TLSConfigData,
		)
		if err != nil {
			return errors.Wrap(err, "polling GET failed")
		}

		pollMap := responseToMap(&pollResp.HttpResponse)
		jqCtx := buildJQContext(crCtx, triggerMap, pollMap)

		done, err := jq.ParseBool(polling.GetDone(), jqCtx)
		if err != nil {
			return errors.Wrap(err, "failed to evaluate polling.done")
		}

		if done {
			if errExpr := polling.GetError(); errExpr != "" {
				exists, checkErr := jq.Exists(errExpr, jqCtx)
				if checkErr != nil {
					return errors.Wrap(checkErr, "failed to evaluate polling.error")
				}
				if exists {
					// The operation finished in failure. For a one-off request this is the end of
					// the line: surface the failing poll response and the operation error, following
					// the resource's existing rollback/retry policy (no separate terminalError state).
					opErr := operationErrorMessage(errExpr, jqCtx)
					return finalizeError(svcCtx, crCtx, pa, errors.New(opErr), &pollResp.HttpResponse)
				}
			}
			return finalizeSuccess(svcCtx, crCtx, pa, &pollResp.HttpResponse)
		}

		timer := time.NewTimer(polling.GetInterval())
		select {
		case <-svcCtx.Ctx.Done():
			timer.Stop()
			return svcCtx.Ctx.Err()
		case <-timer.C:
		}
	}
}

// finalizeSuccess is reached when the operation completed without error. It evaluates
// ExpectedResponse against the final poll response, applies secret injections, clears the
// anchor, and marks the resource synced.
func finalizeSuccess(svcCtx *service.ServiceContext, crCtx *service.DisposableRequestCRContext, pa PollingAware, pollResp *httpClient.HttpResponse) error {
	spec := crCtx.Spec()
	writer := crCtx.StatusWriter()

	isExpected, err := IsResponseAsExpected(spec, *pollResp)
	if err != nil {
		return err
	}

	// The poll response is the meaningful state for a completed LRO; persist it.
	writer.SetStatusCode(pollResp.StatusCode)
	writer.SetHeaders(pollResp.Headers)
	writer.SetBody(pollResp.Body)
	writer.SetLastReconcileTime()

	pa.SetPollingResponse(nil)
	pa.SetOperationStartedAt(nil)

	if !isExpected {
		writer.SetError(errors.New(errResponseFormat + "reached (poll response did not match expectedResponse)"))
		return persistStatus(svcCtx, crCtx)
	}

	if len(spec.GetSecretInjectionConfigs()) > 0 {
		if obj, ok := crCtx.GetCR().(metav1.Object); ok {
			datapatcher.ApplyResponseDataToSecrets(svcCtx.Ctx, svcCtx.LocalKube, svcCtx.Logger, pollResp, spec.GetSecretInjectionConfigs(), obj)
		}
	}

	writer.SetSynced(true)
	return persistStatus(svcCtx, crCtx)
}

// finalizeError records an operation/timeout/config failure using the resource's error
// tracking (increments failed, sets error), clears the anchor so a retry re-fires a fresh
// request, and returns the error so the controller surfaces it.
func finalizeError(svcCtx *service.ServiceContext, crCtx *service.DisposableRequestCRContext, pa PollingAware, opErr error, pollResp *httpClient.HttpResponse) error {
	writer := crCtx.StatusWriter()

	if pollResp != nil {
		writer.SetStatusCode(pollResp.StatusCode)
		writer.SetHeaders(pollResp.Headers)
		writer.SetBody(pollResp.Body)
	}
	writer.SetError(opErr)
	writer.SetLastReconcileTime()

	pa.SetPollingResponse(nil)
	pa.SetOperationStartedAt(nil)

	if err := persistStatus(svcCtx, crCtx); err != nil {
		return errors.Wrap(err, "failed to persist polling error status")
	}
	return opErr
}

// operationErrorMessage projects the polling.error jq value onto a human-readable string.
func operationErrorMessage(errExpr string, jqCtx map[string]interface{}) string {
	if s, err := jq.ParseString(errExpr, jqCtx); err == nil && s != "" {
		return "operation failed: " + s
	}
	return "operation failed"
}

// buildJQContext builds the jq object exposing the spec (ForProvider fields at root),
// the trigger .response, the optional .poll.response, and the .status of the resource
// (mirroring AsyncRequest so expressions like `.status.failed` work identically).
func buildJQContext(crCtx *service.DisposableRequestCRContext, triggerMap, pollMap map[string]interface{}) map[string]interface{} {
	base, _ := json_util.StructToMap(crCtx.Spec())
	if base == nil {
		base = map[string]interface{}{}
	}
	if triggerMap == nil {
		triggerMap = map[string]interface{}{}
	}
	base["response"] = triggerMap
	if pollMap != nil {
		base["poll"] = map[string]interface{}{"response": pollMap}
	}
	base["status"] = crStatusMap(crCtx)
	json_util.ConvertJSONStringsToMaps(&base)
	return base
}

// crStatusMap returns the resource's status as a JSON-compatible map for the jq
// context, or nil when it cannot be marshaled.
func crStatusMap(crCtx *service.DisposableRequestCRContext) map[string]interface{} {
	full := common.StructToMap(crCtx.GetCR())
	if s, ok := full["status"].(map[string]interface{}); ok {
		return s
	}
	return nil
}

// responseToMap converts an HTTP response into the {body, headers, statusCode} map used as
// .response / .poll.response, decoding JSON string bodies into nested maps.
func responseToMap(resp *httpClient.HttpResponse) map[string]interface{} {
	m, _ := json_util.StructToMap(resp)
	json_util.ConvertJSONStringsToMaps(&m)
	return m
}

// persistStatus writes the current CR status to the API server.
func persistStatus(svcCtx *service.ServiceContext, crCtx *service.DisposableRequestCRContext) error {
	return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, crCtx.GetCR())
}

// validateOperationURL returns an empty string when raw is a usable absolute HTTP(S) URL,
// or a short reason why it is not (catching scheme-less GCP LRO resource paths).
func validateOperationURL(raw string) string {
	if raw == "" {
		return "resolved to an empty value"
	}
	u, err := url.Parse(raw)
	if err != nil {
		return err.Error()
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		if u.Scheme == "" {
			return "missing scheme (expected http:// or https://)"
		}
		return "unsupported scheme (expected http or https)"
	}
	if u.Host == "" {
		return "missing host"
	}
	return ""
}
