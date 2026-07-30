package request

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	datapatcher "github.com/Antrakos/provider-http-async/internal/data-patcher"
	"github.com/Antrakos/provider-http-async/internal/jq"
	json_util "github.com/Antrakos/provider-http-async/internal/json"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/request/polling"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestgen"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestmapping"
	"github.com/Antrakos/provider-http-async/internal/service/request/statushandler"
	"github.com/Antrakos/provider-http-async/internal/utils"
)

// DeployAction executes the action based on the given AsyncRequest resource and Mapping configuration.
// poller may be nil; polling.New() is used in that case.
func DeployAction(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, action string, poller ...polling.Poller) error {
	p := polling.New()
	if len(poller) > 0 && poller[0] != nil {
		p = poller[0]
	}
	spec := crCtx.Spec()
	mapping, err := requestmapping.GetMapping(spec, action, svcCtx.Logger)
	if err != nil {
		svcCtx.Logger.Info(err.Error())
		return nil
	}

	// Crash/requeue recovery: if a mutate response is already persisted, the mutate call
	// was already fired before a crash, budget expiry, or terminal failure. Skip re-firing
	// and resume the poll loop, recomputing polling.url against the persisted response.
	// This is the orphan-hazard fix: a bad (later-corrected) polling.url resumes the existing
	// operation instead of re-firing CREATE and minting a duplicate remote resource.
	inFlight := crCtx.Status().GetPollingResponse()

	var mutateResponseMap map[string]interface{}

	if inFlight == nil {
		// Normal path: fire the mutate HTTP call.
		requestDetails, genErr := requestgen.GenerateValidRequestDetails(svcCtx, crCtx, mapping)
		if genErr != nil {
			return genErr
		}

		details, sendErr := svcCtx.HTTP.SendRequest(
			svcCtx.Ctx,
			requestmapping.GetEffectiveMethod(mapping),
			requestDetails.Url,
			requestDetails.Body,
			requestDetails.Headers,
			svcCtx.TLSConfigData,
		)

		// Skip secret injection during deletion to avoid cross-namespace owner reference issues.
		if !meta.WasDeleted(crCtx.GetCR()) {
			secretConfigs := spec.GetSecretInjectionConfigs()
			datapatcher.ApplyResponseDataToSecrets(svcCtx.Ctx, svcCtx.LocalKube, svcCtx.Logger, &details.HttpResponse, secretConfigs, crCtx.GetCR())
		} else {
			svcCtx.Logger.Debug("AsyncRequest is being deleted, skipping secret injection")
		}

		// If polling is not configured, handle the response exactly as before.
		if mapping.GetPolling() == nil {
			// For non-polling actions, still attempt externalRef extraction from the response.
			// A failure here is surfaced (not silently dropped) because downstream OBSERVE
			// URLs may depend on .status.externalRef.
			if extractErr := extractExternalRefFromResponse(svcCtx, crCtx, &details.HttpResponse); extractErr != nil {
				svcCtx.Logger.Info("failed to extract externalRef from response", "error", extractErr.Error())
			}
			statusHdlr, hdlrErr := statushandler.NewStatusHandler(svcCtx, crCtx, details, sendErr)
			if hdlrErr != nil {
				return hdlrErr
			}
			if err := statusHdlr.SetRequestStatus(); err != nil {
				return err
			}
			// Surface a non-2xx mutate response (that is not in allowedStatusCodes) per the design
			// doc's default-retry policy: evaluate spec.isTerminalError against the mutate response.
			// Terminal → SetTerminalFailure (stall until spec change); the full response is already
			// in status.response (above). Retry (unset / empty / false) → return a Go error so the
			// controller reports Synced=False (ReconcileError) and requeues, re-firing the mutate —
			// the base provider-http behavior. Without the IsHTTPError return a non-2xx is swallowed
			// (SetRequestStatus increments the counter and returns nil), so the reconciler reports
			// success and requeues forever — silent rather than merely stuck. Status is persisted
			// first (above), so the response/statusCode/failure counter stay visible.
			if utils.IsHTTPError(details.HttpResponse.StatusCode, spec.GetAllowedStatusCodes()) {
				if msg, terminal := classifyMutateTerminalError(crCtx, &details.HttpResponse); terminal {
					return polling.SetTerminalFailure(svcCtx, crCtx, msg, false)
				}
				return errors.Errorf(utils.ErrStatusCode, requestmapping.GetEffectiveMethod(mapping), strconv.Itoa(details.HttpResponse.StatusCode))
			}
			return nil
		}

		// Polling is configured. Only proceed to polling if the mutate call itself succeeded:
		// a transport error (sendErr != nil) returns it via statusHandler so the controller
		// requeues with backoff (the anchor is not yet persisted, so the next reconcile re-fires
		// the mutate — safe, nothing was side-effected).
		if sendErr != nil {
			statusHdlr, hdlrErr := statushandler.NewStatusHandler(svcCtx, crCtx, details, sendErr)
			if hdlrErr != nil {
				return hdlrErr
			}
			return statusHdlr.SetRequestStatus()
		}

		// Polling-mutate non-2xx: the mutate returned a non-success status (e.g. a 4xx/5xx). The
		// base status-code heuristic cannot detect a 200-with-error-body, but it catches the rest.
		// Per the design doc, the default is retry (re-fire the mutate); spec.isTerminalError is the
		// opt-out to stall. No anchor is persisted yet, so there is nothing to clear — SetTerminalFailure
		// with clearAnchor=false is a no-op on the anchor. The response is surfaced to status.response
		// via statusHandler (below) before classifying, so a terminal message and the response coexist.
		if utils.IsHTTPError(details.HttpResponse.StatusCode, spec.GetAllowedStatusCodes()) {
			statusHdlr, hdlrErr := statushandler.NewStatusHandler(svcCtx, crCtx, details, nil)
			if hdlrErr != nil {
				return hdlrErr
			}
			if err := statusHdlr.SetRequestStatus(); err != nil {
				return err
			}
			if msg, terminal := classifyMutateTerminalError(crCtx, &details.HttpResponse); terminal {
				return polling.SetTerminalFailure(svcCtx, crCtx, msg, false)
			}
			// Retry: return a Go error so the controller requeues (ReconcileError) and re-fires the
			// mutate on the next reconcile. The anchor was not persisted, so re-firing is duplicate-safe.
			return errors.Errorf(utils.ErrStatusCode, requestmapping.GetEffectiveMethod(mapping), strconv.Itoa(details.HttpResponse.StatusCode))
		}

		mutateResponseMap = polling.ResponseToMap(&details.HttpResponse)
		// ANCHOR: persist the mutate response BEFORE the first poll so a crash mid-poll
		// (or a terminal polling.url failure) resumes the in-flight operation instead of
		// re-firing the mutate call. This is the crux of the orphan-hazard fix: even on a
		// bad polling.url the anchor survives, so correcting the expression resumes polling
		// rather than creating a duplicate.
		if persistErr := persistPollingResponse(svcCtx, crCtx, mutateResponseMap); persistErr != nil {
			return persistErr
		}
	} else {
		// Resume: the persisted response is the source of truth for .response across every
		// resume (crash, per-reconcile budget expiry, fixed spec), so polling.done /
		// polling.error / externalRef expressions referencing .response stay correct.
		mutateResponseMap = inFlight
		// A terminal-clear reset StartedAt to nil to give the resumed operation a fresh
		// polling.timeout budget. Persist it now (once) so the poller's deadline is anchored
		// to a stable instant and the timeout actually elapses across requeues. Without this
		// the poller's local now-fallback fires on every reconcile, re-anchoring the deadline
		// and unbounding polling.timeout. No-op when StartedAt is already set (normal resume).
		if crCtx.Status().GetOperationStartedAt() == nil {
			if err := persistOperationStartedAtNow(svcCtx, crCtx); err != nil {
				return err
			}
		}
	}

	// Run the poll loop (foreground, budget-bounded). It recomputes polling.url from
	// mutateResponseMap each call — no frozen resumeURL.
	result, pollErr := p.Poll(svcCtx, crCtx, mapping, mutateResponseMap)
	if pollErr != nil {
		// Transport error: return error so the controller requeues with backoff.
		return pollErr
	}

	// Provider-authored terminal (polling.timeout, bad/empty polling.url). Bypasses
	// isTerminalError per the design doc — these are not properties of a response a user
	// expression can see. The anchor is RETAINED (clearAnchor=false): the operation state
	// is unknown, so a corrected spec must resume the existing operation, not re-fire. The
	// poller only sets ClearAnchor=true on the operation-failure branch (handled below via
	// FailingPollResponse), never on TerminalErr, so this is unconditionally a retain.
	if result.TerminalErr != "" {
		return polling.SetTerminalFailure(svcCtx, crCtx, result.TerminalErr, false)
	}

	// Operation failure (polling.done + polling.error non-null): the poll confirmed the
	// operation finished without creating the resource, so a retry is duplicate-safe
	// (edge case #1). The anchor is cleared so a retry re-fires a fresh mutate, not re-polls
	// a dead operation. spec.isTerminalError decides retry-vs-stall against the failing poll
	// response; default (unset) is retry, matching the base provider-http behavior generalized
	// to polling.
	if result.FailingPollResponse != nil {
		return handlePollOperationFailure(svcCtx, crCtx, mutateResponseMap, result)
	}

	if !result.Done {
		// Budget exhausted but operation still running: the anchor is retained.
		// Return nil so the controller requeues without error backoff.
		return nil
	}

	// Poll completed successfully. Extract externalRef and atomically clear the anchor so
	// future reconciles OBSERVE instead of resuming.
	return extractAndPersistExternalRef(svcCtx, crCtx, mutateResponseMap, result.PollResponse)
}

// handlePollOperationFailure classifies a poll-confirmed operation failure via spec.isTerminalError
// and either stalls (terminal) or retries (re-fire mutate). In BOTH cases the failing poll response
// is surfaced to status.response (the terminalResponse-merged-into-status.response model), so an
// operator can inspect the full failure (statusCode / headers / body):
//
//   - isTerminalError resolves terminal (non-empty string / true): write the failing response to
//     status.response, then SetTerminalFailure with the user's message (string) or the polling.error
//     projection (true), clearing the anchor. status.response retains the full failure until a spec
//     change bumps the generation and clears the terminal.
//   - isTerminalError unset / empty / false / null: retry. Write the failing response to
//     status.response, clear the anchor + StartedAt, and return nil so the controller requeues.
//     status.response holds the failure only until the next reconcile's OBSERVE overwrites it, then
//     the reconciler detects Create/Update and re-fires the mutate (duplicate-safe per edge case #1).
func handlePollOperationFailure(
	svcCtx *service.ServiceContext,
	crCtx *service.RequestCRContext,
	mutateResponseMap map[string]interface{},
	result polling.Result,
) error {
	failingResponseMap := polling.ResponseToMap(result.FailingPollResponse)

	// Surface the failing poll response to status.response in both branches before classifying,
	// so a terminal message and the full response coexist and an operator can inspect the failure.
	// The REAL status code (a wire-200 for a completed-with-error LRO) is persisted: on the
	// externalRef path routing consults .status.externalRef, never .status.response, so the write
	// is inert to routing by construction — no faked statusCode/method is needed, and persisting
	// the truth keeps isTerminalError's jq context honest (see docs/prd-explicit-identity-model.md).
	if err := persistPollFailureResponse(svcCtx, crCtx, result.FailingPollResponse); err != nil {
		return err
	}

	if msg, terminal := classifyTerminalError(crCtx, mutateResponseMap, failingResponseMap, result.OperationErrorMessage); terminal {
		// Terminal stall: the user classified this failure as not self-healing. Clear the
		// anchor (the operation is dead) and persist the message with observedGeneration.
		return polling.SetTerminalFailure(svcCtx, crCtx, msg, true)
	}

	// Retry: the operation failed but the resource was not created, so re-firing the mutate is
	// duplicate-safe (edge case #1). Clear the anchor + StartedAt so the next reconcile re-fires
	// a fresh mutate rather than resuming/re-polling the dead operation, then return nil so the
	// controller requeues.
	return clearAnchorAfterPollFailure(svcCtx, crCtx)
}

// classifyMutateTerminalError evaluates spec.isTerminalError against a failing mutate response
// (CREATE/UPDATE/DELETE, polling or not). The response is .response in the jq context; there is no
// .poll (no poll loop ran). The default message (for a bare-`true` result) is a provider-authored
// mutate-failure string naming the status code. Returns (message, terminal) per the same rules as
// classifyTerminalError.
//
// It is only called on an HTTP-error response (non-2xx not in allowedStatusCodes) — i.e. a mutate
// that already failed, where the only open question is retry-vs-stall. A 2xx mutate is a success by
// definition: nothing failed, so there is nothing to classify. The "200-with-error-body" case (a
// success wire status wrapping an operation failure) exists ONLY on the polling path, where the poll
// loop's polling.error expression detects it and routes it through the poll-failure classification
// (handlePollOperationFailure); a non-polling mutate has no such notion and its 2xx is taken at face
// value, matching base provider-http.
func classifyMutateTerminalError(crCtx *service.RequestCRContext, resp *httpClient.HttpResponse) (string, bool) {
	mutateResponseMap := polling.ResponseToMap(resp)
	defaultMessage := fmt.Sprintf("isTerminalError classified the mutate response (HTTP %d) as terminal", resp.StatusCode)
	return classifyTerminalError(crCtx, mutateResponseMap, nil, defaultMessage)
}

// classifyTerminalError evaluates spec.isTerminalError against the jq context built from the
// failing response and returns (message, terminal). terminal is true when the expression
// resolves to a non-empty string (the message) or boolean true (defaultMessage used). It is
// false when the expression is unset, or resolves to empty / false / null — the retry default.
//
// mutateResponseMap is .response in the jq context (the stable mutate response — the anchor for
// a poll failure, or the failing mutate response itself for a mutate failure). failingResponseMap
// is .poll.response for a poll failure, or nil for a mutate failure. defaultMessage is used when
// the expression resolves to a bare `true` (the user said "terminal" but gave no message): it is
// the polling.error projection for a poll failure, or a provider-authored mutate-failure message.
func classifyTerminalError(
	crCtx *service.RequestCRContext,
	mutateResponseMap map[string]interface{},
	failingResponseMap map[string]interface{},
	defaultMessage string,
) (string, bool) {
	expr := crCtx.Spec().GetIsTerminalError()
	if expr == "" {
		// Unset: the base provider-http behavior generalized to polling — retry.
		return "", false
	}
	// Build the jq context the same way the poll loop does, so isTerminalError sees the exact
	// same {spec, status, response, poll} the user's other expressions see. .response is the
	// failing mutate response (a mutate failure) or the stable mutate response (a poll failure,
	// where .poll.response is the failing poll response).
	ctx := requestgen.GenerateRequestContextFromMap(crCtx.Spec(), crCtx.Status(), mutateResponseMap, failingResponseMap)
	val, err := jq.ParseAny(expr, ctx)
	if err != nil {
		// A broken isTerminalError expression is itself a terminal failure: surfacing it as a
		// retry would hot-loop the mutate on a config error the requeue cannot fix. Stall with
		// the jq error so the operator fixes the expression.
		return fmt.Sprintf("isTerminalError jq expression failed: %s", err.Error()), true
	}
	switch t := val.(type) {
	case string:
		if t == "" {
			return "", false // empty string → retry (the default)
		}
		return t, true
	case bool:
		if !t {
			return "", false // false → retry
		}
		// true → terminal with the default (provider/polling.error) message.
		if defaultMessage == "" {
			return "isTerminalError classified this failure as terminal", true
		}
		return defaultMessage, true
	default:
		// null or any non-string/non-bool value → retry (the default). A user wanting to key off
		// a structured value should coerce it, e.g. `(.response.body.error.code // 0) >= 500`.
		return "", false
	}
}

// persistPollFailureResponse surfaces the failing poll response to status.response so an operator
// can inspect the full failure (statusCode / headers / body). The REAL status code is persisted — a
// completed-with-error LRO is typically HTTP 200 with an error block in the body, and that 200 is
// what is stored, not a faked 5xx.
//
// No faked statusCode/method is needed because routing does not consult this write on the
// externalRef path: IsUpToDate routes on .status.externalRef (and the in-flight anchor), never on
// .status.response, so the stored response is inert to routing by construction (see
// docs/prd-explicit-identity-model.md). Persisting the truth — rather than overriding statusCode to
// 500 to trip base's !(POST && IsHTTPError) guard — also keeps isTerminalError's jq context honest:
// the live status is read by GetStatusMap after this write, so a faked 500 would have poisoned an
// expression like `isTerminalError: .status.response.statusCode >= 500` into classifying every
// wire-200-with-error-body poll failure as terminal.
//
// requestDetails.method is stamped POST because this entry records the CREATE operation's failure
// outcome (the poll confirmed the create never produced a resource), not a standalone GET. It is
// inspection-only: routing does not read it on the externalRef path.
//
// The write goes DIRECTLY via the StatusWriter, deliberately NOT through statushandler: the shared
// handler would take its error branch (incrementFailures) and pollute .status.failed, but a poll GET
// is a read and must not count toward the consecutive-mutate-failure counter. Writing directly leaves
// the counter untouched. RetryOnConflict handles optimistic-lock conflicts.
func persistPollFailureResponse(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, failingResponse *httpClient.HttpResponse) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before persisting poll failure response")
		}
		sw := crCtx.StatusWriter()
		// Persist the real error body/headers/statusCode for operator inspection. statusCode is the
		// true transport code (commonly 200 for an LRO that completed with an error block in the body).
		sw.SetBody(failingResponse.Body)
		sw.SetHeaders(failingResponse.Headers)
		sw.SetStatusCode(failingResponse.StatusCode)
		sw.SetRequestDetails("", http.MethodPost, "", nil)
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}

// clearAnchorAfterPollFailure clears the anchor + StartedAt so the next reconcile re-fires a fresh
// mutate (the operation is dead and the resource was not created — duplicate-safe per edge case #1)
// rather than re-polling. RetryOnConflict handles optimistic-lock conflicts.
func clearAnchorAfterPollFailure(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before clearing anchor after poll failure")
		}
		sw := crCtx.StatusWriter()
		sw.SetPollingResponse(nil)
		sw.SetOperationStartedAt(nil)
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}

// extractExternalRefFromResponse evaluates spec.externalRef against .response for the non-polling path.
func extractExternalRefFromResponse(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, resp *httpClient.HttpResponse) error {
	exprJQ := crCtx.Spec().GetExternalRef()
	if exprJQ == "" {
		return nil
	}
	return extractExternalRefInto(svcCtx, crCtx, polling.ResponseToMap(resp), nil)
}

// extractAndPersistExternalRef evaluates spec.externalRef against the combined jq context
// and writes the result along with a cleared polling.response anchor in one status flush.
// RetryOnConflict handles optimistic-lock conflicts from concurrent reconciles.
func extractAndPersistExternalRef(
	svcCtx *service.ServiceContext,
	crCtx *service.RequestCRContext,
	mutateResponseMap map[string]interface{},
	pollResponseMap map[string]interface{},
) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before setting externalRef")
		}

		sw := crCtx.StatusWriter()
		// Operation complete: drop the anchor so subsequent reconciles OBSERVE, not resume.
		sw.SetPollingResponse(nil)
		sw.SetOperationStartedAt(nil)

		if exprJQ := crCtx.Spec().GetExternalRef(); exprJQ != "" {
			jqCtx := buildExternalRefJQCtx(crCtx.Status(), mutateResponseMap, pollResponseMap)
			value, err := jq.ParseString(exprJQ, jqCtx)
			switch {
			case err != nil:
				// Surface the failure: downstream OBSERVE/UPDATE/DELETE URLs depend on
				// .status.externalRef, so a broken expression is worth a visible log.
				svcCtx.Logger.Info("externalRef jq expression failed to evaluate", "expr", exprJQ, "error", err.Error())
			case value != "":
				sw.SetExternalRef(value)
			default:
				svcCtx.Logger.Info("externalRef jq expression yielded an empty value; leaving status.externalRef unchanged", "expr", exprJQ)
			}
		}

		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}

func extractExternalRefInto(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, mutateResp, pollResp map[string]interface{}) error {
	exprJQ := crCtx.Spec().GetExternalRef()
	if exprJQ == "" {
		return nil
	}
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before setting externalRef")
		}
		jqCtx := buildExternalRefJQCtx(crCtx.Status(), mutateResp, pollResp)
		value, err := jq.ParseString(exprJQ, jqCtx)
		if err != nil || value == "" {
			return nil //nolint:nilerr // intentionally skip if jq returns empty
		}
		crCtx.StatusWriter().SetExternalRef(value)
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}

// buildExternalRefJQCtx assembles the jq context for externalRef evaluation. .status exposes
// the FULL observed state (the same context isTerminalError is evaluated against), so a user
// expression can key off the entire status — not just .status.externalRef.
func buildExternalRefJQCtx(
	status interface {
		GetExternalRefValue() string
		GetStatusMap() map[string]interface{}
	},
	mutateResponse map[string]interface{},
	pollResponse map[string]interface{},
) map[string]interface{} {
	ctx := map[string]interface{}{
		"status":   status.GetStatusMap(),
		"response": mutateResponse,
	}
	if pollResponse != nil {
		ctx["poll"] = map[string]interface{}{
			"response": pollResponse,
		}
	}
	json_util.ConvertJSONStringsToMaps(&ctx)
	return ctx
}

// persistPollingResponse writes status.polling.response (and, on first entry,
// StartedAt) to status and flushes to the API server. The anchor is set before the first
// poll so a crash or terminal failure mid-loop resumes the existing operation rather than
// re-firing the mutate call. RetryOnConflict handles optimistic-lock conflicts.
func persistPollingResponse(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, mutateResponse map[string]interface{}) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before setting polling response")
		}
		sw := crCtx.StatusWriter()
		sw.SetPollingResponse(mutateResponse)
		if crCtx.Status().GetOperationStartedAt() == nil {
			now := metav1.Now()
			sw.SetOperationStartedAt(&now)
		}
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}

// persistOperationStartedAtNow sets StartedAt to now and flushes it to the API server.
// Used on resume after a terminal-clear reset StartedAt to nil (to give the resumed
// operation a fresh polling.timeout budget): it anchors the poller's deadline to a stable
// persisted instant so the timeout actually elapses across requeues. Without this the
// poller's local now-fallback would re-anchor the deadline on every reconcile and
// polling.timeout would never elapse. RetryOnConflict handles optimistic-lock conflicts.
func persistOperationStartedAtNow(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before setting operation startedAt")
		}
		now := metav1.Now()
		crCtx.StatusWriter().SetOperationStartedAt(&now)
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}
