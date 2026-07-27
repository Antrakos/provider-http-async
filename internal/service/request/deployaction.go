package request

import (
	"strconv"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"github.com/Antrakos/provider-http-async/apis/interfaces"
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
			// Surface a non-2xx mutate response (that is not in allowedStatusCodes) as a real
			// error so the controller reports Synced=False (ReconcileError) instead of logging
			// "Successfully requested update" and reporting ReconcileSuccess. Without this a
			// PATCH/PUT/DELETE to a broken URL (e.g. .../models/ from an empty externalRef) that
			// returns 404 is swallowed: SetRequestStatus increments the failures counter and
			// returns nil, so the reconciler reports success and requeues forever — the loop is
			// silent rather than merely stuck. Status is persisted first (above), so the
			// response/statusCode/Failure counter stay visible alongside the failed reconcile.
			if utils.IsHTTPError(details.HttpResponse.StatusCode, spec.GetAllowedStatusCodes()) {
				return errors.Errorf(utils.ErrStatusCode, requestmapping.GetEffectiveMethod(mapping), strconv.Itoa(details.HttpResponse.StatusCode))
			}
			return nil
		}

		// Polling is configured: don't surface the mutate response to statusHandler yet.
		// Only proceed to polling if the mutate call itself succeeded.
		if sendErr != nil {
			statusHdlr, hdlrErr := statushandler.NewStatusHandler(svcCtx, crCtx, details, sendErr)
			if hdlrErr != nil {
				return hdlrErr
			}
			return statusHdlr.SetRequestStatus()
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

	if result.TerminalErr != "" {
		// Terminal poll failure (polling.error non-null, bad/empty polling.url, or
		// timeout): set Ready=False, Synced=False and record the generation; the anchor is
		// PRESERVED so a spec change resumes the existing operation rather than re-creating.
		// terminalResponse is set only on the polling.error branch; pass a plain nil
		// interface (not the nil *HttpResponse) for bad/empty polling.url and timeout so the
		// setter clears the field rather than storing an empty Response via a typed nil.
		var resp interfaces.HTTPResponse
		if result.TerminalResponse != nil {
			resp = result.TerminalResponse
		}
		return polling.SetTerminalFailure(svcCtx, crCtx, result.TerminalErr, resp)
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

// buildExternalRefJQCtx assembles the jq context for externalRef evaluation.
func buildExternalRefJQCtx(
	status interface {
		GetExternalRefValue() string
		GetPollingResponse() map[string]interface{}
	},
	mutateResponse map[string]interface{},
	pollResponse map[string]interface{},
) map[string]interface{} {
	ctx := map[string]interface{}{
		"status": map[string]interface{}{
			"externalRef": status.GetExternalRefValue(),
		},
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
