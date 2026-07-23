package request

import (
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/pkg/errors"
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

	// Crash/requeue recovery: if operationRef is already set for this resource, the
	// mutate call was already fired before a crash or budget expiry. Skip re-firing
	// and go straight to resuming the poll loop.
	resumeURL := crCtx.Status().GetOperationRef()

	var mutateResponseMap map[string]interface{}

	if resumeURL == "" {
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
			return statusHdlr.SetRequestStatus()
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
	}

	// Run the poll loop (foreground, budget-bounded).
	result, pollErr := p.Poll(svcCtx, crCtx, mapping, mutateResponseMap, resumeURL)
	if pollErr != nil {
		// Transport error or timeout: return error so the controller requeues with backoff.
		return pollErr
	}

	if result.TerminalErr != "" {
		// Terminal poll failure: set Ready=False, Synced=False; do not requeue until spec changes.
		return polling.SetTerminalFailure(svcCtx, crCtx, result.TerminalErr)
	}

	if !result.Done {
		// Budget exhausted but operation still running: operationRef is retained.
		// Return nil so the controller requeues without error backoff.
		return nil
	}

	// Poll completed successfully. Extract externalRef and atomically clear operationRef.
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
// and writes the result along with a cleared operationRef in one status flush.
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
		sw.SetOperationRef("")
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
		GetOperationRef() string
	},
	mutateResponse map[string]interface{},
	pollResponse map[string]interface{},
) map[string]interface{} {
	ctx := map[string]interface{}{
		"status": map[string]interface{}{
			"externalRef":  status.GetExternalRefValue(),
			"operationRef": status.GetOperationRef(),
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
