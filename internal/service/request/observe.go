package request

import (
	"net/http"

	"github.com/Antrakos/provider-http-async/apis/common"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	datapatcher "github.com/Antrakos/provider-http-async/internal/data-patcher"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/request/observe"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestgen"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestmapping"
	"github.com/Antrakos/provider-http-async/internal/utils"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

const (
	errNotValidJSON              = "%s is not a valid JSON string: %s"
	errConvertResToMap           = "failed to convert response to map"
	errExpectedResponseCheckType = "%s.Type should be either DEFAULT, CUSTOM or empty"
)

type ObserveRequestDetails struct {
	Details       httpClient.HttpDetails
	ResponseError error
	Synced        bool
	// TerminalError, when non-empty, means the resource is in a stable terminal
	// failure state (a prior poll failed with polling.error non-null, and the spec
	// has not changed since). The controller must report the resource unhealthy
	// (Ready=False) but must NOT re-trigger Create/Update — hence Synced is reported
	// true so the managed reconciler treats it as up-to-date and takes no action.
	TerminalError string
}

// NewObserveRequestDetails is a constructor function that initializes
// an instance of ObserveRequestDetails with default values.
func NewObserve(details httpClient.HttpDetails, resErr error, synced bool) ObserveRequestDetails {
	return ObserveRequestDetails{
		Synced:        synced,
		Details:       details,
		ResponseError: resErr,
	}
}

// NewTerminalObserve returns an observation representing a stable terminal failure:
// up-to-date (no Create/Update) but unhealthy, carrying the failure message.
func NewTerminalObserve(terminalErr string) ObserveRequestDetails {
	return ObserveRequestDetails{
		Synced:        true,
		TerminalError: terminalErr,
	}
}

// NewObserveRequestDetails is a constructor function that initializes
// an instance of ObserveRequestDetails with default values.
func FailedObserve() ObserveRequestDetails {
	return ObserveRequestDetails{
		Synced: false,
	}
}

// IsUpToDate checks whether desired spec up to date with the observed state for a given request.
// When operationRef is non-empty an async mutate LRO is still in flight; OBSERVE must not fire
// because externalRef may not yet be written. Return ResourceUpToDate=false so the reconciler
// calls Create/Update (which will resume the poll loop via resumeURL) instead.
func IsUpToDate(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext) (ObserveRequestDetails, error) {
	status := crCtx.Status()

	// Terminal failure: a prior poll failed terminally (polling.error non-null) or a
	// permanent config error occurred. Stay in this stable state — reporting up-to-date
	// so the reconciler does not re-fire Create/Update — until the spec changes, which is
	// detected via generation drift. This is the "stalled, needs human intervention"
	// pattern; the resource remains visible and is marked unhealthy via conditions.
	if terminalErr := status.GetTerminalError(); terminalErr != "" {
		if status.GetGeneration() == status.GetObservedGeneration() {
			return NewTerminalObserve(terminalErr), nil
		}
		// Spec changed since the terminal failure — clear it and fall through to a normal
		// observe so the resource gets another chance.
		if err := clearTerminalError(svcCtx, crCtx); err != nil {
			return FailedObserve(), err
		}
	}

	if status.GetOperationRef() != "" {
		// LRO in flight — report not-up-to-date so the reconciler re-enters DeployAction to
		// resume the poll loop. Do not trigger an OBSERVE call; externalRef may be empty.
		return NewObserve(httpClient.HttpDetails{}, nil, false), nil
	}

	spec := crCtx.Spec()
	mapping, err := requestmapping.GetMapping(spec, common.ActionObserve, svcCtx.Logger)
	if err != nil {
		return FailedObserve(), err
	}

	objectNotCreated := !isObjectValidForObservation(crCtx)

	// Evaluate the HTTP request template. If successfully templated, attempt to
	// observe the resource.
	requestDetails, err := requestgen.GenerateValidRequestDetails(svcCtx, crCtx, mapping)
	if err != nil {
		if objectNotCreated {
			// The initial request was not successfully templated. Cannot
			// confirm existence of the resource, jumping to the default
			// behavior of creating before observing.
			err = errors.New(observe.ErrObjectNotFound)
		}
		return FailedObserve(), err
	}

	details, responseErr := svcCtx.HTTP.SendRequest(svcCtx.Ctx, requestmapping.GetEffectiveMethod(mapping), requestDetails.Url, requestDetails.Body, requestDetails.Headers, svcCtx.TLSConfigData)
	// The initial observation of an object requires a successful HTTP response
	// to be considered existing.
	if !utils.IsHTTPSuccess(details.HttpResponse.StatusCode) && objectNotCreated {
		// Cannot confirm existence of the resource, jumping to the default
		// behavior of creating before observing.
		return FailedObserve(), errors.New(observe.ErrObjectNotFound)
	}
	if err := determineIfRemoved(svcCtx, crCtx, details, responseErr); err != nil {
		return FailedObserve(), err
	}

	// Apply response data to secrets and update CR status with response
	secretConfigs := spec.GetSecretInjectionConfigs()
	datapatcher.ApplyResponseDataToSecrets(svcCtx.Ctx, svcCtx.LocalKube, svcCtx.Logger, &details.HttpResponse, secretConfigs, crCtx.GetCR())
	return determineIfUpToDate(svcCtx, crCtx, details, responseErr)
}

// determineIfUpToDate determines if the object is up to date based on the response check.
func determineIfUpToDate(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, details httpClient.HttpDetails, responseErr error) (ObserveRequestDetails, error) {
	responseChecker := observe.GetIsUpToDateResponseCheck(svcCtx, crCtx.Spec())
	if responseChecker == nil {
		return FailedObserve(), errors.Errorf(errExpectedResponseCheckType, "expectedResponseCheck")
	}

	result, err := responseChecker.Check(svcCtx, crCtx, details, responseErr)
	if err != nil {
		return FailedObserve(), err
	}

	return NewObserve(details, responseErr, result), nil
}

// determineIfRemoved determines if the object is removed based on the response check.
func determineIfRemoved(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, details httpClient.HttpDetails, responseErr error) error {
	responseChecker := observe.GetIsRemovedResponseCheck(svcCtx, crCtx.Spec())
	if responseChecker == nil {
		return errors.Errorf(errExpectedResponseCheckType, "isRemovedCheck")
	}

	return responseChecker.Check(svcCtx, crCtx, details, responseErr)
}

// clearTerminalError clears the terminal failure message so a spec-changed resource can be
// reconciled again. RetryOnConflict handles optimistic-lock conflicts from concurrent reconciles.
func clearTerminalError(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before clearing terminal error")
		}
		crCtx.StatusWriter().SetTerminalError("")
		return svcCtx.LocalKube.Status().Update(svcCtx.Ctx, resource)
	})
}

// isObjectValidForObservation returns true if the resource has been observed before.
// A resource is considered "already created" if either a prior response was
// stored (the original provider-http signal) OR externalRef is populated (set
// after the first async CREATE+poll cycle, or seeded from external-name for
// import). Without this second condition an async resource whose first OBSERVE
// returns non-2xx would be misidentified as non-existent and CREATE would re-fire.
func isObjectValidForObservation(crCtx *service.RequestCRContext) bool {
	response := crCtx.Status().GetResponse()
	requestDetails := crCtx.Status().GetRequestDetails()
	spec := crCtx.Spec()

	hasResponse := response.GetStatusCode() != 0 &&
		!(requestDetails.GetMethod() == http.MethodPost && utils.IsHTTPError(response.GetStatusCode(), spec.GetAllowedStatusCodes()))

	return hasResponse || crCtx.Status().GetExternalRefValue() != ""
}
