package request

import (
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"github.com/Antrakos/provider-http-async/apis/common"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	datapatcher "github.com/Antrakos/provider-http-async/internal/data-patcher"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/request/observe"
	"github.com/Antrakos/provider-http-async/internal/service/request/polling"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestgen"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestmapping"
	"github.com/Antrakos/provider-http-async/internal/utils"
)

const (
	errNotValidJSON              = "%s is not a valid JSON string: %s"
	errConvertResToMap           = "failed to convert response to map"
	errExpectedResponseCheckType = "%s.Type should be either DEFAULT, CUSTOM or empty"

	// errUpdateMappingNotFound is the terminal message surfaced when OBSERVE
	// determines the resource is out of sync (expectedResponseCheck: false) but
	// no UPDATE or PUT mapping is configured. The reconciler would otherwise
	// silently skip the update and report ReconcileSuccess, hiding the stuck
	// state. Surfacing it as a terminal error makes both conditions honest:
	// Ready=False (Terminal error: ...) and Synced=False (reconcile error).
	errUpdateMappingNotFound = "no UPDATE or PUT mapping is configured but the resource is out of sync (expectedResponseCheck returned false); add an UPDATE mapping or fix the spec so the resource reconciles"
)

type ObserveRequestDetails struct {
	Details       httpClient.HttpDetails
	ResponseError error
	Synced        bool
	// TerminalError, when non-empty, means the resource is in a stable terminal
	// failure state (a prior poll failed with polling.error non-null, and the spec
	// has not changed since). The controller must report the resource unhealthy
	// (Ready=False) and must NOT re-trigger Create/Update — it returns this message
	// as a Go error so the managed reconciler reports Synced=False and persists it.
	TerminalError string
	// InFlight is true when a long-running operation poll is in progress
	// (status.polling.response != nil). The resource is not settled, so the
	// controller reports Ready=False rather than Available.
	InFlight bool
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
// unhealthy, carrying the failure message. The controller surfaces it as Ready=False
// and returns it as an error so the reconciler reports Synced=False.
func NewTerminalObserve(terminalErr string) ObserveRequestDetails {
	return ObserveRequestDetails{
		TerminalError: terminalErr,
	}
}

// NewInFlightObserve returns an observation representing an in-flight long-running
// operation: not up to date (the controller re-enters DeployAction to resume the poll)
// and not settled (the controller reports Ready=False rather than Available).
func NewInFlightObserve() ObserveRequestDetails {
	return ObserveRequestDetails{
		Synced:   false,
		InFlight: true,
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
// When a polling.response anchor is present an async mutate LRO is still in flight; OBSERVE must
// not fire because externalRef may not yet be written. The resume is routed back to DeployAction:
//
//   - If externalRef is not yet set (the operation has not completed — the CREATE+poll case),
//     report ErrObjectNotFound so the controller calls Create() and resumes via the CREATE
//     mapping. This matters for resources that declare no UPDATE mapping (e.g. Vertex AI
//     deployedModel, which has only CREATE/OBSERVE/REMOVE): routing through Update() would hit
//     a missing UPDATE mapping and the poll would never resume.
//   - If externalRef is set (an UPDATE/REMOVE LRO in flight, or a re-poll after completion was
//     interrupted), report not-up-to-date so the controller calls Update() (or holds the
//     finalizer for Delete), whose mappings resolve via .status.externalRef.
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

	if status.GetPollingResponse() != nil {
		// LRO in flight — do not trigger an OBSERVE call; externalRef may be empty. Route the
		// resume back to DeployAction.
		if status.GetExternalRefValue() == "" {
			// No externalRef yet: the CREATE+poll operation is still running. Report the
			// resource as not-existing so the controller calls Create() (the CREATE mapping
			// always exists for a pollable resource) and resumes the poll via the anchor.
			return FailedObserve(), errors.New(observe.ErrObjectNotFound)
		}
		// externalRef is set: an UPDATE/REMOVE LRO is in flight (or a re-poll after a
		// completed operation). Report not-up-to-date so the controller calls Update() /
		// holds the finalizer for Delete(), whose URLs resolve via .status.externalRef.
		// InFlight signals the controller to report Ready=False (still settling) rather
		// than Available.
		return NewInFlightObserve(), nil
	}

	spec := crCtx.Spec()
	mapping, err := requestmapping.GetMapping(spec, common.ActionObserve, svcCtx.Logger)
	if err != nil {
		return FailedObserve(), err
	}

	objectNotCreated := !isObjectValidForObservation(crCtx)

	// Identity gate: an OBSERVE URL built as baseUrl + "/" + .status.externalRef is
	// meaningless while externalRef has never been set. With no identity ever established
	// (no prior response, no anchor) such a URL collapses onto the resource's *collection*
	// endpoint (e.g. .../models/), which a well-behaved API answers with 200 + a list body.
	// That 200 is indistinguishable from "the resource exists" using the HTTP status alone, so
	// the reconciler reads it as "exists but drifted" and routes to Update() — never Create().
	// The correct "resource does not exist" signal before any identity is established is the
	// absence of externalRef itself: report ErrObjectNotFound so the controller calls Create().
	//
	// This runs after the in-flight anchor gate above (which handles its own empty-externalRef
	// case for the CREATE+poll resume) and after the OBSERVE mapping is resolved, so a missing
	// OBSERVE mapping still surfaces its own error, and so the gate can check whether the URL
	// actually depends on externalRef. It applies only when the URL template references
	// .status.externalRef: a constant URL (or one keyed by .response.body.id) does not collapse
	// when externalRef is empty, so OBSERVE must run and answer on its own merits. It is gated
	// on objectNotCreated so a resource that already has a prior response / externalRef (an
	// established identity, or an import) still observes normally — only a never-identified
	// resource routes to Create() here. No HTTP call is made against the malformed URL.
	if objectNotCreated && status.GetExternalRefValue() == "" && urlReferencesExternalRef(mapping.GetURL()) {
		return FailedObserve(), errors.New(observe.ErrObjectNotFound)
	}

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

	// resourceExistsCheck (optional): decouples existence from drift for sub-resources
	// embedded in a parent OBSERVE response that always returns 2xx. When configured,
	// evaluate it against the OBSERVE response before drift detection. A false result
	// means the owned sub-resource is absent → CREATE (same signal the controller maps to
	// ResourceExists:false). A true result falls through to expectedResponseCheck (drift).
	// This is evaluated inside the OBSERVE path, after the in-flight anchor gate above, so
	// it never races an in-flight operation (which returns earlier).
	if existsChecker := observe.GetResourceExistsResponseCheck(spec); existsChecker != nil {
		exists, err := existsChecker.Check(svcCtx, crCtx, details, responseErr)
		if err != nil {
			return FailedObserve(), err
		}
		if !exists {
			// The sub-resource is absent even though the parent returned 2xx → CREATE.
			return FailedObserve(), errors.New(observe.ErrObjectNotFound)
		}
		// Exists: fall through to drift detection below.
	}

	// Apply response data to secrets and update CR status with response
	secretConfigs := spec.GetSecretInjectionConfigs()
	datapatcher.ApplyResponseDataToSecrets(svcCtx.Ctx, svcCtx.LocalKube, svcCtx.Logger, &details.HttpResponse, secretConfigs, crCtx.GetCR())
	return determineIfUpToDate(svcCtx, crCtx, details, responseErr)
}

// determineIfUpToDate determines if the object is up to date based on the response check.
// When the resource is out of sync (expectedResponseCheck: false) and no UPDATE/PUT mapping
// exists, it surfaces a terminal error instead of letting the reconciler silently skip the
// update and report ReconcileSuccess (which hides the stuck state).
func determineIfUpToDate(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, details httpClient.HttpDetails, responseErr error) (ObserveRequestDetails, error) {
	responseChecker := observe.GetIsUpToDateResponseCheck(svcCtx, crCtx.Spec())
	if responseChecker == nil {
		return FailedObserve(), errors.Errorf(errExpectedResponseCheckType, "expectedResponseCheck")
	}

	result, err := responseChecker.Check(svcCtx, crCtx, details, responseErr)
	if err != nil {
		return FailedObserve(), err
	}

	// Drift detected: the controller will call Update(). If there is no UPDATE/PUT
	// mapping, DeployAction would log "skipping operation" and return nil, and the
	// reconciler would report ReconcileSuccess — hiding a permanently stuck state. Surface
	// it as a terminal error so the controller reports Ready=False and returns an error
	// (→ Synced=False) instead. This applies to the CUSTOM check, which explicitly reports
	// drift (expectedResponseCheck: false). The DEFAULT check compares the response to the
	// UPDATE body, so with no UPDATE mapping it cannot detect drift and reports up-to-date
	// — the intended behavior for create-only resources (CREATE/OBSERVE only) that are in
	// sync; those are not a stuck state and must stay Ready=True. Persisting the terminal
	// (terminalError + observedGeneration) makes it stable: IsUpToDate short-circuits on the
	// next reconcile via GetTerminalError() and stops re-firing OBSERVE, and a spec change
	// (e.g. adding an UPDATE mapping) bumps the generation, clears the terminal via
	// clearTerminalError, and re-evaluates — the same recovery path as a poll terminal.
	if !result {
		if _, mapErr := requestmapping.GetMapping(crCtx.Spec(), common.ActionUpdate, svcCtx.Logger); mapErr != nil {
			// Configuration terminal (no UPDATE mapping), not a polling.error result: no
			// structured response value to retain, so pass nil — terminalError carries it.
			if persistErr := polling.SetTerminalFailure(svcCtx, crCtx, errUpdateMappingNotFound, nil); persistErr != nil {
				return FailedObserve(), persistErr
			}
			return NewTerminalObserve(errUpdateMappingNotFound), nil
		}
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
// reconciled again, and resets the poll StartedAt so the resumed operation gets a fresh
// polling.timeout budget (e.g. raising polling.timeout after a timeout then resumes with
// now + newTimeout rather than oldStart + newTimeout). RetryOnConflict handles optimistic-lock
// conflicts from concurrent reconciles.
func clearTerminalError(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext) error {
	resource := crCtx.GetCR()
	nn := types.NamespacedName{Name: resource.GetName(), Namespace: resource.GetNamespace()}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := svcCtx.LocalKube.Get(svcCtx.Ctx, nn, resource); err != nil {
			return errors.Wrap(err, "failed to get resource before clearing terminal error")
		}
		sw := crCtx.StatusWriter()
		sw.SetTerminalError("")
		sw.SetTerminalResponse(nil)
		sw.SetOperationStartedAt(nil)
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
		!(requestDetails.GetMethod() == http.MethodPost && utils.IsHTTPError(response.GetStatusCode(), spec.GetAllowedStatusCodes())) //nolint:staticcheck // De Morgan equivalent changes short-circuit behavior with 0 status code

	return hasResponse || crCtx.Status().GetExternalRefValue() != ""
}

// urlReferencesExternalRef reports whether the OBSERVE URL jq template depends on
// .status.externalRef. The identity gate routes a never-identified resource to Create() only
// when the URL would be malformed by an empty externalRef; a constant URL, or one keyed by
// .response.body.id, does not collapse and must be allowed to OBSERVE on its own merits.
func urlReferencesExternalRef(urlTemplate string) bool {
	return strings.Contains(urlTemplate, ".status.externalRef")
}
