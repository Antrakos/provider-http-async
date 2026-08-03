/*
Copyright 2023 The Crossplane Authors.

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
	"context"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/feature"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"

	"github.com/Antrakos/provider-http-async/apis/cluster/disposablerequest/v1alpha2"
	apisv1alpha1 "github.com/Antrakos/provider-http-async/apis/cluster/v1alpha1"
	"github.com/Antrakos/provider-http-async/apis/common"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/disposablerequest"
	"github.com/Antrakos/provider-http-async/internal/utils"
)

const (
	errNotDisposableRequest              = "managed resource is not an AsyncDisposableRequest custom resource"
	errTrackPCUsage                      = "cannot track ProviderConfig usage"
	errNewHttpClient                     = "cannot create new Http client"
	errProviderNotRetrieved              = "provider could not be retrieved"
	errFailedToSendHttpDisposableRequest = "failed to send http request"
	errExtractCredentials                = "cannot extract credentials"
	errInvalidAuthSelection              = "invalid auth configuration"

	conditionTypeErrorObserved = "ErrorObserved"

	defaultProviderConfig = "default"
)

// errorConditionReconciler wraps a reconciler to add ErrorObserved condition after reconciliation
type errorConditionReconciler struct {
	reconciler reconcile.Reconciler
	kube       client.Client
}

// Reconcile implements reconcile.Reconciler
func (r *errorConditionReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	// Call the managed reconciler
	result, err := r.reconciler.Reconcile(ctx, req)

	// After reconciliation, check if we need to add ErrorObserved condition
	cr := &v1alpha2.AsyncDisposableRequest{}
	if getErr := r.kube.Get(ctx, req.NamespacedName, cr); getErr != nil {
		// If we can't get the resource, just return the original result
		return result, err
	}

	if cr.Status.Error != "" && utils.RollBackEnabled(cr.Spec.ForProvider.RollbackRetriesLimit) && utils.RetriesLimitReached(cr.Status.Failed, cr.Spec.ForProvider.RollbackRetriesLimit) {
		// Ensure the ErrorObserved condition is present and current
		if ensureErr := r.ensureErrorObserved(ctx, cr); ensureErr != nil {
			// Log but don't fail the reconciliation
			return result, err
		}
	} else if clearErr := r.clearErrorObserved(ctx, cr); clearErr != nil {
		// Recovered (or not yet terminal): drop any stale ErrorObserved condition.
		return result, err
	}

	return result, err
}

// ensureErrorObserved sets or updates the ErrorObserved condition to the current error message
func (r *errorConditionReconciler) ensureErrorObserved(ctx context.Context, cr *v1alpha2.AsyncDisposableRequest) error {
	// Check if ErrorObserved condition already exists and is current
	for _, c := range cr.Status.Conditions {
		if c.Type == conditionTypeErrorObserved && c.Status == corev1.ConditionTrue && c.Message == cr.Status.Error {
			return nil
		}
	}

	// Update conditions
	conditions := cr.Status.Conditions
	foundIndex := -1
	for i, c := range conditions {
		if c.Type == conditionTypeErrorObserved {
			foundIndex = i
			break
		}
	}

	errorCondition := xpv2.Condition{
		Type:               conditionTypeErrorObserved,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "RetriesExhausted",
		Message:            cr.Status.Error,
	}

	if foundIndex >= 0 {
		conditions[foundIndex] = errorCondition
	} else {
		conditions = append(conditions, errorCondition)
	}

	cr.SetConditions(conditions...)

	// Update the resource status
	return r.kube.Status().Update(ctx, cr)
}

// clearErrorObserved removes a previously-set ErrorObserved condition once the resource
// is no longer in a terminal-error state (e.g. after a successful retry).
func (r *errorConditionReconciler) clearErrorObserved(ctx context.Context, cr *v1alpha2.AsyncDisposableRequest) error {
	filtered := cr.Status.Conditions[:0]
	removed := false
	for _, c := range cr.Status.Conditions {
		if c.Type == conditionTypeErrorObserved {
			removed = true
			continue
		}
		filtered = append(filtered, c)
	}
	if !removed {
		return nil
	}
	cr.Status.Conditions = filtered
	return r.kube.Status().Update(ctx, cr)
}

// Setup adds a controller that reconciles AsyncDisposableRequest managed resources.
func Setup(mgr ctrl.Manager, o controller.Options, timeout time.Duration) error {
	name := managed.ControllerName(v1alpha2.AsyncDisposableRequestGroupKind)

	reconcilerOptions := []managed.ReconcilerOption{
		managed.WithExternalConnecter(&connector{
			logger:          o.Logger,
			kube:            mgr.GetClient(),
			usage:           resource.NewLegacyProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
			newHttpClientFn: httpClient.NewClient,
		}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		WithCustomPollIntervalHook(),
		managed.WithTimeout(timeout),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))),
		managed.WithDeterministicExternalName(true),
	}

	if o.Features.Enabled(feature.EnableBetaManagementPolicies) {
		reconcilerOptions = append(reconcilerOptions, managed.WithManagementPolicies())
	}

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha2.AsyncDisposableRequestGroupVersionKind),
		reconcilerOptions...,
	)

	// Wrap the reconciler to add ErrorObserved condition after managed reconciler completes
	wrappedReconciler := &errorConditionReconciler{
		reconciler: ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter),
		kube:       mgr.GetClient(),
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha2.AsyncDisposableRequest{}).
		Complete(wrappedReconciler)
}

type connector struct {
	logger          logging.Logger
	kube            client.Client
	usage           *resource.LegacyProviderConfigUsageTracker
	newHttpClientFn func(log logging.Logger, timeout time.Duration, creds string) (httpClient.Client, error)
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha2.AsyncDisposableRequest)
	if !ok {
		return nil, errors.New(errNotDisposableRequest)
	}

	l := c.logger.WithValues("disposableRequest", cr.Name)

	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	if cr.GetProviderConfigReference() == nil {
		cr.SetProviderConfigReference(&xpv2.Reference{Name: defaultProviderConfig})
		l.Debug("No providerConfigRef specified, defaulting to 'default'")
	}

	pc := &apisv1alpha1.ProviderConfig{}
	n := types.NamespacedName{Name: cr.GetProviderConfigReference().Name}
	if err := c.kube.Get(ctx, n, pc); err != nil {
		return nil, errors.Wrap(err, errProviderNotRetrieved)
	}

	h, err := c.buildHTTPClient(ctx, l, cr, pc)
	if err != nil {
		var authErr *httpClient.AuthSelectionError
		if errors.As(err, &authErr) {
			cr.Status.SetConditions(
				xpv2.Unavailable().WithMessage(authErr.Error()),
				xpv2.ReconcileError(errors.Wrap(err, errInvalidAuthSelection)),
			)
			_ = c.kube.Status().Update(ctx, cr)
			return nil, errors.Wrap(err, errInvalidAuthSelection)
		}
		return nil, err
	}

	// Merge TLS configs: resource-level overrides provider-level
	mergedTLSConfig := httpClient.MergeTLSConfigs(cr.Spec.ForProvider.TLSConfig, pc.Spec.TLS)

	// Apply InsecureSkipTLSVerify from AsyncDisposableRequest spec if set
	if cr.Spec.ForProvider.InsecureSkipTLSVerify {
		if mergedTLSConfig == nil {
			mergedTLSConfig = &common.TLSConfig{}
		}
		mergedTLSConfig.InsecureSkipVerify = true
	}

	// Load TLS configuration from secrets
	tlsConfigData, err := httpClient.LoadTLSConfig(ctx, c.kube, mergedTLSConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load TLS configuration")
	}

	return &external{
		localKube:     c.kube,
		logger:        l,
		http:          h,
		tlsConfigData: tlsConfigData,
	}, nil
}

// buildHTTPClient constructs the HTTP client, resolving credentials and wrapping it with
// an identity block (OIDC or GCP) when set. It enforces the auth-selection rules by
// returning an httpClient.AuthSelectionError, which the caller surfaces as Synced=False.
func (c *connector) buildHTTPClient(ctx context.Context, l logging.Logger, cr *v1alpha2.AsyncDisposableRequest, pc *apisv1alpha1.ProviderConfig) (httpClient.Client, error) {
	effectiveGCP := common.MergeGCPConfigs(cr.Spec.ForProvider.GCP, pc.Spec.GCP)
	effectiveOIDC := common.MergeOIDCConfigs(cr.Spec.ForProvider.OIDC, pc.Spec.OIDC)

	if err := httpClient.ValidateAuthSelection(httpClient.AuthSelection{
		CredentialsSet: pc.Spec.Credentials != nil,
		GCP:            effectiveGCP,
		OIDC:           effectiveOIDC,
	}); err != nil {
		return nil, err
	}

	creds := ""
	if pc.Spec.Credentials != nil && pc.Spec.Credentials.Source == xpv2.CredentialsSourceSecret {
		data, err := resource.CommonCredentialExtractor(ctx, pc.Spec.Credentials.Source, c.kube, pc.Spec.Credentials.CommonCredentialSelectors)
		if err != nil {
			return nil, errors.Wrap(err, errExtractCredentials)
		}
		creds = string(data)
	}

	h, err := c.newHttpClientFn(l, utils.WaitTimeout(cr.Spec.ForProvider.WaitTimeout), creds)
	if err != nil {
		return nil, errors.Wrap(err, errNewHttpClient)
	}

	// gcp and oidc are validated as mutually exclusive above; at most one is set.
	if effectiveGCP != nil {
		h, err = httpClient.NewGCPClient(ctx, h, effectiveGCP)
		if err != nil {
			return nil, errors.Wrap(err, errNewHttpClient)
		}
	} else if effectiveOIDC != nil {
		h = httpClient.NewOIDCClient(h, effectiveOIDC)
	}
	return h, nil
}

type external struct {
	localKube     client.Client
	logger        logging.Logger
	http          httpClient.Client
	tlsConfigData *httpClient.TLSConfigData
}

// Observe checks the state of the AsyncDisposableRequest resource and updates its status accordingly.
//
//gocyclo:ignore
func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha2.AsyncDisposableRequest)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotDisposableRequest)
	}

	if meta.WasDeleted(mg) {
		c.logger.Debug("AsyncDisposableRequest is being deleted, skipping observation and secret injection")
		return managed.ExternalObservation{
			ResourceExists: false,
		}, nil
	}

	// Check if retries are needed (error occurred and haven't exhausted retries)
	needsRetry := utils.ShouldRetry(cr.Spec.ForProvider.RollbackRetriesLimit, cr.Status.Failed) && !utils.RetriesLimitReached(cr.Status.Failed, cr.Spec.ForProvider.RollbackRetriesLimit)

	// Terminal failure: rollbackRetriesLimit is set and exhausted. Stop re-issuing the
	// request and surface Ready=Unavailable instead of churning no-op Creates (which would
	// otherwise leave the standard Synced condition misleadingly True). Raising
	// rollbackRetriesLimit later reopens the retry path.
	if !cr.Status.Synced && cr.Status.Error != "" &&
		utils.RollBackEnabled(cr.Spec.ForProvider.RollbackRetriesLimit) &&
		utils.RetriesLimitReached(cr.Status.Failed, cr.Spec.ForProvider.RollbackRetriesLimit) {
		if err := disposablerequest.MarkResourceUnavailable(ctx, cr, c.localKube, cr.Status.Error); err != nil {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{
			ResourceExists:   true,
			ResourceUpToDate: true,
		}, nil
	}

	// For retries, respect nextReconcile timing if configured
	isUpToDate := true
	if needsRetry {
		if cr.Spec.ForProvider.NextReconcile != nil {
			// Only retry if enough time has passed since last reconcile
			nextReconcileDuration := common.ParseDuration(cr.Spec.ForProvider.NextReconcile, 0)
			last := cr.Status.LastReconcileTime.Time
			if last.IsZero() {
				last = time.Now()
			}
			if !time.Now().Before(last.Add(nextReconcileDuration)) {
				c.logger.Debug("NextReconcile time reached and retry needed, marking resource as not up-to-date")
				isUpToDate = false
			} else {
				c.logger.Debug("Retry needed but nextReconcile time not yet reached, keeping resource up-to-date")
			}
		} else {
			// No nextReconcile configured, allow immediate retry
			isUpToDate = false
		}
	}

	isAvailable := isUpToDate

	// If the resource is not yet marked as synced, we would normally trigger
	// a Create (or Update) which causes an immediate deployment. However,
	// when a retry is pending and the configured NextReconcile time has not yet
	// been reached we should avoid triggering an immediate deployment.
	if !cr.Status.Synced {
		if needsRetry && isUpToDate {
			c.logger.Debug("Retry pending and nextReconcile not reached; suppressing Create to respect NextReconcile timing")
			return managed.ExternalObservation{
				ResourceExists:   true,
				ResourceUpToDate: true,
			}, nil
		}

		return managed.ExternalObservation{
			ResourceExists: false,
		}, nil
	}

	svcCtx := service.NewServiceContext(ctx, c.localKube, c.logger, c.http, c.tlsConfigData)
	crCtx := service.NewDisposableRequestCRContext(cr)
	isExpected, storedResponse, err := disposablerequest.ValidateStoredResponse(svcCtx, crCtx)
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	if !isExpected {
		c.logger.Debug("Response does not match expected criteria")
		// Respect nextReconcile timing even for validation failures
		if cr.Spec.ForProvider.NextReconcile != nil {
			nextReconcileDuration := common.ParseDuration(cr.Spec.ForProvider.NextReconcile, 0)
			last := cr.Status.LastReconcileTime.Time
			if last.IsZero() {
				last = time.Now()
			}
			if time.Now().Before(last.Add(nextReconcileDuration)) {
				c.logger.Debug("Validation failed but nextReconcile time not yet reached, keeping resource up-to-date")
				return managed.ExternalObservation{
					ResourceExists:   isAvailable,
					ResourceUpToDate: true,
				}, nil
			}
		}
		c.logger.Debug("Validation failed and nextReconcile time reached (or not configured), marking resource as not up-to-date")
		return managed.ExternalObservation{
			ResourceExists:   isAvailable,
			ResourceUpToDate: false,
		}, nil
	}

	isUpToDate = disposablerequest.CalculateUpToDateStatus(crCtx, isUpToDate)

	// If nextReconcile is configured and no retry is pending, check if regular reconcile time has passed
	if !needsRetry && cr.Spec.ForProvider.NextReconcile != nil {
		nextReconcileDuration := common.ParseDuration(cr.Spec.ForProvider.NextReconcile, 0)
		last := cr.Status.LastReconcileTime.Time
		if last.IsZero() {
			last = time.Now()
		}
		if !time.Now().Before(last.Add(nextReconcileDuration)) {
			c.logger.Debug("NextReconcile time reached, marking resource as not up-to-date to force deployment")
			isUpToDate = false
		}
	}

	if isAvailable {
		if err := disposablerequest.UpdateResourceStatus(ctx, cr, c.localKube); err != nil {
			return managed.ExternalObservation{}, err
		}
	}

	if len(cr.Spec.ForProvider.SecretInjectionConfigs) > 0 && cr.Status.Response.StatusCode != 0 {
		disposablerequest.ApplySecretInjectionsFromStoredResponse(svcCtx, crCtx, storedResponse)
	}

	return managed.ExternalObservation{
		ResourceExists:    isAvailable,
		ResourceUpToDate:  isUpToDate,
		ConnectionDetails: nil,
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha2.AsyncDisposableRequest)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotDisposableRequest)
	}

	if err := utils.IsRequestValid(cr.Spec.ForProvider.Method, cr.Spec.ForProvider.URL); err != nil {
		return managed.ExternalCreation{}, err
	}

	svcCtx := service.NewServiceContext(ctx, c.localKube, c.logger, c.http, c.tlsConfigData)
	crCtx := service.NewDisposableRequestCRContext(cr)
	return managed.ExternalCreation{}, errors.Wrap(disposablerequest.DeployAction(svcCtx, crCtx), errFailedToSendHttpDisposableRequest)
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha2.AsyncDisposableRequest)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotDisposableRequest)
	}

	if err := utils.IsRequestValid(cr.Spec.ForProvider.Method, cr.Spec.ForProvider.URL); err != nil {
		return managed.ExternalUpdate{}, err
	}

	svcCtx := service.NewServiceContext(ctx, c.localKube, c.logger, c.http, c.tlsConfigData)
	crCtx := service.NewDisposableRequestCRContext(cr)
	return managed.ExternalUpdate{}, errors.Wrap(disposablerequest.DeployAction(svcCtx, crCtx), errFailedToSendHttpDisposableRequest)
}

func (c *external) Delete(_ context.Context, _ resource.Managed) (managed.ExternalDelete, error) {
	return managed.ExternalDelete{}, nil
}

// Disconnect does nothing. It never returns an error.
func (c *external) Disconnect(_ context.Context) error {
	return nil
}

// WithCustomPollIntervalHook returns a managed.ReconcilerOption that sets a custom poll interval based on the AsyncDisposableRequest spec.
func WithCustomPollIntervalHook() managed.ReconcilerOption {
	return managed.WithPollIntervalHook(customPollIntervalHook)
}

// customPollIntervalHook computes the duration until the next reconcile based on the
// AsyncDisposableRequest's spec and status. If LastReconcileTime is zero (not yet observed),
// treat it as now to avoid premature short-interval requeues.
func customPollIntervalHook(mg resource.Managed, _ time.Duration) time.Duration {
	defaultPollInterval := 30 * time.Second

	cr, ok := mg.(*v1alpha2.AsyncDisposableRequest)
	if !ok {
		return defaultPollInterval
	}

	if cr.Spec.ForProvider.NextReconcile == nil {
		return defaultPollInterval
	}

	// Calculate next reconcile time based on NextReconcile duration
	nextReconcileDuration := common.ParseDuration(cr.Spec.ForProvider.NextReconcile, 0)
	lastReconcileTime := cr.Status.LastReconcileTime.Time
	if lastReconcileTime.IsZero() {
		// Status update may not have propagated yet; consider last reconcile as now.
		lastReconcileTime = time.Now()
	}
	nextReconcileTime := lastReconcileTime.Add(nextReconcileDuration)

	// Determine if the current time is past the next reconcile time
	now := time.Now()
	if now.Before(nextReconcileTime) {
		// If not yet time to reconcile, calculate remaining time
		return nextReconcileTime.Sub(now)
	}

	// Default poll interval if the next reconcile time is in the past
	return defaultPollInterval
}
