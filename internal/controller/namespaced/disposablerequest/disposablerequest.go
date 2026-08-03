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

	"github.com/Antrakos/provider-http-async/apis/common"
	"github.com/Antrakos/provider-http-async/apis/namespaced/disposablerequest/v1alpha2"
	apisv1alpha2 "github.com/Antrakos/provider-http-async/apis/namespaced/v1alpha2"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/disposablerequest"
	"github.com/Antrakos/provider-http-async/internal/utils"
)

const (
	errNotDisposableRequest              = "managed resource is not a namespaced AsyncDisposableRequest custom resource"
	errTrackPCUsage                      = "cannot track ProviderConfig usage"
	errNewHttpClient                     = "cannot create new Http client"
	errFailedToSendHttpDisposableRequest = "failed to send http request"
	errExtractCredentials                = "cannot extract credentials"
	errBuildTLSConfig                    = "failed to build TLS configuration"
	errInvalidAuthSelection              = "invalid auth configuration"

	errGetPC  = "cannot get ProviderConfig"
	errGetCPC = "cannot get ClusterProviderConfig"

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
	result, err := r.reconciler.Reconcile(ctx, req)

	cr := &v1alpha2.AsyncDisposableRequest{}
	if getErr := r.kube.Get(ctx, req.NamespacedName, cr); getErr != nil {
		return result, err
	}

	if cr.Status.Error != "" && utils.RollBackEnabled(cr.Spec.ForProvider.RollbackRetriesLimit) && utils.RetriesLimitReached(cr.Status.Failed, cr.Spec.ForProvider.RollbackRetriesLimit) {
		if ensureErr := r.ensureErrorObserved(ctx, cr); ensureErr != nil {
			return result, err
		}
	} else if clearErr := r.clearErrorObserved(ctx, cr); clearErr != nil {
		return result, err
	}

	return result, err
}

// ensureErrorObserved sets or updates the ErrorObserved condition to the current error message
func (r *errorConditionReconciler) ensureErrorObserved(ctx context.Context, cr *v1alpha2.AsyncDisposableRequest) error {
	for _, c := range cr.Status.Conditions {
		if c.Type == conditionTypeErrorObserved && c.Status == corev1.ConditionTrue && c.Message == cr.Status.Error {
			return nil
		}
	}

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

// Setup adds a controller that reconciles namespaced AsyncDisposableRequest managed resources.
func Setup(mgr ctrl.Manager, o controller.Options, timeout time.Duration) error {
	name := managed.ControllerName(v1alpha2.AsyncDisposableRequestGroupKind)

	reconcilerOptions := []managed.ReconcilerOption{
		managed.WithExternalConnecter(&connector{
			logger:          o.Logger,
			kube:            mgr.GetClient(),
			usage:           resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha2.ProviderConfigUsage{}),
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
	usage           *resource.ProviderConfigUsageTracker
	newHttpClientFn func(log logging.Logger, timeout time.Duration, creds string) (httpClient.Client, error)
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha2.AsyncDisposableRequest)
	if !ok {
		return nil, errors.New(errNotDisposableRequest)
	}

	l := c.logger.WithValues("namespacedDisposableRequest", cr.Name)

	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	if cr.GetProviderConfigReference() == nil {
		cr.SetProviderConfigReference(&xpv2.ProviderConfigReference{
			Name: defaultProviderConfig,
			Kind: "ClusterProviderConfig",
		})
		l.Debug("No providerConfigRef specified, defaulting to 'default'")
	}

	cd, providerTLS, providerOIDC, providerGCP, err := c.resolveProviderConfig(ctx, mg)
	if err != nil {
		return nil, err
	}

	h, err := c.buildHTTPClient(ctx, l, cr, cd, providerOIDC, providerGCP)
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

	tlsConfigData, err := c.buildTLSConfigData(ctx, cr, providerTLS)
	if err != nil {
		return nil, errors.Wrap(err, errBuildTLSConfig)
	}

	return &external{
		localKube:     c.kube,
		logger:        l,
		http:          h,
		tlsConfigData: tlsConfigData,
	}, nil
}

// resolveProviderConfig fetches credentials, TLS, OIDC, and GCP from the referenced
// ProviderConfig or ClusterProviderConfig.
func (c *connector) resolveProviderConfig(ctx context.Context, mg resource.Managed) (*apisv1alpha2.ProviderCredentials, *common.TLSConfig, *common.OIDCConfig, *common.GCPAuth, error) {
	m := mg.(resource.ModernManaged)
	ref := m.GetProviderConfigReference()
	switch ref.Kind {
	case "ProviderConfig":
		pc := &apisv1alpha2.ProviderConfig{}
		if err := c.kube.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: m.GetNamespace()}, pc); err != nil {
			return nil, nil, nil, nil, errors.Wrap(err, errGetPC)
		}
		return pc.Spec.Credentials, pc.Spec.TLS, pc.Spec.OIDC, pc.Spec.GCP, nil
	case "ClusterProviderConfig":
		cpc := &apisv1alpha2.ClusterProviderConfig{}
		if err := c.kube.Get(ctx, types.NamespacedName{Name: ref.Name}, cpc); err != nil {
			return nil, nil, nil, nil, errors.Wrap(err, errGetCPC)
		}
		return cpc.Spec.Credentials, cpc.Spec.TLS, cpc.Spec.OIDC, cpc.Spec.GCP, nil
	default:
		return nil, nil, nil, nil, errors.Errorf("unsupported provider config kind: %s", ref.Kind)
	}
}

// buildHTTPClient constructs the HTTP client, resolving credentials and wrapping it with
// an identity block (OIDC or GCP) when set. It enforces the auth-selection rules by
// returning an httpClient.AuthSelectionError, which the caller surfaces as Synced=False.
func (c *connector) buildHTTPClient(ctx context.Context, l logging.Logger, cr *v1alpha2.AsyncDisposableRequest, cd *apisv1alpha2.ProviderCredentials, providerOIDC *common.OIDCConfig, providerGCP *common.GCPAuth) (httpClient.Client, error) {
	effectiveGCP := common.MergeGCPConfigs(cr.Spec.ForProvider.GCP, providerGCP)
	effectiveOIDC := common.MergeOIDCConfigs(cr.Spec.ForProvider.OIDC, providerOIDC)

	if err := httpClient.ValidateAuthSelection(httpClient.AuthSelection{
		CredentialsSet: cd != nil,
		GCP:            effectiveGCP,
		OIDC:           effectiveOIDC,
	}); err != nil {
		return nil, err
	}

	creds := ""
	if cd != nil && cd.Source == xpv2.CredentialsSourceSecret {
		data, err := resource.CommonCredentialExtractor(ctx, cd.Source, c.kube, cd.CommonCredentialSelectors)
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

// buildTLSConfigData merges TLS configuration from the resource and provider, then loads cert data.
func (c *connector) buildTLSConfigData(ctx context.Context, cr *v1alpha2.AsyncDisposableRequest, providerTLS *common.TLSConfig) (*httpClient.TLSConfigData, error) {
	merged := httpClient.MergeTLSConfigs(cr.Spec.ForProvider.TLSConfig, providerTLS)
	if cr.Spec.ForProvider.InsecureSkipTLSVerify {
		if merged == nil {
			merged = &common.TLSConfig{}
		}
		merged.InsecureSkipVerify = true
	}
	return httpClient.LoadTLSConfig(ctx, c.kube, merged)
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

	isUpToDate := true
	if needsRetry {
		if cr.Spec.ForProvider.NextReconcile != nil {
			nextReconcileDuration := cr.Spec.ForProvider.NextReconcile.Duration
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
			isUpToDate = false
		}
	}

	isAvailable := isUpToDate

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
		if cr.Spec.ForProvider.NextReconcile != nil {
			nextReconcileDuration := cr.Spec.ForProvider.NextReconcile.Duration
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

	if !needsRetry && cr.Spec.ForProvider.NextReconcile != nil {
		nextReconcileDuration := cr.Spec.ForProvider.NextReconcile.Duration
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

func customPollIntervalHook(mg resource.Managed, _ time.Duration) time.Duration {
	defaultPollInterval := 30 * time.Second

	cr, ok := mg.(*v1alpha2.AsyncDisposableRequest)
	if !ok {
		return defaultPollInterval
	}

	if cr.Spec.ForProvider.NextReconcile == nil {
		return defaultPollInterval
	}

	nextReconcileDuration := cr.Spec.ForProvider.NextReconcile.Duration
	lastReconcileTime := cr.Status.LastReconcileTime.Time
	if lastReconcileTime.IsZero() {
		lastReconcileTime = time.Now()
	}
	nextReconcileTime := lastReconcileTime.Add(nextReconcileDuration)

	now := time.Now()
	if now.Before(nextReconcileTime) {
		return nextReconcileTime.Sub(now)
	}

	return defaultPollInterval
}
