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

package request

import (
	"context"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/feature"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"

	"github.com/Antrakos/provider-http-async/apis/common"
	"github.com/Antrakos/provider-http-async/apis/namespaced/request/v1alpha2"
	apisv1alpha2 "github.com/Antrakos/provider-http-async/apis/namespaced/v1alpha2"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/request"
	"github.com/Antrakos/provider-http-async/internal/service/request/observe"
	"github.com/Antrakos/provider-http-async/internal/service/request/statushandler"
	"github.com/Antrakos/provider-http-async/internal/utils"
)

const (
	errNotRequest              = "managed resource is not a namespaced AsyncRequest custom resource"
	errTrackPCUsage            = "cannot track ProviderConfig usage"
	errNewHttpClient           = "cannot create new HTTP client"
	errFailedToSendHttpRequest = "failed to execute HTTP action"
	errFailedToCheckIfUpToDate = "failed to check if request is up to date"
	errGetLatestVersion        = "failed to get the latest version of the resource"
	errExtractCredentials      = "cannot extract credentials"
	errBuildTLSConfig          = "failed to build TLS configuration"

	errGetPC  = "cannot get ProviderConfig"
	errGetCPC = "cannot get ClusterProviderConfig"

	defaultProviderConfig = "default"
)

// Setup adds a controller that reconciles namespaced AsyncRequest managed resources.
func Setup(mgr ctrl.Manager, o controller.Options, timeout time.Duration) error {
	name := managed.ControllerName(v1alpha2.RequestGroupKind)

	reconcilerOptions := []managed.ReconcilerOption{
		managed.WithExternalConnecter(&connector{
			logger:          o.Logger,
			kube:            mgr.GetClient(),
			usage:           resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha2.ProviderConfigUsage{}),
			newHttpClientFn: httpClient.NewClient,
		}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithTimeout(timeout),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))),
	}

	if o.Features.Enabled(feature.EnableBetaManagementPolicies) {
		reconcilerOptions = append(reconcilerOptions, managed.WithManagementPolicies())
	}

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha2.RequestGroupVersionKind),
		reconcilerOptions...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&v1alpha2.AsyncRequest{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// A connector is expected to produce an ExternalClient when its Connect method
// is called.
type connector struct {
	logger          logging.Logger
	kube            client.Client
	usage           *resource.ProviderConfigUsageTracker
	newHttpClientFn func(log logging.Logger, timeout time.Duration, creds string) (httpClient.Client, error)
}

// Connect creates a new external client using the provider config.
func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha2.AsyncRequest)
	if !ok {
		return nil, errors.New(errNotRequest)
	}

	l := c.logger.WithValues("namespacedRequest", cr.Name)

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

	cd, providerTLS, providerOIDC, err := c.resolveProviderConfig(ctx, mg)
	if err != nil {
		return nil, err
	}

	h, err := c.buildHTTPClient(ctx, l, cr, cd, providerOIDC)
	if err != nil {
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

// resolveProviderConfig fetches credentials, TLS, and OIDC from the referenced ProviderConfig or ClusterProviderConfig.
func (c *connector) resolveProviderConfig(ctx context.Context, mg resource.Managed) (apisv1alpha2.ProviderCredentials, *common.TLSConfig, *common.OIDCConfig, error) {
	m := mg.(resource.ModernManaged)
	ref := m.GetProviderConfigReference()
	switch ref.Kind {
	case "ProviderConfig":
		pc := &apisv1alpha2.ProviderConfig{}
		if err := c.kube.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: m.GetNamespace()}, pc); err != nil {
			return apisv1alpha2.ProviderCredentials{}, nil, nil, errors.Wrap(err, errGetPC)
		}
		return pc.Spec.Credentials, pc.Spec.TLS, pc.Spec.OIDC, nil
	case "ClusterProviderConfig":
		cpc := &apisv1alpha2.ClusterProviderConfig{}
		if err := c.kube.Get(ctx, types.NamespacedName{Name: ref.Name}, cpc); err != nil {
			return apisv1alpha2.ProviderCredentials{}, nil, nil, errors.Wrap(err, errGetCPC)
		}
		return cpc.Spec.Credentials, cpc.Spec.TLS, cpc.Spec.OIDC, nil
	default:
		return apisv1alpha2.ProviderCredentials{}, nil, nil, errors.Errorf("unsupported provider config kind: %s", ref.Kind)
	}
}

// buildHTTPClient constructs the HTTP client, optionally wrapping it with OIDC.
func (c *connector) buildHTTPClient(ctx context.Context, l logging.Logger, cr *v1alpha2.AsyncRequest, cd apisv1alpha2.ProviderCredentials, providerOIDC *common.OIDCConfig) (httpClient.Client, error) {
	data, err := resource.CommonCredentialExtractor(ctx, cd.Source, c.kube, cd.CommonCredentialSelectors)
	if err != nil {
		return nil, errors.Wrap(err, errExtractCredentials)
	}

	h, err := c.newHttpClientFn(l, utils.WaitTimeout(cr.Spec.ForProvider.WaitTimeout), string(data))
	if err != nil {
		return nil, errors.Wrap(err, errNewHttpClient)
	}

	if effectiveOIDC := common.MergeOIDCConfigs(cr.Spec.ForProvider.OIDC, providerOIDC); effectiveOIDC != nil {
		h = httpClient.NewOIDCClient(h, effectiveOIDC)
	}
	return h, nil
}

// buildTLSConfigData merges TLS configuration from the resource and provider, then loads cert data.
func (c *connector) buildTLSConfigData(ctx context.Context, cr *v1alpha2.AsyncRequest, providerTLS *common.TLSConfig) (*httpClient.TLSConfigData, error) {
	merged := httpClient.MergeTLSConfigs(cr.Spec.ForProvider.TLSConfig, providerTLS)
	if cr.Spec.ForProvider.InsecureSkipTLSVerify {
		if merged == nil {
			merged = &common.TLSConfig{}
		}
		merged.InsecureSkipVerify = true
	}
	return httpClient.LoadTLSConfig(ctx, c.kube, merged)
}

// An ExternalClient observes, then either creates, updates, or deletes an
// external resource to ensure it reflects the managed resource's desired state.
type external struct {
	localKube     client.Client
	logger        logging.Logger
	http          httpClient.Client
	tlsConfigData *httpClient.TLSConfigData
}

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha2.AsyncRequest)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotRequest)
	}

	svcCtx := service.NewServiceContext(ctx, c.localKube, c.logger, c.http, c.tlsConfigData)
	crCtx := service.NewRequestCRContext(cr)

	// Import / orphan-recovery: seed status.externalRef from crossplane.io/external-name
	// if it is not yet populated.
	if crCtx.Status().GetExternalRefValue() == "" {
		if extName := meta.GetExternalName(cr); extName != "" {
			crCtx.StatusWriter().SetExternalRef(extName)
			if err := c.localKube.Status().Update(ctx, cr); err != nil {
				return managed.ExternalObservation{}, errors.Wrap(err, "failed to seed externalRef from external-name annotation")
			}
		}
	}

	observeRequestDetails, err := request.IsUpToDate(svcCtx, crCtx)
	if err != nil && err.Error() == observe.ErrObjectNotFound {
		return managed.ExternalObservation{
			ResourceExists: false,
		}, nil
	}

	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errFailedToCheckIfUpToDate)
	}

	// Terminal failure: report the resource unhealthy but up-to-date so the reconciler
	// does not re-fire Create/Update. It stays in this state until the spec changes.
	if observeRequestDetails.TerminalError != "" {
		cr.Status.SetConditions(
			xpv2.Unavailable().WithMessage(observeRequestDetails.TerminalError),
			xpv2.ReconcileError(errors.New(observeRequestDetails.TerminalError)),
		)
		if updateErr := c.localKube.Status().Update(ctx, cr); updateErr != nil {
			return managed.ExternalObservation{}, errors.Wrap(updateErr, " failed updating status")
		}
		return managed.ExternalObservation{
			ResourceExists:   true,
			ResourceUpToDate: true,
		}, nil
	}

	statusHandler, err := statushandler.NewStatusHandler(svcCtx, crCtx, observeRequestDetails.Details, observeRequestDetails.ResponseError)
	if err != nil {
		return managed.ExternalObservation{}, err
	}

	synced := observeRequestDetails.Synced
	if synced {
		statusHandler.ResetFailures()
	}

	cr.Status.SetConditions(xpv2.Available())
	err = statusHandler.SetRequestStatus()
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, " failed updating status")
	}

	return managed.ExternalObservation{
		ResourceExists:    true,
		ResourceUpToDate:  synced,
		ConnectionDetails: nil,
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha2.AsyncRequest)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotRequest)
	}

	// Get the latest version of the resource before deploying
	if err := c.localKube.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, cr); err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errGetLatestVersion)
	}

	svcCtx := service.NewServiceContext(ctx, c.localKube, c.logger, c.http, c.tlsConfigData)
	crCtx := service.NewRequestCRContext(cr)
	return managed.ExternalCreation{}, errors.Wrap(request.DeployAction(svcCtx, crCtx, v1alpha2.ActionCreate), errFailedToSendHttpRequest)
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha2.AsyncRequest)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotRequest)
	}

	// Get the latest version of the resource before deploying
	if err := c.localKube.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, cr); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errGetLatestVersion)
	}

	svcCtx := service.NewServiceContext(ctx, c.localKube, c.logger, c.http, c.tlsConfigData)
	crCtx := service.NewRequestCRContext(cr)
	return managed.ExternalUpdate{}, errors.Wrap(request.DeployAction(svcCtx, crCtx, v1alpha2.ActionUpdate), errFailedToSendHttpRequest)
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) (managed.ExternalDelete, error) {
	cr, ok := mg.(*v1alpha2.AsyncRequest)
	if !ok {
		return managed.ExternalDelete{}, errors.New(errNotRequest)
	}

	// Get the latest version of the resource before deploying
	if err := c.localKube.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, cr); err != nil {
		return managed.ExternalDelete{}, errors.Wrap(err, errGetLatestVersion)
	}

	svcCtx := service.NewServiceContext(ctx, c.localKube, c.logger, c.http, c.tlsConfigData)
	crCtx := service.NewRequestCRContext(cr)
	return managed.ExternalDelete{}, errors.Wrap(request.DeployAction(svcCtx, crCtx, v1alpha2.ActionRemove), errFailedToSendHttpRequest)
}

// Disconnect does nothing. It never returns an error.
func (c *external) Disconnect(_ context.Context) error {
	return nil
}
