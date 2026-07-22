package requestgen

import (
	"fmt"
	"strings"

	"github.com/Antrakos/provider-http-async/apis/interfaces"
	httpClient "github.com/Antrakos/provider-http-async/internal/clients/http"
	datapatcher "github.com/Antrakos/provider-http-async/internal/data-patcher"
	json_util "github.com/Antrakos/provider-http-async/internal/json"
	"github.com/Antrakos/provider-http-async/internal/service"
	"github.com/Antrakos/provider-http-async/internal/service/request/requestprocessing"
	"github.com/Antrakos/provider-http-async/internal/utils"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
)

type RequestDetails struct {
	Url     string
	Body    httpClient.Data
	Headers httpClient.Data
}

// GenerateRequestDetails generates request details.
// status may be nil; when non-nil it exposes .status (externalRef/operationRef) to
// the URL, body, and header jq expressions, as required by the PRD jq-context contract.
func GenerateRequestDetails(svcCtx *service.ServiceContext, methodMapping interfaces.HTTPMapping, forProvider interfaces.MappedHTTPRequestSpec, status interfaces.RequestStatusReader, response interfaces.HTTPResponse) (RequestDetails, error, bool) {
	patchedResponse, err := datapatcher.PatchSecretsIntoResponse(svcCtx.Ctx, svcCtx.LocalKube, response, svcCtx.Logger)
	if err != nil {
		return RequestDetails{}, err, false
	}

	jqObject := GenerateRequestContext(forProvider, status, patchedResponse)
	url, err := generateURL(methodMapping.GetURL(), jqObject)
	if err != nil {
		return RequestDetails{}, err, false
	}

	if !utils.IsUrlValid(url) {
		return RequestDetails{}, errors.Errorf(utils.ErrInvalidURL, url), false
	}

	body, err := generateBody(svcCtx, methodMapping.GetBody(), jqObject)
	if err != nil {
		return RequestDetails{}, err, false
	}

	headersData, err := generateHeaders(svcCtx, coalesceHeaders(methodMapping, forProvider), jqObject)
	if err != nil {
		return RequestDetails{}, err, false
	}

	return RequestDetails{Body: body, Url: url, Headers: headersData}, nil, true
}

// GenerateRequestContext creates a JSON-compatible map from the specified AsyncRequest's ForProvider and Response fields.
// It merges the two maps, converts JSON strings to nested maps, and returns the resulting map.
// status may be nil (no .status key injected).
func GenerateRequestContext(forProvider interfaces.MappedHTTPRequestSpec, status interfaces.RequestStatusReader, patchedResponse interfaces.HTTPResponse) map[string]interface{} {
	return GenerateRequestContextForPoll(forProvider, status, patchedResponse, nil)
}

// GenerateRequestContextForPoll builds the jq object with optional .status and .poll.response injection.
// status may be nil (no .status key injected). pollResponse may be nil (no .poll key injected).
func GenerateRequestContextForPoll(
	forProvider interfaces.MappedHTTPRequestSpec,
	status interfaces.RequestStatusReader,
	patchedResponse interfaces.HTTPResponse,
	pollResponse map[string]interface{},
) map[string]interface{} {
	baseMap, _ := json_util.StructToMap(forProvider)
	responseMap, _ := json_util.StructToMap(map[string]interface{}{
		"response": patchedResponse,
	})

	maps.Copy(baseMap, responseMap)

	if status != nil {
		statusMap := map[string]interface{}{
			"externalRef":  status.GetExternalRefValue(),
			"operationRef": status.GetOperationRef(),
		}
		baseMap["status"] = statusMap
	}

	if pollResponse != nil {
		baseMap["poll"] = map[string]interface{}{
			"response": pollResponse,
		}
	}

	json_util.ConvertJSONStringsToMaps(&baseMap)

	if resp, ok := baseMap["response"].(map[string]interface{}); ok {
		if _, exists := resp["headers"]; !exists {
			resp["headers"] = nil
		}
	}

	return baseMap
}

// GenerateRequestContextFromMap builds the jq object using a raw mutate-response map
// (already in {body, headers, statusCode} form) rather than an interfaces.HTTPResponse.
// This is the path used by the poll loop, where .response is the stable mutate response
// carried as a map across iterations. Injecting it through StructToMap on a wrapper type
// would drop the body (unexported fields serialize to {}), so it is merged directly.
func GenerateRequestContextFromMap(
	forProvider interfaces.MappedHTTPRequestSpec,
	status interfaces.RequestStatusReader,
	responseMap map[string]interface{},
	pollResponse map[string]interface{},
) map[string]interface{} {
	baseMap, _ := json_util.StructToMap(forProvider)

	if responseMap == nil {
		responseMap = map[string]interface{}{}
	}
	baseMap["response"] = responseMap

	if status != nil {
		baseMap["status"] = map[string]interface{}{
			"externalRef":  status.GetExternalRefValue(),
			"operationRef": status.GetOperationRef(),
		}
	}

	if pollResponse != nil {
		baseMap["poll"] = map[string]interface{}{
			"response": pollResponse,
		}
	}

	json_util.ConvertJSONStringsToMaps(&baseMap)

	if resp, ok := baseMap["response"].(map[string]interface{}); ok {
		if _, exists := resp["headers"]; !exists {
			resp["headers"] = nil
		}
	}

	return baseMap
}

// GenerateValidRequestDetails generates valid request details based on the given AsyncRequest resource and Mapping configuration.
// It first attempts to generate request details using the HTTP response stored in the AsyncRequest's status. If the generated
// details are valid, the function returns them. If not, it falls back to using the cached response in the AsyncRequest's status
// and attempts to generate request details again. The function returns the generated request details or an error if the
// generation process fails.
func GenerateValidRequestDetails(svcCtx *service.ServiceContext, crCtx *service.RequestCRContext, mapping interfaces.HTTPMapping) (RequestDetails, error) {
	spec := crCtx.Spec()
	status := crCtx.Status()
	response := status.GetResponse()
	cachedResponse := crCtx.CachedResponse().GetCachedResponse()

	requestDetails, _, ok := GenerateRequestDetails(svcCtx, mapping, spec, status, response)
	if IsRequestValid(requestDetails) && ok {
		return requestDetails, nil
	}

	requestDetails, err, _ := GenerateRequestDetails(svcCtx, mapping, spec, status, cachedResponse)
	if err != nil {
		return RequestDetails{}, err
	}

	return requestDetails, nil
}

// IsRequestValid checks if the request details are valid.
func IsRequestValid(requestDetails RequestDetails) bool {
	return (!strings.Contains(fmt.Sprint(requestDetails), "null")) && (requestDetails.Url != "")
}

// coalesceHeaders returns the non-nil headers, or the default headers if both are nil.
func coalesceHeaders(mapping interfaces.HTTPMapping, spec interfaces.HTTPRequestSpec) map[string][]string {
	if headers := mapping.GetHeaders(); headers != nil {
		return headers
	}
	return spec.GetHeaders()
}

// generateURL applies a JQ filter to generate a URL.
func generateURL(urlJQFilter string, jqObject map[string]interface{}) (string, error) {
	getURL, err := requestprocessing.ApplyJQOnStr(urlJQFilter, jqObject)
	if err != nil {
		return "", err
	}

	return getURL, nil
}

// generateBody applies a mapping body to generate the request body.
func generateBody(svcCtx *service.ServiceContext, mappingBody string, jqObject map[string]interface{}) (httpClient.Data, error) {
	if mappingBody == "" {
		return httpClient.Data{
			Encrypted: "",
			Decrypted: "",
		}, nil
	}

	jqQuery := utils.NormalizeWhitespace(mappingBody)
	body, err := requestprocessing.ApplyJQOnStr(jqQuery, jqObject)
	if err != nil {
		return httpClient.Data{}, err
	}

	sensitiveBody, err := datapatcher.PatchSecretsIntoString(svcCtx.Ctx, svcCtx.LocalKube, body, svcCtx.Logger)
	if err != nil {
		return httpClient.Data{}, err
	}

	return httpClient.Data{
		Encrypted: body,
		Decrypted: sensitiveBody,
	}, nil
}

// generateHeaders applies JQ queries to generate headers.
func generateHeaders(svcCtx *service.ServiceContext, headers map[string][]string, jqObject map[string]interface{}) (httpClient.Data, error) {
	generatedHeaders, err := requestprocessing.ApplyJQOnMapStrings(headers, jqObject)
	if err != nil {
		return httpClient.Data{}, err
	}

	sensitiveHeaders, err := datapatcher.PatchSecretsIntoHeaders(svcCtx.Ctx, svcCtx.LocalKube, generatedHeaders, svcCtx.Logger)
	if err != nil {
		return httpClient.Data{}, err
	}

	return httpClient.Data{
		Encrypted: generatedHeaders,
		Decrypted: sensitiveHeaders,
	}, nil
}
