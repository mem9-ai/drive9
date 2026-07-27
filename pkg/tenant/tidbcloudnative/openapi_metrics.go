package tidbcloudnative

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/mem9-ai/drive9/pkg/metrics"
)

const (
	tidbCloudAPICluster = "cluster"
	tidbCloudAPIIAM     = "iam"

	tidbCloudOperationCreateCluster         = "create_cluster"
	tidbCloudOperationBatchCreateClusters   = "batch_create_clusters"
	tidbCloudOperationGetCluster            = "get_cluster"
	tidbCloudOperationListClusters          = "list_clusters"
	tidbCloudOperationUpdateCluster         = "update_cluster"
	tidbCloudOperationDeleteCluster         = "delete_cluster"
	tidbCloudOperationCreateBranch          = "create_branch"
	tidbCloudOperationGetBranch             = "get_branch"
	tidbCloudOperationDeleteBranch          = "delete_branch"
	tidbCloudOperationResolveAPIKeyIdentity = "resolve_api_key_identity"

	tidbCloudResultOK             = "ok"
	tidbCloudResultClientError    = "client_error"
	tidbCloudResultRateLimited    = "rate_limited"
	tidbCloudResultUpstreamError  = "upstream_error"
	tidbCloudResultProtocolError  = "protocol_error"
	tidbCloudResultDigestError    = "digest_error"
	tidbCloudResultTimeout        = "timeout"
	tidbCloudResultTransportError = "transport_error"
	tidbCloudResultCanceled       = "canceled"
)

func recordTiDBCloudOpenAPIRequest(api, operation, result string) {
	if api == "" || operation == "" {
		return
	}
	metrics.RecordTiDBCloudOpenAPIRequest(api, operation, result)
}

func recordTiDBCloudHTTPResponse(api, operation string, statusCode int, responseValid bool) {
	recordTiDBCloudOpenAPIRequest(api, operation, tiDBCloudHTTPResult(statusCode, responseValid))
}

func tiDBCloudHTTPResult(statusCode int, responseValid bool) string {
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		if responseValid {
			return tidbCloudResultOK
		}
		return tidbCloudResultProtocolError
	}
	if statusCode == http.StatusTooManyRequests {
		return tidbCloudResultRateLimited
	}
	if statusCode >= http.StatusBadRequest && statusCode < http.StatusInternalServerError {
		return tidbCloudResultClientError
	}
	if statusCode >= http.StatusInternalServerError && statusCode <= 599 {
		return tidbCloudResultUpstreamError
	}
	return tidbCloudResultProtocolError
}

func tiDBCloudRequestErrorResult(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return tidbCloudResultCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return tidbCloudResultTimeout
	default:
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return tidbCloudResultTimeout
		}
		return tidbCloudResultTransportError
	}
}

func tidbCloudAPIForRequest(uri string) string {
	if strings.Contains(uri, "/v1beta1/apikeys/") {
		return tidbCloudAPIIAM
	}
	return tidbCloudAPICluster
}

func tidbCloudOperationForRequest(method, uri string) string {
	path := uri
	if parsed, err := url.Parse(uri); err == nil {
		path = parsed.Path
	}
	switch {
	case strings.HasSuffix(path, "/clusters:batchCreate"):
		return tidbCloudOperationBatchCreateClusters
	case strings.Contains(path, "/apikeys/"):
		return tidbCloudOperationResolveAPIKeyIdentity
	case strings.HasSuffix(path, "/v1beta1/clusters"):
		if method == http.MethodGet {
			return tidbCloudOperationListClusters
		}
		return tidbCloudOperationCreateCluster
	case strings.Contains(path, "/branches/"):
		switch method {
		case http.MethodGet:
			return tidbCloudOperationGetBranch
		case http.MethodDelete:
			return tidbCloudOperationDeleteBranch
		default:
			return tidbCloudOperationCreateBranch
		}
	case strings.HasSuffix(path, "/branches"):
		return tidbCloudOperationCreateBranch
	case strings.Contains(path, "/clusters/"):
		switch method {
		case http.MethodGet:
			return tidbCloudOperationGetCluster
		case http.MethodDelete:
			return tidbCloudOperationDeleteCluster
		case http.MethodPatch:
			return tidbCloudOperationUpdateCluster
		default:
			return tidbCloudOperationGetCluster
		}
	default:
		// All production callers use one of the fixed IAM/cluster paths above;
		// this bounded fallback is for malformed or newly introduced paths and
		// must never be replaced with the raw URI as a metric label.
		return "unknown"
	}
}
