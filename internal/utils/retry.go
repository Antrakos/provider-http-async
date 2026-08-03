package utils

import (
	"time"

	"github.com/Antrakos/provider-http-async/apis/common"
)

const (
	defaultWaitTimeout = 5 * time.Minute
)

// ShouldRetry determines if the request should be retried based on the status of the request and the rollback retries limit.
func ShouldRetry(rollbackRetriesLimit *int32, statusFailed int32) bool {
	return RollBackEnabled(rollbackRetriesLimit) && statusFailed != 0
}

// RollBackEnabled determines if the rollback retries limit is enabled.
func RollBackEnabled(rollbackRetriesLimit *int32) bool {
	return rollbackRetriesLimit != nil
}

// RetriesLimitReached determines if the rollback retries limit has been reached.
func RetriesLimitReached(statusFailed int32, rollbackRetriesLimit *int32) bool {
	return statusFailed >= *rollbackRetriesLimit
}

// WaitTimeout returns the wait timeout duration.
func WaitTimeout(timeout *string) time.Duration {
	return common.ParseDuration(timeout, defaultWaitTimeout)
}

// GetRollbackRetriesLimit returns the rollback retries limit.
func GetRollbackRetriesLimit(rollbackRetriesLimit *int32) int32 {
	limit := int32(1)
	if rollbackRetriesLimit != nil {
		limit = *rollbackRetriesLimit
	}
	return limit
}
