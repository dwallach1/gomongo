package gomongo

import (
	"fmt"
	"time"
)

type RetryPolicy struct {
	MaxRetries      int
	BackoffStrategy func(int) time.Duration
}

func retryable[T any](l Logger, retries int, waittime func(int) time.Duration, fn func() (T, error)) (result T, err error) {
	attempt := 0
	for attempt < retries {
		result, err = fn()
		if err != nil {
			l.Warn("retying error: %v", err)
			time.Sleep(waittime(attempt))
			attempt++
			continue
		}
		return result, nil
	}
	return result, fmt.Errorf("max retries exceeded: %w", err)
}
