package gomongo

import (
	"errors"
	"fmt"
	"time"
)

type RetryPolicy struct {
	MaxRetries      int
	BackoffStrategy func(int) time.Duration
}

func retryable[T any](l Logger, retries int, waittime func(int) time.Duration, fn func() (T, error), errsToSkip []error) (result T, err error) {
	attempt := 0
	for attempt < retries {
		result, err = fn()
		if err != nil {
			for _, e := range errsToSkip {
				if errors.Is(err, e) {
					return result, err
				}
			}
			l.Warn("retying error: %v", err)
			time.Sleep(waittime(attempt))
			attempt++
			continue
		}
		return result, nil
	}
	return result, fmt.Errorf("max retries exceeded: %w", err)
}
