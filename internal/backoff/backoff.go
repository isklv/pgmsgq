package backoff

import "time"

type Strategy func(retry int) time.Duration

func Fixed(delay time.Duration) Strategy {
	return func(_ int) time.Duration { return delay }
}

func Linear(base time.Duration) Strategy {
	return func(retry int) time.Duration { return base * time.Duration(retry) }
}

func Exponential(base time.Duration) Strategy {
	return func(retry int) time.Duration {
		if retry <= 0 {
			return 0
		}
		return base * (1 << (retry - 1)) // 1x, 2x, 4x...
	}
}