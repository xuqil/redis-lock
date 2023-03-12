package redis_lock

import "time"

// RetryStrategy is the retry strategy of Lock
type RetryStrategy interface {
	// Next determines the time interval for Lock
	// and whether Lock to retry
	Next() (time.Duration, bool)
}

type FixedIntervalRetryStrategy struct {
	Interval time.Duration
	MaxCnt   int
	cnt      int
}

func (f *FixedIntervalRetryStrategy) Next() (time.Duration, bool) {
	if f.cnt >= f.MaxCnt {
		return 0, false
	}
	f.cnt++
	return f.Interval, true
}
