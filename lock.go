// Copyright 2023 xuqil
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis_lock

import (
	"context"
	_ "embed"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

var (
	ErrFailedToPreemptLock = errors.New("redis-lock: failed to lock")
	ErrLockNotHold         = errors.New("redis-lock: lock not hold")
	ErrLockTimeout         = errors.New("redis-lock: lock timeout")

	//go:embed lua/unlock.lua
	luaUnlock string

	//go:embed lua/refresh.lua
	luaRefresh string

	//go:embed lua/lock.lua
	luaLock string
)

// Client implements Redis distributed lock
type Client struct {
	client  redis.Cmdable
	varFunc func() string
	g       singleflight.Group
}

// NewClient creates a *Client
func NewClient(client redis.Cmdable) *Client {
	return &Client{
		client: client,
		varFunc: func() string {
			return uuid.New().String()
		},
	}
}

// SingleflightLock is an enhancement of Lock for high concurrency
// scenarios with singleflight restrictions for concurrency of a key
func (c *Client) SingleflightLock(ctx context.Context,
	key string,
	expiration time.Duration,
	timeout time.Duration, retry RetryStrategy) (*Lock, error) {
	for {
		var flag bool
		resCh := c.g.DoChan(key, func() (interface{}, error) {
			flag = true
			return c.Lock(ctx, key, expiration, timeout, retry)
		})
		select {
		case res := <-resCh:
			if flag {
				c.g.Forget(key)
				if res.Err != nil {
					return nil, res.Err
				}
				return res.Val.(*Lock), nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Lock tries to acquire a lock with timeout and retry strategy
func (c *Client) Lock(ctx context.Context,
	key string,
	expiration time.Duration,
	timeout time.Duration, retry RetryStrategy) (*Lock, error) {
	var timer *time.Timer
	val := c.varFunc()
	for {
		lCtx, cancel := context.WithTimeout(ctx, timeout)
		res, err := c.client.Eval(lCtx, luaLock, []string{key}, val, expiration.Seconds()).Result()
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		if res == "OK" {
			return newLock(c.client, key, val, expiration), nil
		}

		interval, ok := retry.Next()
		if !ok {
			return nil, ErrLockTimeout
		}
		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// TryLock tries to acquire a lock
func (c *Client) TryLock(ctx context.Context,
	key string,
	expiration time.Duration) (*Lock, error) {
	val := c.varFunc()
	ok, err := c.client.SetNX(ctx, key, val, expiration).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrFailedToPreemptLock
	}
	return newLock(c.client, key, val, expiration), nil
}

// Lock is responsible for unlocking and refreshing
type Lock struct {
	client     redis.Cmdable
	key        string
	value      string
	expiration time.Duration
	unlock     chan struct{}
	unlockOne  sync.Once
}

func newLock(client redis.Cmdable, key string, value string, expiration time.Duration) *Lock {
	return &Lock{
		client:     client,
		key:        key,
		value:      value,
		expiration: expiration,
		unlock:     make(chan struct{}, 1),
	}
}

// Unlock releases the lock
func (l *Lock) Unlock(ctx context.Context) error {
	res, err := l.client.Eval(ctx, luaUnlock, []string{l.key}, l.value).Int64()
	defer func() {
		l.unlockOne.Do(func() {
			l.unlock <- struct{}{}
			close(l.unlock)
		})
	}()
	if err == redis.Nil {
		return ErrLockNotHold
	}
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrLockNotHold
	}
	return nil
}

// Refresh refreshes the lock by expiration
func (l *Lock) Refresh(ctx context.Context) error {
	res, err := l.client.Eval(ctx, luaRefresh, []string{l.key}, l.value, l.expiration.Seconds()).Int64()
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrLockNotHold
	}
	return nil
}

// AutoRefreshChan refreshes automatically at interval,
// timeout specifies the refresh timeout,
// returns a chan with error
func (l *Lock) AutoRefreshChan(interval time.Duration, timeout time.Duration) <-chan error {
	errChan := make(chan error, 1)
	go func() {
		err := l.AutoRefresh(interval, timeout)
		if err != nil {
			errChan <- err
		}
	}()
	return errChan
}

// AutoRefresh refreshes automatically at interval,
// timeout specifies the refresh timeout
func (l *Lock) AutoRefresh(interval time.Duration, timeout time.Duration) error {
	timeoutChan := make(chan struct{}, 1)
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		close(timeoutChan)
	}()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := l.Refresh(ctx)
			cancel()
			// timeout retry
			if errors.Is(err, context.DeadlineExceeded) {
				select {
				case timeoutChan <- struct{}{}:
				default:
				}
				continue
			}
			if err != nil {
				return err
			}
		case <-timeoutChan:
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := l.Refresh(ctx)
			cancel()
			// timeout retry
			if errors.Is(err, context.DeadlineExceeded) {
				select {
				case timeoutChan <- struct{}{}:
				default:
				}
				continue
			}
			if err != nil {
				return err
			}
		case <-l.unlock:
			return nil
		}
	}
}
