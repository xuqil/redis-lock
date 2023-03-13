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
	"errors"
	"github.com/stretchr/testify/require"
	"github.com/xuqil/redis-lock/mocks"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestClient_Lock(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testCases := []struct {
		name string

		mock func() redis.Cmdable

		key        string
		expiration time.Duration
		retry      RetryStrategy
		timeout    time.Duration

		wantLock *Lock
		wantErr  string
	}{
		{
			name: "locked",
			mock: func() redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background(), nil)
				res.SetVal("OK")
				cmd.EXPECT().Eval(gomock.Any(), luaLock, []string{"locked-key"}, gomock.Any()).
					Return(res)
				return cmd
			},
			key:        "locked-key",
			expiration: time.Minute,
			retry:      &FixedIntervalRetry{Interval: time.Second, Max: 1},
			timeout:    time.Second,
			wantLock: &Lock{
				key:        "locked-key",
				expiration: time.Minute,
			},
		},
		{
			name: "not retryable",
			mock: func() redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background(), nil)
				res.SetErr(errors.New("network error"))
				cmd.EXPECT().Eval(gomock.Any(), luaLock, []string{"locked-key"}, gomock.Any()).
					Return(res)
				return cmd
			},
			key:        "locked-key",
			expiration: time.Minute,
			retry:      &FixedIntervalRetry{Interval: time.Second, Max: 1},
			timeout:    time.Second,
			wantErr:    "network error",
		},
		{
			name: "retry over times",
			mock: func() redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				first := redis.NewCmd(context.Background(), nil)
				first.SetErr(context.DeadlineExceeded)
				cmd.EXPECT().Eval(gomock.Any(), luaLock, []string{"retry-key"}, gomock.Any()).
					Times(3).Return(first)
				return cmd
			},
			key:        "retry-key",
			expiration: time.Minute,
			retry:      &FixedIntervalRetry{Interval: time.Millisecond, Max: 2},
			timeout:    time.Second,
			wantErr:    "redis-lock: lock timeout",
		},
		{
			name: "retry over times-lock holded",
			mock: func() redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				first := redis.NewCmd(context.Background(), nil)
				cmd.EXPECT().Eval(gomock.Any(), luaLock, []string{"retry-key"}, gomock.Any()).
					Times(3).Return(first)
				return cmd
			},
			key:        "retry-key",
			expiration: time.Minute,
			retry:      &FixedIntervalRetry{Interval: time.Millisecond, Max: 2},
			timeout:    time.Second,
			wantErr:    "redis-lock: lock timeout",
		},
		{
			name: "retry and success",
			mock: func() redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				first := redis.NewCmd(context.Background(), nil)
				first.SetVal("")
				cmd.EXPECT().Eval(gomock.Any(), luaLock, []string{"retry-key"}, gomock.Any()).
					Times(2).Return(first)
				second := redis.NewCmd(context.Background(), nil)
				second.SetVal("OK")
				cmd.EXPECT().Eval(gomock.Any(), luaLock, []string{"retry-key"}, gomock.Any()).
					Return(second)
				return cmd
			},
			key:        "retry-key",
			expiration: time.Minute,
			retry:      &FixedIntervalRetry{Interval: time.Millisecond, Max: 2},
			timeout:    time.Second,
			wantLock: &Lock{
				key:        "retry-key",
				expiration: time.Minute,
			},
		},
		{
			name: "retry but timeout",
			mock: func() redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				first := redis.NewCmd(context.Background(), nil)
				first.SetVal("")
				cmd.EXPECT().Eval(gomock.Any(), luaLock, []string{"retry-key"}, gomock.Any()).
					Times(2).Return(first)
				return cmd
			},
			key:        "retry-key",
			expiration: time.Minute,
			retry:      &FixedIntervalRetry{Interval: time.Millisecond * 550, Max: 2},
			timeout:    time.Second,
			wantErr:    "context deadline exceeded",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockRedisCmd := tc.mock()
			client := NewClient(mockRedisCmd)
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			l, err := client.Lock(ctx, tc.key, tc.expiration, time.Second, tc.retry)
			if tc.wantErr != "" {
				assert.EqualError(t, err, tc.wantErr)
				return
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, mockRedisCmd, l.client)
			assert.Equal(t, tc.key, l.key)
			assert.Equal(t, tc.expiration, l.expiration)
			assert.NotEmpty(t, l.value)
		})
	}
}

func TestClient_TryLock(t *testing.T) {
	testCases := []struct {
		name string
		mock func(ctrl *gomock.Controller) redis.Cmdable

		key string

		wantErr  error
		wantLock *Lock
	}{
		{
			name: "setnx error",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				res := redis.NewBoolResult(false, context.DeadlineExceeded)
				cmd.EXPECT().SetNX(context.Background(), "key1", gomock.Any(), time.Minute).
					Return(res)
				return cmd
			},
			key:     "key1",
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "failed to preempt lock",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				res := redis.NewBoolResult(false, nil)
				cmd.EXPECT().SetNX(context.Background(), "key1", gomock.Any(), time.Minute).
					Return(res)
				return cmd
			},
			key:     "key1",
			wantErr: ErrFailedToPreemptLock,
		},
		{
			name: "locked",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				res := redis.NewBoolResult(true, nil)
				cmd.EXPECT().SetNX(context.Background(), "key1", gomock.Any(), time.Minute).
					Return(res)
				return cmd
			},
			key: "key1",
			wantLock: &Lock{
				key:        "key1",
				expiration: time.Minute,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			client := NewClient(tc.mock(ctrl))
			l, err := client.TryLock(context.Background(), tc.key, time.Minute)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantLock.key, l.key)
			assert.Equal(t, tc.wantLock.expiration, l.expiration)
			assert.NotEmpty(t, l.value)
		})
	}
}

func TestLock_Unlock(t *testing.T) {
	testCases := []struct {
		name string

		mock  func(ctl *gomock.Controller) redis.Cmdable
		key   string
		value string

		wantErr error
	}{
		{
			name: "eval error",
			mock: func(ctl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctl)
				res := redis.NewCmd(context.Background())
				res.SetErr(context.DeadlineExceeded)
				cmd.EXPECT().Eval(context.Background(), luaUnlock, []string{"key1"}, []any{"value1"}).
					Return(res)
				return cmd
			},
			key:     "key1",
			value:   "value1",
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "lock not hold",
			mock: func(ctl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctl)
				res := redis.NewCmd(context.Background())
				res.SetVal(int64(0))
				cmd.EXPECT().Eval(context.Background(), luaUnlock, []string{"key1"}, []any{"value1"}).
					Return(res)
				return cmd
			},
			key:     "key1",
			value:   "value1",
			wantErr: ErrLockNotHold,
		},
		{
			name: "unlocked",
			mock: func(ctl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctl)
				res := redis.NewCmd(context.Background())
				res.SetVal(int64(1))
				cmd.EXPECT().Eval(context.Background(), luaUnlock, []string{"key1"}, []any{"value1"}).
					Return(res)
				return cmd
			},
			key:   "key1",
			value: "value1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			lock := newLock(tc.mock(ctrl), tc.key, tc.value, time.Minute)
			err := lock.Unlock(context.Background())
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestLock_Refresh(t *testing.T) {
	testCases := []struct {
		name string

		mock       func(ctrl *gomock.Controller) redis.Cmdable
		key        string
		value      string
		expiration time.Duration

		wantErr error
	}{
		{
			name: "eval error",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetErr(context.DeadlineExceeded)
				cmd.EXPECT().Eval(context.Background(), luaRefresh, []string{"key1"}, []any{"value1", float64(60)}).
					Return(res)
				return cmd
			},
			key:        "key1",
			value:      "value1",
			expiration: time.Minute,
			wantErr:    context.DeadlineExceeded,
		},
		{
			name: "lock not hold",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetVal(int64(0))
				cmd.EXPECT().Eval(context.Background(), luaRefresh, []string{"key2"}, []any{"value2", float64(60)}).
					Return(res)
				return cmd
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
			wantErr:    ErrLockNotHold,
		},
		{
			name: "refresh",
			mock: func(ctrl *gomock.Controller) redis.Cmdable {
				cmd := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetVal(int64(1))
				cmd.EXPECT().Eval(context.Background(), luaRefresh, []string{"key2"}, []any{"value2", float64(60)}).
					Return(res)
				return cmd
			},
			key:        "key2",
			value:      "value2",
			expiration: time.Minute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			lock := newLock(tc.mock(ctrl), tc.key, tc.value, time.Minute)
			err := lock.Refresh(context.Background())
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestClient_SingleflightLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rdb := mocks.NewMockCmdable(ctrl)
	cmd := redis.NewCmd(context.Background())
	cmd.SetVal("OK")
	rdb.EXPECT().Eval(gomock.Any(), luaLock, gomock.Any(), gomock.Any()).
		Return(cmd)
	client := NewClient(rdb)
	// TODO 并发测试
	_, err := client.SingleflightLock(context.Background(),
		"key1",
		time.Minute,
		time.Second,
		&FixedIntervalRetry{
			Interval: time.Millisecond,
			Max:      3,
		})
	require.NoError(t, err)
}

func TestLock_AutoRefresh(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testCases := []struct {
		name         string
		unlockTiming time.Duration
		lock         func() *Lock
		interval     time.Duration
		timeout      time.Duration
		wantErr      error
	}{
		{
			name:         "auto refresh success",
			interval:     time.Millisecond * 100,
			unlockTiming: time.Second,
			timeout:      time.Second * 2,
			lock: func() *Lock {
				rdb := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background(), nil)
				res.SetVal(int64(1))
				rdb.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"auto-refreshed"}, []any{"123", float64(60)}).
					AnyTimes().Return(res)
				cmd := redis.NewCmd(context.Background())
				cmd.SetVal(int64(1))
				rdb.EXPECT().Eval(gomock.Any(), luaUnlock, gomock.Any(), gomock.Any()).
					Return(cmd)
				return newLock(rdb, "auto-refreshed", "123", time.Minute)
			},
		},
		{
			name:         "auto refresh failed",
			interval:     time.Millisecond * 100,
			unlockTiming: time.Second,
			timeout:      time.Second * 2,
			lock: func() *Lock {
				rdb := mocks.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background(), nil)
				res.SetErr(errors.New("network error"))
				rdb.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"auto-refreshed"}, []any{"123", float64(60)}).
					AnyTimes().Return(res)
				cmd := redis.NewCmd(context.Background())
				cmd.SetVal(int64(1))
				rdb.EXPECT().Eval(gomock.Any(), luaUnlock, gomock.Any(), gomock.Any()).
					Return(cmd)
				return newLock(rdb, "auto-refreshed", "123", time.Minute)
			},
			wantErr: errors.New("network error"),
		},
		{
			name:         "auto refresh timeout",
			interval:     time.Millisecond * 100,
			unlockTiming: time.Second * 1,
			timeout:      time.Second * 2,
			lock: func() *Lock {
				rdb := mocks.NewMockCmdable(ctrl)
				first := redis.NewCmd(context.Background(), nil)
				first.SetErr(context.DeadlineExceeded)
				rdb.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"auto-refreshed"}, []any{"123", float64(60)}).Return(first)

				second := redis.NewCmd(context.Background(), nil)
				second.SetVal(int64(1))
				rdb.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"auto-refreshed"}, []any{"123", float64(60)}).AnyTimes().Return(first)

				cmd := redis.NewCmd(context.Background())
				cmd.SetVal(int64(1))
				rdb.EXPECT().Eval(gomock.Any(), luaUnlock, gomock.Any(), gomock.Any()).
					Return(cmd)
				return newLock(rdb, "auto-refreshed", "123", time.Minute)
			},
			wantErr: nil,
		},
	}

	for _, tt := range testCases {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			lock := tc.lock()
			go func() {
				time.Sleep(tc.unlockTiming)
				err := lock.Unlock(context.Background())
				require.NoError(t, err)
			}()
			err := lock.AutoRefresh(tc.interval, tc.timeout)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
