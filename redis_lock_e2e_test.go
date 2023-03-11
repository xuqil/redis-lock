//go:build e2e

package redis_lock

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_e2e_TryLock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	testCases := []struct {
		name       string
		before     func(t *testing.T)
		after      func(t *testing.T)
		key        string
		expiration time.Duration

		wantErr  error
		wantLock *Lock
	}{
		{
			name: "key exist",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				res, err := rdb.Set(ctx, "key1", "value1", time.Minute).Result()
				require.NoError(t, err)
				assert.Equal(t, "OK", res)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				res, err := rdb.GetDel(ctx, "key1").Result()
				require.NoError(t, err)
				assert.Equal(t, "value1", res)
			},
			key:     "key1",
			wantErr: ErrFailedToPreemptLock,
		},
		{
			name:   "locked",
			before: func(t *testing.T) {},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// confirm and delete key
				res, err := rdb.GetDel(ctx, "key2").Result()
				require.NoError(t, err)
				assert.NotEmpty(t, res)
			},
			key: "key2",
			wantLock: &Lock{
				key: "key2",
			},
		},
	}

	client := NewClient(rdb)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			lock, err := client.TryLock(ctx, tc.key, time.Minute)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantLock.key, lock.key)
			assert.Equal(t, tc.wantLock.expiration, lock.expiration)
			assert.NotEmpty(t, lock.value)
			assert.NotNil(t, lock.client)
			tc.after(t)
		})
	}
}

func TestClient_e2e_Unlock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	testCases := []struct {
		name   string
		before func(t *testing.T)
		after  func(t *testing.T)

		lock *Lock

		wantErr error
	}{
		{
			name:   "lock not hold",
			before: func(t *testing.T) {},
			after:  func(t *testing.T) {},
			lock: &Lock{
				key:    "unlock_key1",
				value:  "123",
				client: rdb,
			},
			wantErr: ErrLockNotHold,
		},
		{
			name: "lock hold by others",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				res, err := rdb.Set(ctx, "unlock_key2", "value2", time.Minute).Result()
				require.NoError(t, err)
				assert.Equal(t, "OK", res)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// confirm and delete key
				res, err := rdb.GetDel(ctx, "unlock_key2").Result()
				require.NoError(t, err)
				assert.Equal(t, "value2", res)
			},
			lock: &Lock{
				key:    "unlock_key2",
				value:  "123",
				client: rdb,
			},
			wantErr: ErrLockNotHold,
		},
		{
			name: "unlocked",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				res, err := rdb.Set(ctx, "unlock_key3", "value3", time.Minute).Result()
				require.NoError(t, err)
				assert.Equal(t, "OK", res)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				// determine if unlocked
				res, err := rdb.Exists(ctx, "unlock_key3").Result()
				require.NoError(t, err)
				assert.Equal(t, int64(0), res)
			},
			lock: &Lock{
				key:    "unlock_key3",
				value:  "value3",
				client: rdb,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			err := tc.lock.Unlock(ctx)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			tc.after(t)
		})
	}
}
func TestClient_e2e_Refresh(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	testCases := []struct {
		name   string
		before func(t *testing.T)
		after  func(t *testing.T)

		lock *Lock

		wantErr error
	}{
		{
			name:   "lock not hold",
			before: func(t *testing.T) {},
			after:  func(t *testing.T) {},
			lock: &Lock{
				key:    "refresh_key1",
				value:  "123",
				client: rdb,
			},
			wantErr: ErrLockNotHold,
		},
		{
			name: "lock hold by others",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				res, err := rdb.Set(ctx, "refresh_key2", "value2", time.Second*10).Result()
				require.NoError(t, err)
				assert.Equal(t, "OK", res)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				timeout, err := rdb.TTL(context.Background(), "refresh_key2").Result()
				require.NoError(t, err)
				assert.Equal(t, true, timeout <= time.Second*10)
				// clear
				_, err = rdb.Del(ctx, "refresh_key2").Result()
				require.NoError(t, err)
			},
			lock: &Lock{
				key:        "refresh_key2",
				value:      "123",
				client:     rdb,
				expiration: time.Minute,
			},
			wantErr: ErrLockNotHold,
		},
		{
			name: "unlocked",
			before: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				res, err := rdb.Set(ctx, "refresh_key3", "value3", time.Second*10).Result()
				require.NoError(t, err)
				assert.Equal(t, "OK", res)
			},
			after: func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()
				timeout, err := rdb.TTL(context.Background(), "refresh_key2").Result()
				require.NoError(t, err)
				assert.Equal(t, true, timeout <= time.Second*50)
				// clear
				_, err = rdb.Del(ctx, "refresh_key3").Result()
				require.NoError(t, err)
			},
			lock: &Lock{
				key:        "refresh_key3",
				value:      "value3",
				client:     rdb,
				expiration: time.Minute,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			err := tc.lock.Refresh(ctx)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			tc.after(t)
		})
	}
}
