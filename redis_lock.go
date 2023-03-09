package redis_lock

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	ErrFailedToPreemptLock = errors.New("redis-lock: failed to lock")
	ErrLockNotHold         = errors.New("redis-lock: lock not hold")

	//go:embed lua/unlock.lua
	luaUnlock string
)

// Client implements Redis distributed lock
type Client struct {
	client redis.Cmdable
}

// NewClient creates a *Client
func NewClient(client redis.Cmdable) *Client {
	return &Client{
		client: client,
	}
}

func (c *Client) Lock(ctx context.Context,
	key string,
	expiration time.Duration,
	timeout time.Duration) (*Lock, error) {
	panic("implement me")
}

// TryLock tries to acquire a lock
func (c *Client) TryLock(ctx context.Context,
	key string,
	expiration time.Duration) (*Lock, error) {
	val := uuid.New().String()
	ok, err := c.client.SetNX(ctx, key, val, expiration).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrFailedToPreemptLock
	}
	return &Lock{
		client:     c.client,
		key:        key,
		value:      val,
		expiration: expiration,
	}, nil
}

type Lock struct {
	client     redis.Cmdable
	key        string
	value      string
	expiration time.Duration
}

func (l *Lock) Unlock(ctx context.Context) error {
	res, err := l.client.Eval(ctx, luaUnlock, []string{l.key}, l.value).Int64()
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrLockNotHold
	}
	return nil
}
