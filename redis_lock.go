package redis_lock

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
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

type Lock struct {
	client     redis.Cmdable
	key        string
	value      string
	expiration time.Duration
	unlockChan chan struct{}
}

func (l *Lock) Unlock(ctx context.Context) error {
	panic("implement me")
}
