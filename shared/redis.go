package shared

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/go-redis/redis"
)

func RedisConnect(host, port string) (*goredis.Client, error) {
	opts := &goredis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Password: "",
		DB:       0,
	}

	client := goredis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := client.WithContext(ctx).Ping().Result(); err != nil {
		return nil, err
	}

	return client, nil
}
