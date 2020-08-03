package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/go-redis/redis"
)

func main() {
	rc, err := redisConnect()
	if err != nil {
		panic(err)
	}
	defer rc.Close()

	q, err := NewQueue("test-stream", "my-test-group", rc)
	if err != nil {
		panic(err)
	}

	j := &testJobData{Data: "hello world"}
	payload, err := json.Marshal(j)
	if err != nil {
		panic(err)
	}

	if err := q.Add(payload); err != nil {
		panic(err)
	}

	if err := q.Drain(1, worker); err != nil {
		panic(err)
	}

	fmt.Println("---")

	for i := 0; i < 2; i++ {
		j := &testJobData{Data: fmt.Sprintf("test str %d", i)}
		payload, err := json.Marshal(j)
		if err != nil {
			panic(err)
		}

		if err := q.Add(payload); err != nil {
			panic(err)
		}
	}

	// drain with 0 count, which should pull all unread rather than limiting
	if err := q.Drain(0, worker); err != nil {
		panic(err)
	}

}

func redisConnect() (*goredis.Client, error) {
	opts := &goredis.Options{
		Addr:     fmt.Sprintf("%s:%d", "localhost", 6379),
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
