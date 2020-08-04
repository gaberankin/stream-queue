package main

import (
	"os"

	"github.com/gaberankin/stream-queue/shared"
)

func main() {
	host := os.Getenv("REDIS_HOST")
	port := os.Getenv("REDIS_PORT")

	rc, err := shared.RedisConnect(host, port)
	if err != nil {
		panic(err)
	}
	defer rc.Close()

	q, err := shared.NewQueue("test-stream", "my-test-group", rc)
	if err != nil {
		panic(err)
	}

	for {
		if err := q.Drain(0, shared.Worker); err != nil {
			panic(err)
		}
	}
}
