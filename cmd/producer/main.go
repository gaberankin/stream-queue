package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

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

	l := 0
	for {
		j := &shared.TestJobData{Data: fmt.Sprintf("%d / %s", l, time.Now().Format("2006-01-02 13:14:15"))}
		payload, err := json.Marshal(j)
		if err != nil {
			panic(err)
		}

		if err := q.Add(payload); err != nil {
			panic(err)
		}
		fmt.Println("pushed: ", l)
		l++
		time.Sleep(1 * time.Second)
	}
}
