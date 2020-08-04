package shared

import (
	"context"
	"fmt"
	"strings"
	"time"

	goredis "github.com/go-redis/redis"
)

type RedisPubSub struct {
	rc          *goredis.Client
	workerGroup string
	topic       string
}

func (ps RedisPubSub) Publish(payload []byte) error {
	if err := ps.rc.XAdd(&goredis.XAddArgs{
		Stream: ps.topic,
		Values: map[string]interface{}{
			"data": payload,
		},
	}).Err(); err != nil {
		return err
	}
	return nil
}

func (ps RedisPubSub) Subscribe(handler func([]byte)) error {
	res, err := ps.rc.XReadGroup(&goredis.XReadGroupArgs{
		Group: ps.workerGroup,
		// this shouldn't be hardcoded.  if we had multiple consumers, we'd use something like the current machine's hostname or something.
		// probably need to understand more about how Redis uses this
		// and how deterministic/usable we need to make this.
		Consumer: "consumer-1",
		Streams:  []string{ps.topic, ">"},
		Count:    1, // todo: should probably make this configurable at the implementation level
	}).Result()
	if err != nil {
		return err
	}

	for _, streamData := range res {
		// note that there are a few points of failure in this loop, and i am currently not doing anything here, as i'm unsure whether i should treat
		// these errors as loop-breaking.
		// the problem with what we're dealing with is that we're working with something that will persist as 'pending' unless they're ack'ed.
		// in fact, EVERY OTHER MESSAGE pulled with this XReadGroup call (note we're allowing the `count` parameter) will _also_ be marked as pending
		// if there's an error with the information, should the 1 message be left as pending?  if we return an error immediately, the rest of the batch
		// is left hanging.
		// alternative is to remove the `count` parameter, and only process 1 item at a time.  if there's an issue with it, we can more easily make a
		// decision on what to do at that point.
		for _, d := range streamData.Messages {
			p, ok := d.Values["data"]
			if !ok {
				// this would happen if the "data" field is not on the stream message.
				// Per `Add`, this should not happen in a perfect world.  unsure how to approach if it does happen tho.
				continue
			}

			// handler
			handler(p)

			// mark as done
			ps.rc.XAck(ps.topic, ps.workerGroup, d.ID)
		}
	}
}

// NewRedisPubSub creates a new queue handler
func NewRedisPubSub(topic, workerGroup string, rc *goredis.Client) (PublisherSubscriber, error) {
	q := &RedisPubSub{
		workerGroup: workerGroup,
		topic:       topic,
		rc:          rc,
	}

	// we need to go ahead and create the stream and group if they don't already exist
	err := q.rc.XGroupCreateMkStream(q.topic, q.workerGroup, "$").Err()
	if err != nil {
		// in the case of group creation, we may run into an issue with the stream group already existing.
		// this should not be treated as an error, as we're trying to just get the group established.
		// TO BE HONEST, i dont like this, and am considering removing the error check altogether.
		// currently, the event-stream-library actively ignores errors that come from this call.
		if !strings.Contains(err.Error(), "Consumer Group name already exists") {
			return q, err
		}
	}

	return q, nil
}

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
