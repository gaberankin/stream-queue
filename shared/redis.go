package shared

import (
	"context"
	"fmt"
	"strings"
	"time"

	goredis "github.com/go-redis/redis"
	"github.com/google/uuid"
)

// RedisPubSub handler that can push/publish events into redis stream
// and pull (subscribe to) events on redis stream.
type RedisPubSub struct {
	rc           *goredis.Client
	workerGroup  string
	topic        string
	consumerName string
}

// Publish push bytes into RedisPubSub's queue
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

// Subscribe drain queue by 1 and run provided handler on returned bytes.
// if handler returns an error, the error is returned by Subscribe and the message is left in a pending state.
func (ps RedisPubSub) Subscribe(handler func([]byte) error) error {
	res, err := ps.rc.XReadGroup(&goredis.XReadGroupArgs{
		Group:    ps.workerGroup,
		Consumer: ps.consumerName,
		Streams:  []string{ps.topic, ">"},
		// todo: should probably make this configurable at the implementation level, but to do this we'll need
		// to consider how errors should be handled in processing loop below.
		Count: 1,
	}).Result()
	if err != nil {
		return err
	}

	for _, streamData := range res {
		// for the time being, only 1 item at max is processed at a time.  if the `Count` parameter above ever changes,
		// we'll need to work out a reasonable way to figure out how to properly handle errors.
		for _, d := range streamData.Messages {
			p, ok := d.Values["data"]
			if !ok {
				// this would happen if the "data" field is not on the stream message.
				// Per `Publish`, this should not happen in a perfect world.  unsure how to approach if it does happen tho.
				return fmt.Errorf("Invalid `data` parameter found on event %s", d.ID)
			}

			// handler
			if err := handler([]byte(p.(string))); err != nil {
				return err
			}

			// mark as done
			ps.rc.XAck(ps.topic, ps.workerGroup, d.ID)
		}
	}
	return nil
}

// String descriptive string about the RedisPubSub
func (ps RedisPubSub) String() string {
	return fmt.Sprintf("stream-name: '%s', stream-group: '%s', consumer-name: '%s'", ps.topic, ps.workerGroup, ps.consumerName)
}

// NewRedisPubSub creates a new queue handler that pushes into and pulls from a redis stream
func NewRedisPubSub(topic, workerGroup string, rc *goredis.Client) (PublisherSubscriber, error) {
	q := &RedisPubSub{
		workerGroup:  workerGroup,
		topic:        topic,
		consumerName: uuid.New().String(),
		rc:           rc,
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
