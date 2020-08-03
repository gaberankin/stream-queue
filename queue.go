package main

import (
	"encoding/json"
	"strings"

	goredis "github.com/go-redis/redis"
)

// Queue queue client
type Queue struct {
	StreamName  string
	StreamGroup string
	rc          *goredis.Client
}

type envelope struct {
	Data json.RawMessage
}

// NewQueue creates a new queue handler
func NewQueue(streamName, streamGroup string, rc *goredis.Client) (*Queue, error) {
	q := &Queue{
		StreamName:  streamName,
		StreamGroup: streamGroup,
		rc:          rc,
	}

	// we need to go ahead and create the stream and group if they don't already exist
	err := q.rc.XGroupCreateMkStream(q.StreamName, q.StreamGroup, "$").Err()
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

// Add adds a json message to the queue, suitable for processing via `Drain`
func (q Queue) Add(data json.RawMessage) error {
	e := envelope{data}
	p, err := json.Marshal(e)
	if err != nil {
		return err
	}

	if err := q.rc.XAdd(&goredis.XAddArgs{
		Stream: q.StreamName,
		Values: map[string]interface{}{
			"data": p,
		},
	}).Err(); err != nil {
		return err
	}
	return nil
}

// Drain drains and processes messages on the queue, limited to passed to `count` parameter.  if `count` is zero, drains as many messages as currently available.
// `job` function will recieve bytes appropriate for json.Unmarshal.
// I'm kind of considering making `job` a member of the `Queue` struct, as it shouldn't change per-item in queue.
func (q Queue) Drain(count int64, job func(json.RawMessage) error) error {
	res, err := q.rc.XReadGroup(&goredis.XReadGroupArgs{
		Group: q.StreamGroup,
		// this shouldn't be hardcoded.  if we had multiple consumers, we'd use something like the current machine's hostname or something.
		Consumer: "consumer-1",
		Streams:  []string{q.StreamName, ">"},
		Count:    count,
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
			var e envelope
			if err := json.Unmarshal([]byte(p.(string)), &e); err != nil {
				//again, how to approach errors here?
				continue
			}
			if err := job(e.Data); err != nil {
				// ??
				continue
			}
			// mark as done
			q.rc.XAck(q.StreamName, q.StreamGroup, d.ID)
		}
	}

	return nil
}
