package publish

import (
	"context"
	"encoding/json"
	"reliable-execution/src/models"

	"github.com/go-redis/redis/v9"
)

type IPublish interface {
	PubLish(ctx context.Context, message *models.Message) error
}

type _publish struct {
	r     *redis.Client
	topic string
}

func NewPublisher(r *redis.Client, topic string) IPublish {
	return &_publish{
		r:     r,
		topic: topic,
	}
}

func (p *_publish) PubLish(ctx context.Context, raw *models.Message) error {
	b, _ := json.Marshal(raw)
	return p.r.Publish(ctx, p.topic, b).Err()
}
