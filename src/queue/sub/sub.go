package sub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reliable-execution/src/models"
	"reliable-execution/src/queue/publish"
	"sync"

	"github.com/go-redis/redis/v9"
)

type Handler func(ctx context.Context, m *models.Message) error

type ISubcriber interface {
	RegisterTask(action string, h Handler) error
	Start(ctx context.Context) error
}

type _subcriber struct {
	topic      string
	r          *redis.Client
	handlerMap map[string]Handler
	mu         sync.Mutex
	pub        publish.IPublish
}

func New(r *redis.Client, topic string, pub publish.IPublish) ISubcriber {
	return &_subcriber{
		handlerMap: make(map[string]Handler),
		topic:      topic,
		r:          r,
		pub:        pub,
	}
}

func (s *_subcriber) RegisterTask(action string, h Handler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h == nil {
		return fmt.Errorf("handler is nil")
	}

	if len(action) == 0 {
		return fmt.Errorf("action is nil")
	}

	if _, ok := s.handlerMap[action]; ok {
		return fmt.Errorf("action is exixted")
	}

	s.handlerMap[action] = h
	return nil
}

func (s *_subcriber) Start(ctx context.Context) error {
	sub := s.r.Subscribe(ctx, s.topic)
	defer sub.Close()

	controlCh := sub.Channel()
	fmt.Println("start listening on control PubSub")

	for msg := range controlCh {
		m := &models.Message{}
		err := json.Unmarshal([]byte(msg.Payload), m)
		if err != nil {
			log.Println(err, "json unmarshal error")
			continue
		}

		if h, ok := s.handlerMap[m.Action]; ok {
			err = h(ctx, m)
			if err != nil {
				// re pusblist unack message
				log.Println(err, "[2] handler error, resend to queue")
				s.pub.PubLish(ctx, m)
			}
		}
	}

	return nil
}
