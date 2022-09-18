package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reliable-execution/src/config"
	"reliable-execution/src/consumers"
	"reliable-execution/src/external"
	"reliable-execution/src/models"
	"reliable-execution/src/queue/publish"
	"reliable-execution/src/queue/sub"
	"reliable-execution/src/store"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v9"
)

type Service struct {
	store *store.UserStore
	pub   publish.IPublish
}

func NewService(cfg *config.Config, store *store.UserStore, pub publish.IPublish, r *redis.Client) *Service {
	go func() {
		ext := external.NewExternalCreateUser()
		handlerConsume := consumers.NewCreateUserConsumer(ext, store)
		subcriber := sub.New(r, cfg.Topic, pub)
		subcriber.RegisterTask("CREATE", handlerConsume.HandlerCreateUser)
		subcriber.RegisterTask("DEAD", handlerConsume.HandlerDeadLetter)
		err := subcriber.Start(context.Background())
		if err != nil {
			panic("start sub error" + err.Error())
		}
	}()

	return &Service{
		store: store,
		pub:   pub,
	}
}

func (s *Service) createNewUser(ctx *gin.Context, req *models.CreateUserRequest) {
	if req.ReqId == "" {
		req.ReqId = fmt.Sprint(time.Now().UnixMilli())
	}

	r := models.User{
		Name:      req.Name,
		ReqId:     req.ReqId,
		RetryTime: 0,
		Status:    models.USER_INIT,
	}

	err := s.store.Save(ctx.Request.Context(), &r)
	if err != nil {
		log.Println(err, "save to record error")
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	raw := consumers.CreateUserMessage{
		ReqId: req.ReqId,
	}

	b, _ := json.Marshal(raw)
	err = s.pub.PubLish(ctx.Request.Context(), &models.Message{
		Action:    "CREATE",
		MessageId: req.ReqId,
		Attributes: map[string]interface{}{
			"retry_cnt": 0,
		},
		Data: b,
	})

	if err != nil {
		log.Println(err, "send to redis pubsub error")
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err})
		return
	}

	log.Println("create new user success")
	ctx.JSON(http.StatusOK, r)
}

func (s *Service) handleExistedReqId(ctx *gin.Context, u *models.User, req *models.CreateUserRequest) {
	ctx.JSON(http.StatusOK, u)
}

func (s *Service) CreateUser(ctx *gin.Context) {
	var req models.CreateUserRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	if req.ReqId == "" {
		s.createNewUser(ctx, &req)
		return
	}

	u, _ := s.store.GetByReqId(ctx.Request.Context(), req.ReqId)
	if u == nil {
		s.createNewUser(ctx, &req)
		return
	}

	s.handleExistedReqId(ctx, u, &req)
}
