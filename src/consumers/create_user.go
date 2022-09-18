package consumers

import (
	"context"
	"encoding/json"
	"log"
	"reliable-execution/src/external"
	"reliable-execution/src/models"
	"reliable-execution/src/store"
)

type CreateUser struct {
	userExternal external.IExternalCreateUser
	maxRetryTime int
	s            *store.UserStore
}

func NewCreateUserConsumer(user external.IExternalCreateUser, s *store.UserStore) *CreateUser {
	return &CreateUser{
		userExternal: user,
		maxRetryTime: 2,
		s:            s,
	}
}

type CreateUserMessage struct {
	ReqId string `json:"req_id,omitempty"`
}

func (c *CreateUser) HandlerCreateUser(ctx context.Context, m *models.Message) error {
	b, _ := json.MarshalIndent(m, "", "\t")
	log.Println("COMSUMER receiver message ", string(b))
	var msg CreateUserMessage
	err := json.Unmarshal(m.Data, &msg)
	if err != nil || len(msg.ReqId) == 0 {
		log.Println("Un expected messsage")
		return nil
	}

	user, err := c.s.GetByReqId(ctx, msg.ReqId)
	if err != nil || user == nil {
		log.Println(err, "reqId not found, do nothing")
		return nil
	}

	// TODO get lock user reqId
	if user.Status != models.USER_INIT {
		log.Println("pipeline is finished, do noting")
		return nil
	}

	err = c.userExternal.CreateUser(ctx)
	if err != nil {
		log.Println(err, "send requst to third party error")
		nowRetry := m.Attributes["retry_cnt"]
		user.RetryTime = int(nowRetry.(float64)) + 1
		errSaveRecord := c.s.Save(ctx, user)
		if errSaveRecord != nil {
			log.Println(err, "Save record to db error")
		}

		if int(nowRetry.(float64)) > c.maxRetryTime {
			// can't rety, move to dead letter queue
			log.Println("send to dead letter", string(b))
			m.Action = "DEAD"
			return err
		}

		// retry able
		m.Attributes["retry_cnt"] = nowRetry.(float64) + 1
		log.Println("resend to queue witn new retry count", string(b))
		return err
	}

	log.Println("create new user success from third party")
	user.Status = models.USER_SUCCESS
	err = c.s.Save(ctx, user)
	if err != nil {
		log.Println(err, "save record to db error")
	}

	return nil
}

func (c *CreateUser) HandlerDeadLetter(ctx context.Context, m *models.Message) error {
	var msg CreateUserMessage
	err := json.Unmarshal(m.Data, &msg)
	if err != nil || len(msg.ReqId) == 0 {
		log.Println("Un expected messsage")
		return nil
	}

	user, err := c.s.GetByReqId(ctx, msg.ReqId)
	if err != nil || user == nil {
		log.Println(err, "reqId not found, do nothing")
		return nil
	}

	user.Status = models.USER_FAIL
	err = c.s.Save(ctx, user)
	if err != nil {
		log.Println(err, "save record to db error")
	}

	return nil
}
