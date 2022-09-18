package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reliable-execution/src/config"
	"reliable-execution/src/queue/publish"
	"reliable-execution/src/service"
	"reliable-execution/src/store"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	cfg := config.Load()
	b, _ := json.MarshalIndent(cfg, "", "\t")
	fmt.Println(string(b))
	db, err := gorm.Open(mysql.Open(cfg.MySqlUrl))
	if err != nil {
		panic(err)
	}

	dbx, err := db.DB()
	if err != nil {
		panic(err)
	}

	err = dbx.Ping()
	if err != nil {
		panic("ping db error" + err.Error())
	}

	db = db.Debug()

	rdOtps, err := redis.ParseURL(cfg.RedisUrl)
	if err != nil {
		panic(err)
	}

	redisClient := redis.NewClient(rdOtps)
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		panic("ping redis error" + err.Error())
	}
	fmt.Println("ping redis success")

	publisher := publish.NewPublisher(redisClient, cfg.Topic)
	userStore := store.NewUseStore(db)
	svc := service.NewService(cfg, userStore, publisher, redisClient)

	engine := gin.Default()
	engine.POST("/api/v1/users", svc.CreateUser)
	engine.Run()
}
