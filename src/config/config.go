package config

import "os"

type Config struct {
	RedisUrl string `json:"redis_url,omitempty"`
	MySqlUrl string `json:"my_sql_url,omitempty"`
	Topic    string `json:"topic,omitempty"`
}

func Load() *Config {
	cfg := &Config{
		RedisUrl: os.Getenv("REDIS_URL"),
		MySqlUrl: os.Getenv("MYSQL_URL"),
		Topic:    "create_user",
	}

	if len(cfg.MySqlUrl) == 0 {
		cfg.MySqlUrl = "root:12345678@tcp(localhost:3306)/reliable_execution?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=True&loc=Local"
	}

	if len(cfg.RedisUrl) == 0 {
		cfg.RedisUrl = "redis://localhost:6379?read_timeout=30&pool_fifo=true&dial_timeout=10&write_timeout=30&pool_size=10&pool_timeout=30"
	}

	return cfg
}
