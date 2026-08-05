package main

import (
	"log"
	"net/http"

	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := LoadConfig()

	store := NewOrderStore()
	idem := NewIdempotencyStore()
	producer := NewOrderProducer(cfg.KafkaBroker, cfg.OrdersTopic)
	defer producer.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", acceptOrderHandler(store, idem, producer))
	mux.HandleFunc("DELETE /v1/orders/{orderId}", cancelOrderHandler(store, producer))
	mux.HandleFunc("GET /v1/markets/{market}/orderbook", orderbookHandler(redisClient))

	addr := ":" + cfg.Port
	log.Printf("주문 접수 API 서버 시작: %s (Kafka broker=%s, topic=%s, redis=%s)", addr, cfg.KafkaBroker, cfg.OrdersTopic, cfg.RedisAddr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
