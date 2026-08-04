package main

import (
	"log"
	"net/http"
)

func main() {
	cfg := LoadConfig()

	store := NewOrderStore()
	idem := NewIdempotencyStore()
	producer := NewOrderProducer(cfg.KafkaBroker, cfg.OrdersTopic)
	defer producer.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", acceptOrderHandler(store, idem, producer))
	mux.HandleFunc("DELETE /v1/orders/{orderId}", cancelOrderHandler(store, producer))

	addr := ":" + cfg.Port
	log.Printf("주문 접수 API 서버 시작: %s (Kafka broker=%s, topic=%s)", addr, cfg.KafkaBroker, cfg.OrdersTopic)
	log.Fatal(http.ListenAndServe(addr, mux))
}
