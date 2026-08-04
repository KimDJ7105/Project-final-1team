package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

// Config는 orderapi 실행에 필요한 환경변수를 담습니다.
type Config struct {
	Port        string
	KafkaBroker string
	OrdersTopic string
}

// LoadConfig는 로컬의 .env 파일(있으면)을 읽어들인 뒤, 환경변수 기반 설정을 반환합니다.
// backend/config.Load()와 같은 패턴 — prod에는 .env가 없고 환경변수가 직접 주입되므로
// .env 로드 실패 자체는 오류로 취급하지 않습니다.
func LoadConfig() Config {
	if err := godotenv.Load(); err != nil {
		log.Printf("'.env' 파일을 찾지 못했습니다 (prod 환경이라면 정상): %v", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	topic := os.Getenv("ORDERS_TOPIC")
	if topic == "" {
		topic = "orders"
	}

	return Config{Port: port, KafkaBroker: broker, OrdersTopic: topic}
}
