package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

// Config는 trader가 연결할 외부 서비스 주소를 담습니다. backend/config.Load(),
// orderapi/config.go의 LoadConfig()와 같은 패턴 — .env가 없어도(prod) 오류로 취급하지 않습니다.
type Config struct {
	BackendURL  string
	OrderAPIURL string
}

// LoadConfig는 로컬의 .env 파일(있으면)을 읽어들인 뒤, 환경변수 기반 설정을 반환합니다.
func LoadConfig() Config {
	if err := godotenv.Load(); err != nil {
		log.Printf("'.env' 파일을 찾지 못했습니다 (prod 환경이라면 정상): %v", err)
	}

	backendURL := os.Getenv("BACKEND_URL")
	if backendURL == "" {
		backendURL = "http://localhost:8080"
	}

	orderAPIURL := os.Getenv("ORDERAPI_URL")
	if orderAPIURL == "" {
		orderAPIURL = "http://localhost:8081"
	}

	return Config{BackendURL: backendURL, OrderAPIURL: orderAPIURL}
}
