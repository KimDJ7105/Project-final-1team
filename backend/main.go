package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"backend/kafka"
	"backend/upbit"

	// 환경 변수용
	"github.com/joho/godotenv"
)

func main() {
	// .env 파일 로드. (에러가 나도 로컬 기본값 등으로 방어 가능)
	_ = godotenv.Load()

	// 환경 변수를 통한 카프카 브로커 주소 설정
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	// 카프카 프로듀서 초기화
	kafkaProducer := kafka.NewProducer(broker, "upbit-ticker")
	defer kafkaProducer.Close()

	// TRUSS 시스템의 목표치인 20개 마켓 설정
	codes := []string{
		"KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL", "KRW-DOGE",
		"KRW-ADA", "KRW-AVAX", "KRW-DOT", "KRW-BCH", "KRW-LINK",
		"KRW-SHIB", "KRW-ETC", "KRW-SUI", "KRW-SEI", "KRW-APT",
		"KRW-STX", "KRW-NEAR", "KRW-AAVE", "KRW-SAND", "KRW-MANA",
	}

	// 프로그램 종료 신호를 처리할 채널 설정
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// 웹소켓 패키지에 종료를 알리기 위한 채널 생성
	done := make(chan struct{})

	// 패키지 간 데이터 전달을 위한 버퍼 채널 생성
	tickerCh := make(chan upbit.UpbitTicker, 100)

	// 웹소켓 연결 및 수신 로직을 백그라운드(고루틴)에서 실행
	go upbit.ConnectAndListen(codes, done, tickerCh)

	// 채널로 수신된 데이터를 카프카로 전송하는 독립된 고루틴
	go func() {
		for ticker := range tickerCh {
			err := kafkaProducer.SendMessage(context.Background(), ticker.Code, ticker)
			if err != nil {
				fmt.Printf("카프카 전송 실패 (%s): %v\n", ticker.Code, err)
			}
		}
	}()

	// 사용자의 종료 신호(Ctrl+C 등)가 들어올 때까지 대기
	<-interrupt
	fmt.Println("\n종료 신호를 감지했습니다. 프로그램을 종료합니다.")

	// 웹소켓 로직에 종료 신호 전달
	close(done)
	close(tickerCh)
}
