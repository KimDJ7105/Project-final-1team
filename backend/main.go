package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"backend/upbit"
)

func main() {
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

	// 웹소켓 연결 및 수신 로직을 백그라운드(고루틴)에서 실행
	go upbit.ConnectAndListen(codes, done)

	// 사용자의 종료 신호(Ctrl+C 등)가 들어올 때까지 대기
	<-interrupt
	fmt.Println("\n종료 신호를 감지했습니다. 프로그램을 종료합니다.")

	// 웹소켓 로직에 종료 신호 전달
	close(done)
}
