package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/websocket"
)

type UpbitTicker struct {
	Type             string  `json:"type"`
	Code             string  `json:"code"`
	TradePrice       float64 `json:"trade_price"`
	OpeningPrice     float64 `json:"opening_price"`
	HighPrice        float64 `json:"high_price"`
	LowPrice         float64 `json:"low_price"`
	PrevClosingPrice float64 `json:"prev_closing_price"`
	AccTradeVolume   float64 `json:"acc_trade_volume"`
	Timestamp        int64   `json:"timestamp"`
}

func main() {
	url := "wss://api.upbit.com/websocket/v1"

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatalf("웹소켓 연결 실패: %v", err)
	}
	defer conn.Close()

	fmt.Println("업비트 웹소켓 서버에 성공적으로 연결되었습니다.")

	requestPayload := []map[string]interface{}{
		{"ticket": "truss-test"},
		{
			"type":  "ticker",
			"codes": []string{"KRW-BTC"},
		},
	}

	payloadBytes, err := json.Marshal(requestPayload)
	if err != nil {
		log.Fatalf("요청 데이터 변환 실패: %v", err)
	}

	err = conn.WriteMessage(websocket.TextMessage, payloadBytes)
	if err != nil {
		log.Fatalf("구독 요청 전송 실패: %v", err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("메시지 수신 에러: %v", err)
				return
			}

			var ticker UpbitTicker
			if err := json.Unmarshal(message, &ticker); err != nil {
				log.Printf("JSON 파싱 에러: %v", err)
				continue
			}

			fmt.Printf("[%s] 현재가: %.0f KRW | 전일종가: %.0f KRW | 고가: %.0f KRW | 저가: %.0f KRW\n",
				ticker.Code, ticker.TradePrice, ticker.PrevClosingPrice, ticker.HighPrice, ticker.LowPrice)
		}
	}()

	<-interrupt
	fmt.Println("\n프로그램을 종료합니다.")
}