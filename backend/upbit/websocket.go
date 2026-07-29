package upbit

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gorilla/websocket"
)

// UpbitTicker 구조체는 업비트 서버에서 보내주는 JSON 형식의 시세 데이터를 Go 언어에서 다루기 위해 정의한 틀입니다.
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

// ConnectAndListen 함수는 웹소켓 연결을 맺고 지정된 코인들의 시세 데이터를 지속적으로 수신하여 출력합니다.
func ConnectAndListen(codes []string, done chan struct{}, tickerCh chan<- UpbitTicker) {
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
			"codes": codes,
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

	// 수신 루프를 고루틴으로 실행하여 메인 스레드가 블로킹되지 않도록 합니다.
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("메시지 수신 에러 또는 연결 종료: %v", err)
				return
			}

			var ticker UpbitTicker
			if err := json.Unmarshal(message, &ticker); err != nil {
				log.Printf("JSON 파싱 에러: %v", err)
				continue
			}

			fmt.Printf("[%s] 현재가: %.4f KRW | 전일종가: %.4f KRW | 고가: %.4f KRW | 저가: %.4f KRW\n",
				ticker.Code, ticker.TradePrice, ticker.PrevClosingPrice, ticker.HighPrice, ticker.LowPrice)

			// 파싱 완료된 구조체 데이터를 main.go로 전달하기 위해 채널에 송신합니다.
			tickerCh <- ticker
		}
	}()

	// done 채널을 통해 종료 신호가 들어올 때까지 대기합니다.
	<-done
	fmt.Println("웹소켓 연결을 안전하게 종료합니다.")
}
