package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	// 고릴라 웹소켓 패키지입니다. Go 언어에서 웹소켓 통신을 할 때 표준처럼 가장 널리 쓰이는 외부 라이브러리입니다.
	"github.com/gorilla/websocket"
)

// UpbitTicker 구조체는 업비트 서버에서 보내주는 JSON 형식의 시세 데이터를 Go 언어에서 다루기 위해 정의한 틀입니다.
// 백틱 기호로 둘러싸인 json 태그는 수신된 JSON 데이터의 어떤 키값이 구조체의 어떤 필드에 연결되는지 알려주는 역할을 합니다.
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

	// 지정한 업비트 웹소켓 주소로 연결을 시도합니다.
	// 두 번째 반환값은 HTTP 응답 헤더인데 여기서는 사용하지 않으므로 밑줄 기호로 처리하여 무시했습니다.
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatalf("웹소켓 연결 실패: %v", err)
	}

	// defer는 이 함수(main)가 종료되기 직전에 이 코드를 실행하라는 예약 명령어입니다.
	// 프로그램이 끝날 때 열어둔 웹소켓 연결을 안전하게 닫기 위해 사용합니다.
	defer conn.Close()

	fmt.Println("업비트 웹소켓 서버에 성공적으로 연결되었습니다.")

	// 업비트 웹소켓은 연결 직후 우리가 어떤 데이터를 받고 싶은지 구독 요청을 해야 합니다.
	// ticket은 해당 요청을 식별하기 위한 고유값이며, type과 codes를 통해 비트코인 시세를 요청합니다.
	requestPayload := []map[string]interface{}{
		{"ticket": "truss-test"},
		{
			"type":  "ticker",
			"codes": []string{"KRW-BTC"},
		},
	}

	// Go 언어의 데이터 구조를 네트워크로 전송할 수 있도록 JSON 바이트 배열로 변환하는 과정입니다.
	payloadBytes, err := json.Marshal(requestPayload)
	if err != nil {
		log.Fatalf("요청 데이터 변환 실패: %v", err)
	}

	// 변환된 JSON 데이터를 웹소켓 텍스트 메시지 형태로 업비트 서버에 전송합니다.
	err = conn.WriteMessage(websocket.TextMessage, payloadBytes)
	if err != nil {
		log.Fatalf("구독 요청 전송 실패: %v", err)
	}

	// 프로그램이 바로 종료되는 것을 막고, 사용자가 강제 종료를 누를 때 신호를 받기 위한 채널을 생성합니다.
	interrupt := make(chan os.Signal, 1)

	// 운영체제에서 발생하는 인터럽트 신호나 종료 신호를 위에서 만든 interrupt 채널로 전달하도록 설정합니다.
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// go 키워드는 고루틴을 실행합니다. 메인 흐름과 별개로 동시에 백그라운드에서 동작하는 가벼운 스레드입니다.
	// 시세 데이터를 계속 수신하기 위해 무한 루프를 돌려야 하므로, 메인 흐름이 막히지 않게 고루틴으로 분리했습니다.
	go func() {
		// 무한 루프를 돌며 서버에서 보내는 시세 데이터를 실시간으로 대기합니다.
		for {
			// ReadMessage는 서버로부터 메시지가 올 때까지 대기하다가, 메시지가 도착하면 읽어옵니다.
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("메시지 수신 에러: %v", err)
				return
			}

			// 비어있는 UpbitTicker 구조체를 하나 준비합니다.
			var ticker UpbitTicker

			// 수신한 JSON 문자열을 분석해서 미리 준비한 ticker 구조체 변수에 데이터를 채워 넣습니다.
			// 주소값을 넘겨주어야 함수 내부에서 원본 변수의 값을 변경할 수 있기 때문에 & 기호를 사용합니다.
			if err := json.Unmarshal(message, &ticker); err != nil {
				log.Printf("JSON 파싱 에러: %v", err)
				continue
			}

			fmt.Printf("[%s] 현재가: %.0f KRW | 전일종가: %.0f KRW | 고가: %.0f KRW | 저가: %.0f KRW\n",
				ticker.Code, ticker.TradePrice, ticker.PrevClosingPrice, ticker.HighPrice, ticker.LowPrice)
		}
	}()

	// 메인 함수가 여기까지 오면 interrupt 채널에 신호가 들어올 때까지 여기서 대기하게 됩니다.
	// 즉, 고루틴이 백그라운드에서 계속 시세를 받는 동안 프로그램이 종료되지 않도록 붙잡아두는 역할을 합니다.
	<-interrupt
	fmt.Println("\n프로그램을 종료합니다.")
}
