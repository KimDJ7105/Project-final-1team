// Package kafkaclient는 매칭 엔진의 Kafka 입출력을 담당합니다 — orders 토픽 소비,
// executions 토픽 발행. 메시지 wire 포맷은 orderapi/kafkaclient.go와 같은 이유로
// 독립적으로 다시 선언합니다(모듈 간 타입 비공유 원칙).
package kafkaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/shopspring/decimal"
	kafka "github.com/segmentio/kafka-go"

	"matching/engine"
	"matching/orderbook"
)

// orderMessage는 orderapi/kafkaclient.go의 orderEvent와 같은 모양입니다. 이 규격은
// 아직 잠정입니다(FR-22 Kafka 메시지 규격 문서가 없음, orderapi와 동일한 상황).
type orderMessage struct {
	Type     string `json:"type"` // "NEW" | "CANCEL"
	OrderID  string `json:"orderId"`
	Market   string `json:"market"`
	Side     string `json:"side,omitempty"`
	Price    string `json:"price,omitempty"`
	Quantity string `json:"quantity,omitempty"`
}

// OrderReader는 orders 토픽을 항상 처음부터 읽습니다. Phase 1은 단일 인스턴스라 컨슈머
// 그룹/파티션 재분배가 없고, "이 마켓은 어디까지 이미 처리됐는지"는 (여러 파티션에
// 걸쳐 있을 수 있는 전체 스트림을 다시 훑는 대신) 각 엔진의 스냅샷 오프셋과 비교해
// 호출부(main.go)가 건너뛰는 방식으로 처리합니다. 마켓별로 정확히 파티션을 나눠 그
// 파티션만 seek하는 방식은 Phase 2(FR-11, ConsumerGroup 저수준 API)에서 다룹니다.
type OrderReader struct {
	reader *kafka.Reader
}

func NewOrderReader(broker, topic string) *OrderReader {
	return &OrderReader{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{broker},
			Topic:       topic,
			StartOffset: kafka.FirstOffset,
		}),
	}
}

// Run은 메시지를 계속 읽어 handle에 넘깁니다. 파싱 실패나 handle의 에러는 로그만 남기고
// 다음 메시지로 넘어갑니다 — 한 이벤트의 문제로 전체 컨슈밍이 멈추면 다른 마켓들까지
// 영향받기 때문입니다(handle 쪽에서 실패한 마켓은 다음 스냅샷 시점까지만 뒤처지고,
// 재기동하면 그 시점부터 다시 시도됩니다).
func (r *OrderReader) Run(ctx context.Context, handle func(ctx context.Context, ev engine.OrderEvent) error) error {
	for {
		msg, err := r.reader.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("orders 토픽 읽기 실패: %w", err)
		}

		var raw orderMessage
		if err := json.Unmarshal(msg.Value, &raw); err != nil {
			log.Printf("orders 메시지 파싱 실패, 건너뜀: %v", err)
			continue
		}

		ev, err := toOrderEvent(raw, msg.Offset)
		if err != nil {
			log.Printf("orders 메시지 변환 실패, 건너뜀 (market=%s, orderId=%s): %v", raw.Market, raw.OrderID, err)
			continue
		}

		if err := handle(ctx, ev); err != nil {
			log.Printf("이벤트 처리 실패 (market=%s, orderId=%s): %v", ev.Market, ev.OrderID, err)
		}
	}
}

func (r *OrderReader) Close() error {
	return r.reader.Close()
}

func toOrderEvent(raw orderMessage, offset int64) (engine.OrderEvent, error) {
	ev := engine.OrderEvent{OrderID: raw.OrderID, Market: raw.Market, Offset: offset}

	switch raw.Type {
	case "NEW":
		ev.Type = engine.EventNew
	case "CANCEL":
		ev.Type = engine.EventCancel
		return ev, nil // CANCEL 메시지에는 price/quantity/side가 없음
	default:
		return ev, fmt.Errorf("알 수 없는 이벤트 타입: %q", raw.Type)
	}

	switch raw.Side {
	case "BUY":
		ev.Side = orderbook.Buy
	case "SELL":
		ev.Side = orderbook.Sell
	default:
		return ev, fmt.Errorf("알 수 없는 side: %q", raw.Side)
	}

	price, err := decimal.NewFromString(raw.Price)
	if err != nil {
		return ev, fmt.Errorf("price 파싱 실패: %w", err)
	}
	qty, err := decimal.NewFromString(raw.Quantity)
	if err != nil {
		return ev, fmt.Errorf("quantity 파싱 실패: %w", err)
	}
	ev.Price = price
	ev.Quantity = qty
	return ev, nil
}
