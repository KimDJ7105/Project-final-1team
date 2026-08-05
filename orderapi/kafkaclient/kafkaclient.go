// Package kafkaclient는 orderapi가 orders 토픽에 주문 이벤트를 발행하는 부분을 담당합니다.
package kafkaclient

import (
	"context"
	"encoding/json"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"orderapi/order"
)

// Publisher는 주문 이벤트를 어딘가에 발행합니다. 핸들러는 이 인터페이스만 알고,
// 실제 구현(OrderProducer)은 몰라도 됩니다 — 테스트에서는 실제 Kafka 없이
// 가짜 구현체를 주입할 수 있습니다.
type Publisher interface {
	PublishNew(ctx context.Context, o *order.Order) error
	PublishCancel(ctx context.Context, orderID, market string) error
}

// OrderProducer는 주문 이벤트를 Kafka orders 토픽에 발행하는 Publisher 구현체입니다.
//
// backend/kafka/producer.go와 의도적으로 다른 점: 여기서는 &kafka.Hash{} 밸런서를
// 씁니다. backend 쪽이 쓰던 &kafka.LeastBytes{}는 메시지 Key를 무시하고 파티션별
// 누적 바이트량만으로 파티션을 고르는 전략이라 "같은 마켓은 항상 같은 파티션"
// (FR-04, 순서 보장 NFR-07)을 보장하지 못합니다 — Hash는 Key의 해시로 파티션을
// 정해서 같은 Key(마켓명)가 항상 같은 파티션에 가는 걸 보장합니다.
type OrderProducer struct {
	writer *kafka.Writer
}

// NewOrderProducer는 topic에 발행하는 Publisher를 만듭니다.
func NewOrderProducer(broker, topic string) *OrderProducer {
	return &OrderProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(broker),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 10 * time.Millisecond,
			// 로컬 dev-kafka(KAFKA_AUTO_CREATE_TOPICS_ENABLE=true)에 맞춰 토픽이 없으면
			// 자동 생성을 요청합니다. kafka-go의 기본값은 false라 브로커가 자동 생성을
			// 허용해도 클라이언트가 따로 켜주지 않으면 "Unknown Topic Or Partition"으로
			// 실패합니다(실제로 이 버그를 겪고 알게 됨). prod에서 토픽을 미리 만들어
			// 관리하게 되면 이 옵션은 굳이 없어도 되지만, 켜져 있어도 무해합니다.
			AllowAutoTopicCreation: true,
		},
	}
}

// orderEvent는 Kafka orders 토픽에 싣는 메시지 모양입니다. FR-22가 말하는
// "별도의 Kafka 메시지 규격 문서"가 아직 없어 최소한의 모양으로 정한 것 —
// 그 문서가 생기면 그쪽 기준으로 맞춰야 합니다(CLAUDE.md에 기록).
type orderEvent struct {
	Type       string `json:"type"` // "NEW" | "CANCEL"
	OrderID    string `json:"orderId"`
	Market     string `json:"market"`
	Side       string `json:"side,omitempty"`
	Price      string `json:"price,omitempty"`
	Quantity   string `json:"quantity,omitempty"`
	AcceptedAt string `json:"acceptedAt,omitempty"`
}

// PublishNew는 신규 접수된 주문을 발행합니다. Key는 market — 같은 마켓 주문이
// 항상 같은 파티션에 순서대로 쌓이게 합니다.
func (p *OrderProducer) PublishNew(ctx context.Context, o *order.Order) error {
	return p.publish(ctx, o.Market, orderEvent{
		Type:       "NEW",
		OrderID:    o.OrderID,
		Market:     o.Market,
		Side:       o.Side,
		Price:      o.Price,
		Quantity:   o.Quantity,
		AcceptedAt: o.AcceptedAt,
	})
}

// PublishCancel은 취소 이벤트를 발행합니다. 신규 주문과 같은 마켓 파티션에 실어서,
// 매칭 엔진이 같은 마켓 안에서는 신규/취소를 순서대로 처리하게 합니다.
func (p *OrderProducer) PublishCancel(ctx context.Context, orderID, market string) error {
	return p.publish(ctx, market, orderEvent{Type: "CANCEL", OrderID: orderID, Market: market})
}

func (p *OrderProducer) publish(ctx context.Context, key string, event orderEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: body})
}

// Close는 프로듀서 연결을 정리합니다.
func (p *OrderProducer) Close() error {
	return p.writer.Close()
}
