// Package kafka는 orders/executions 토픽을 소비하는 얇은 리더입니다. 기록기는
// matching처럼 마켓별 인메모리 상태를 지키기 위한 리밸런스 핸드오프가 필요
//없으므로(메시지 하나마다 독립적으로 DB에 쓰기만 함), matching/kafkaclient의
// ConsumerGroup+커스텀 파티션 밸런서 같은 복잡한 장치 없이 평범한 컨슈머 그룹
// (kafka.Reader{GroupID: ...})으로 충분합니다 — Kafka 자체의 파티션 배정과
// 오프셋 커밋을 그대로 신뢰합니다.
package kafka

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"

	"recorder/events"
)

// OrderReader는 orders 토픽을 소비합니다.
type OrderReader struct {
	reader *kafka.Reader
}

func NewOrderReader(broker, topic, groupID string) *OrderReader {
	return &OrderReader{reader: kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: groupID,
	})}
}

// Run은 메시지를 계속 읽어 handle에 넘깁니다.
//
// kafka.Reader.ReadMessage는 메시지를 가져오는 즉시(호출부가 처리하기도 전에)
// 오프셋을 커밋합니다 — DB 쓰기가 그 뒤에 실패하면 그 메시지는 재시도 기회 없이
// 영원히 사라집니다(FR-09 "Kafka 발행 건수와 DB 저장 건수가 일치" 검증 기준을
// 깨는 실제 버그였음, 로컬 검증 중 발견). 그래서 커밋 안 하는 FetchMessage로
// 읽고, handle이 성공한 뒤에만 명시적으로 CommitMessages합니다.
//
// 디코딩 실패(메시지 자체가 깨짐)는 재시도해도 똑같이 실패하므로 로그만 남기고
// 커밋 후 건너뜁니다. handle 실패(예: DB 일시 장애)는 다르게 취급합니다 —
// 커밋하지 않고 에러를 반환해 이 리더를 멈춥니다. main.go가 이 에러로
// log.Fatal해서 전체 프로세스를 재시작시키면, 다음 실행이 마지막으로 커밋된
// 오프셋부터 이 메시지를 다시 읽어 재시도합니다(DB 장애 같은 일시적 문제는
// 재시도로 해결될 여지가 있는 반면, 디코딩 실패는 재시도로 해결될 여지가 없다는
// 차이).
func (r *OrderReader) Run(ctx context.Context, handle func(ctx context.Context, ev events.OrderEvent) error) error {
	for {
		msg, err := r.reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("orders 토픽 읽기 실패: %w", err)
		}

		ev, err := events.DecodeOrderEvent(msg.Value)
		if err != nil {
			log.Printf("orders 메시지 디코딩 실패, 건너뜀: %v", err)
			if err := r.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("orders 오프셋 커밋 실패: %v", err)
			}
			continue
		}

		if err := handle(ctx, ev); err != nil {
			return fmt.Errorf("orders 이벤트 처리 실패 (orderId=%s): %w", ev.OrderID, err)
		}

		if err := r.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("orders 오프셋 커밋 실패 (orderId=%s): %w", ev.OrderID, err)
		}
	}
}

func (r *OrderReader) Close() error {
	return r.reader.Close()
}
