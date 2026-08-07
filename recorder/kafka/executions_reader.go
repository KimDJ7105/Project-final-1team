package kafka

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"

	"recorder/events"
)

// ExecutionReader는 executions 토픽을 소비합니다. OrderReader와 별도의
// 컨슈머 그룹 ID를 써서 서로의 파티션 배정에 관여하지 않습니다.
type ExecutionReader struct {
	reader *kafka.Reader
}

func NewExecutionReader(broker, topic, groupID string) *ExecutionReader {
	return &ExecutionReader{reader: kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: groupID,
	})}
}

// Run — orders_reader.go의 Run과 같은 이유로 FetchMessage+명시적 CommitMessages를
// 씁니다(kafka.Reader.ReadMessage는 처리 전에 즉시 커밋해버려서 DB 쓰기 실패 시
// 메시지가 재시도 없이 영원히 사라지는 문제가 있었음, 로컬 검증 중 발견).
func (r *ExecutionReader) Run(ctx context.Context, handle func(ctx context.Context, ev events.ExecutionEvent) error) error {
	for {
		msg, err := r.reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("executions 토픽 읽기 실패: %w", err)
		}

		ev, err := events.DecodeExecution(msg.Value)
		if err != nil {
			log.Printf("executions 메시지 디코딩 실패, 건너뜀: %v", err)
			if err := r.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("executions 오프셋 커밋 실패: %v", err)
			}
			continue
		}

		if err := handle(ctx, ev); err != nil {
			return fmt.Errorf("executions 이벤트 처리 실패 (buyOrderId=%s, sellOrderId=%s): %w", ev.BuyOrderID, ev.SellOrderID, err)
		}

		if err := r.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("executions 오프셋 커밋 실패 (buyOrderId=%s, sellOrderId=%s): %w", ev.BuyOrderID, ev.SellOrderID, err)
		}
	}
}

func (r *ExecutionReader) Close() error {
	return r.reader.Close()
}
