package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

// Producer는 카프카 메시지 전송을 담당하는 구조체입니다.
type Producer struct {
	writer *segmentio.Writer
}

// NewProducer는 새로운 카프카 프로듀서를 생성하여 반환합니다.
func NewProducer(brokerAddress string, topic string) *Producer {
	w := &segmentio.Writer{
		Addr:         segmentio.TCP(brokerAddress),
		Topic:        topic,
		Balancer:     &segmentio.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond, // 데이터 실시간 처리를 위해 지연 시간을 최소화합니다.
	}

	return &Producer{
		writer: w,
	}
}

// SendMessage는 수집한 구조체 데이터를 JSON으로 변환하여 카프카로 전송합니다.
func (p *Producer) SendMessage(ctx context.Context, key string, data interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	msg := segmentio.Message{
		Key:   []byte(key),
		Value: bytes,
	}

	err = p.writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("카프카 메시지 전송 실패: %v\n", err)
		return err
	}

	return nil
}

// Close는 애플리케이션 종료 시 프로듀서의 연결을 안전하게 해제합니다.
func (p *Producer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
