package main

import (
	"context"
	"log"
	"time"

	"matching/engine"
	"matching/kafkaclient"
	"matching/snapshotstore"
)

// 스냅샷은 마켓당 이 이벤트 수 또는 이 시간 중 먼저 도달하는 쪽마다 한 번씩 (비동기로)
// Redis에 남깁니다 — FR-08(재시작 복구) 체크포인트와 FR-12(호가창 조회) 응답을 겸합니다.
const (
	snapshotEveryEvents = 50
	snapshotInterval    = 100 * time.Millisecond
)

func main() {
	cfg := LoadConfig()
	ctx := context.Background()

	store := snapshotstore.NewRedisStore(cfg.RedisAddr)
	producer := kafkaclient.NewExecutionProducer(cfg.KafkaBroker, cfg.ExecutionsTopic)
	defer producer.Close()

	// 마켓마다 Engine을 하나씩 만들고, 저장된 스냅샷이 있으면 그 상태로 복구합니다(FR-08).
	// Phase 1은 단일 인스턴스라 모든 마켓을 이 프로세스 하나가 전부 담당합니다
	// (마켓 재분배는 Phase 2, FR-11).
	engines := make(map[string]*engine.Engine, len(TargetMarkets))
	resumeFrom := make(map[string]int64, len(TargetMarkets))

	for _, market := range TargetMarkets {
		e := engine.New(market, producer, store, snapshotEveryEvents, snapshotInterval)
		from, err := e.Recover(ctx)
		if err != nil {
			log.Fatalf("[%s] 복구 실패: %v", market, err)
		}
		if from > 0 {
			log.Printf("[%s] 스냅샷에서 복구 완료, offset %d부터 이어서 처리", market, from)
		}
		engines[market] = e
		resumeFrom[market] = from
	}

	reader := kafkaclient.NewOrderReader(cfg.KafkaBroker, cfg.OrdersTopic)
	defer reader.Close()

	log.Printf("매칭 엔진 시작 (Kafka broker=%s, orders=%s, executions=%s, redis=%s, 마켓 %d개)",
		cfg.KafkaBroker, cfg.OrdersTopic, cfg.ExecutionsTopic, cfg.RedisAddr, len(TargetMarkets))

	// orders 토픽은 항상 처음부터 읽지만(kafkaclient.NewOrderReader), 마켓별로 이미
	// 스냅샷에 반영된 오프셋보다 낮은 이벤트는 건너뜁니다 — 그래야 재시작 복구 시간이
	// "스냅샷 이후분"으로 제한됩니다(FR-08 설계, matching/engine/engine.go 참고).
	err := reader.Run(ctx, func(ctx context.Context, ev engine.OrderEvent) error {
		e, ok := engines[ev.Market]
		if !ok {
			return nil // 대상 20개 마켓 외 메시지는 무시
		}
		if ev.Offset < resumeFrom[ev.Market] {
			return nil
		}
		return e.Apply(ctx, ev)
	})
	log.Fatalf("매칭 엔진 종료: %v", err)
}
