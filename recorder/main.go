package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"recorder/archive"
	"recorder/events"
	rkafka "recorder/kafka"
	"recorder/store"
)

// 아카이브 마이크로배치는 건수 또는 시간 중 먼저 도달하면 플러시합니다 —
// matching/engine.Engine의 스냅샷 이중 트리거와 같은 패턴. orders/executions는
// 각자 별도 Batcher를 써서 한쪽이 몰려도 다른 쪽 플러시 주기가 밀리지 않습니다.
const (
	archiveFlushEvery    = 500
	archiveFlushInterval = 30 * time.Second

	// 컨슈머 그룹 ID는 고정 상수입니다 — matching과 마찬가지로 배포 전체가
	// 공유해야 하는 값이라 환경변수가 아닙니다. orders/executions/assignments를
	// 별도 그룹으로 나눠서 서로의 파티션 배정에 관여하지 않게 합니다.
	ordersGroupID      = "recorder-orders"
	executionsGroupID  = "recorder-executions"
	assignmentsGroupID = "recorder-assignments"
)

func main() {
	cfg := LoadConfig()
	ctx := context.Background()

	db, err := sql.Open("mysql", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("MySQL 드라이버 초기화 실패: %v", err)
	}
	defer db.Close()
	// sql.Open은 실제로 연결하지 않고 지연 연결하므로, 시작 시점에 곧바로 확인해서
	// (연결 문자열이 잘못됐거나 DB가 안 떠 있으면) 여기서 바로 실패가 드러나게 합니다.
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("MySQL 연결 확인 실패: %v", err)
	}
	dbStore := store.NewMySQLStore(db)

	var archiveStore archive.Store
	if cfg.ArchiveBucket != "" {
		archiveStore = archive.NewS3Store(cfg.ArchiveBucket)
	} else {
		archiveStore = archive.NewLocalStore("records")
	}
	orderBatcher := archive.NewBatcher(archiveStore, "orders", archiveFlushEvery)
	execBatcher := archive.NewBatcher(archiveStore, "executions", archiveFlushEvery)
	go runPeriodicFlush(ctx, orderBatcher, archiveFlushInterval)
	go runPeriodicFlush(ctx, execBatcher, archiveFlushInterval)

	orderReader := rkafka.NewOrderReader(cfg.KafkaBroker, cfg.OrdersTopic, ordersGroupID)
	defer orderReader.Close()
	execReader := rkafka.NewExecutionReader(cfg.KafkaBroker, cfg.ExecutionsTopic, executionsGroupID)
	defer execReader.Close()
	assignmentReader := rkafka.NewAssignmentReader(cfg.KafkaBroker, cfg.AssignmentsTopic, assignmentsGroupID)
	defer assignmentReader.Close()

	archiveDest := cfg.ArchiveBucket
	if archiveDest == "" {
		archiveDest = "(로컬 ./records)"
	}
	log.Printf("기록기 시작 (broker=%s, orders=%s, executions=%s, assignments=%s, archive=%s)",
		cfg.KafkaBroker, cfg.OrdersTopic, cfg.ExecutionsTopic, cfg.AssignmentsTopic, archiveDest)

	go func() {
		err := execReader.Run(ctx, func(ctx context.Context, ev events.ExecutionEvent) error {
			execBatcher.Add(ev)
			return store.ApplyExecutionEvent(ctx, dbStore, ev)
		})
		log.Fatalf("executions 리더 종료: %v", err)
	}()

	go func() {
		err := assignmentReader.Run(ctx, func(ctx context.Context, ev events.AssignmentEvent) error {
			return store.ApplyAssignmentEvent(ctx, dbStore, ev)
		})
		log.Fatalf("assignments 리더 종료: %v", err)
	}()

	err = orderReader.Run(ctx, func(ctx context.Context, ev events.OrderEvent) error {
		orderBatcher.Add(ev)
		return store.ApplyOrderEvent(ctx, dbStore, ev)
	})
	log.Fatalf("orders 리더 종료: %v", err)
}

func runPeriodicFlush(ctx context.Context, b *archive.Batcher, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.Flush()
		}
	}
}
