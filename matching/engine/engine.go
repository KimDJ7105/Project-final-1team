// Package engine은 마켓 하나를 담당하는 조립체입니다 — orderbook.OrderBook을 소유하고,
// orders 토픽에서 온 이벤트를 순서대로 적용하며, 체결이 나오면 발행하고, 주기적으로
// 스냅샷을 남깁니다(FR-08 재시작 복구 + FR-12 호가창 조회를 하나의 스냅샷으로 처리).
// Kafka/Redis의 구체 구현은 모르고 인터페이스로만 의존합니다.
package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"matching/orderbook"
)

// OrderEventType은 orders 토픽 메시지의 종류입니다.
type OrderEventType string

const (
	EventNew    OrderEventType = "NEW"
	EventCancel OrderEventType = "CANCEL"
)

// OrderEvent는 orders 토픽의 한 메시지를 매칭 엔진이 다루기 좋은 형태로 옮긴 것입니다.
// kafkaclient 패키지가 실제 Kafka 메시지(JSON)를 이 타입으로 변환해 넘겨줍니다.
type OrderEvent struct {
	Type     OrderEventType
	OrderID  string
	Market   string
	Side     orderbook.Side
	Price    decimal.Decimal
	Quantity decimal.Decimal
	Offset   int64 // 이 이벤트를 소비한 orders 토픽 파티션 안의 오프셋
}

// ExecutionPublisher는 체결 결과를 내보냅니다(FR-09 중 Kafka 발행 부분 — DB 저장은
// "기록기"가 executions를 구독해서 하는 것이라 매칭 엔진의 책임이 아닙니다).
type ExecutionPublisher interface {
	Publish(ctx context.Context, exec orderbook.Execution) error
}

// OrderView는 스냅샷에 저장하는 개별 미체결 주문입니다(FR-08 복구용 — 잔량과 FIFO
// 순서를 그대로 보존해야 해서 집계하지 않습니다. 조회용 집계 뷰는 orderapi가 이
// 스냅샷을 읽어 depth만큼만 잘라 합산해서 만듭니다).
type OrderView struct {
	OrderID  string
	Price    decimal.Decimal
	Quantity decimal.Decimal
	Offset   int64
}

// Snapshot은 한 마켓의 특정 시점 상태 + 그 상태가 반영된 마지막 오프셋입니다.
type Snapshot struct {
	Market string
	Offset int64
	Bids   []OrderView
	Asks   []OrderView
}

// SnapshotStore는 스냅샷을 저장/로드합니다. engine은 이게 Redis인지, 어떻게
// 비동기 처리되는지 몰라도 됩니다 — Save가 핫패스를 막지 않는 건 구현체(snapshotstore
// 패키지)의 책임입니다.
type SnapshotStore interface {
	Load(ctx context.Context, market string) (Snapshot, bool, error)
	Save(ctx context.Context, snap Snapshot) error
}

// Engine은 마켓 하나를 담당합니다. 한 마켓은 고루틴 하나가 이 타입의 Apply를 순차
// 호출한다는 전제로 동시성 보호를 하지 않습니다(orderbook.OrderBook과 같은 이유).
type Engine struct {
	Market string
	book   *orderbook.OrderBook

	publisher ExecutionPublisher
	snapshots SnapshotStore

	snapshotEvery    int
	snapshotInterval time.Duration

	sinceSnapshot  int
	lastSnapshotAt time.Time
	lastOffset     int64
}

// New는 market을 담당하는 Engine을 만듭니다. snapshotEvery/snapshotInterval은
// "N건마다 또는 T 시간마다, 둘 중 먼저 도달하면 스냅샷"의 기준입니다.
func New(market string, publisher ExecutionPublisher, snapshots SnapshotStore, snapshotEvery int, snapshotInterval time.Duration) *Engine {
	return &Engine{
		Market:           market,
		book:             orderbook.New(market),
		publisher:        publisher,
		snapshots:        snapshots,
		snapshotEvery:    snapshotEvery,
		snapshotInterval: snapshotInterval,
		lastSnapshotAt:   time.Now(),
	}
}

// Recover는 저장된 스냅샷을 불러와 호가창을 그 상태로 세팅합니다(FR-08). 스냅샷이
// 없으면(최초 실행) 빈 호가창인 채로 오프셋 0부터 시작합니다. 반환값은 이어서 컨슘을
// 시작해야 할 offset입니다.
func (e *Engine) Recover(ctx context.Context) (resumeFromOffset int64, err error) {
	snap, ok, err := e.snapshots.Load(ctx, e.Market)
	if err != nil {
		return 0, fmt.Errorf("스냅샷 로드 실패: %w", err)
	}
	if !ok {
		return 0, nil
	}

	for _, ov := range snap.Bids {
		e.book.Restore(&orderbook.Order{OrderID: ov.OrderID, Market: e.Market, Side: orderbook.Buy, Price: ov.Price, Quantity: ov.Quantity, Offset: ov.Offset})
	}
	for _, ov := range snap.Asks {
		e.book.Restore(&orderbook.Order{OrderID: ov.OrderID, Market: e.Market, Side: orderbook.Sell, Price: ov.Price, Quantity: ov.Quantity, Offset: ov.Offset})
	}
	e.lastOffset = snap.Offset
	return snap.Offset + 1, nil
}

// Apply는 orders 토픽에서 온 이벤트 하나를 처리합니다. NEW면 매칭 후 체결을 전부
// 발행하고(FR-09), CANCEL이면 호가창에서 제거합니다(FR-10). 체결 발행이 전부 성공한
// 뒤에만 이 이벤트의 offset을 "안전하게 처리됨"으로 기록합니다 — 발행 확인 전에 먼저
// 기록해두면, 그 사이 크래시가 나면 다음 복구 시 이 체결이 재생되지 않고 영원히
// 사라집니다(스냅샷 체크포인트 설계, FR-08).
func (e *Engine) Apply(ctx context.Context, ev OrderEvent) error {
	switch ev.Type {
	case EventNew:
		o := &orderbook.Order{OrderID: ev.OrderID, Market: ev.Market, Side: ev.Side, Price: ev.Price, Quantity: ev.Quantity, Offset: ev.Offset}
		for _, exec := range e.book.Apply(o) {
			if err := e.publisher.Publish(ctx, exec); err != nil {
				return fmt.Errorf("체결 발행 실패 (market=%s): %w", e.Market, err)
			}
		}
	case EventCancel:
		e.book.Cancel(ev.OrderID)
	}

	e.lastOffset = ev.Offset
	e.sinceSnapshot++
	if e.shouldSnapshot() {
		if err := e.snapshot(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) shouldSnapshot() bool {
	return e.sinceSnapshot >= e.snapshotEvery || time.Since(e.lastSnapshotAt) >= e.snapshotInterval
}

func (e *Engine) snapshot(ctx context.Context) error {
	snap := Snapshot{
		Market: e.Market,
		Offset: e.lastOffset,
		Bids:   toOrderViews(e.book.AllOrders(orderbook.Buy)),
		Asks:   toOrderViews(e.book.AllOrders(orderbook.Sell)),
	}
	if err := e.snapshots.Save(ctx, snap); err != nil {
		return fmt.Errorf("스냅샷 저장 실패 (market=%s): %w", e.Market, err)
	}
	e.sinceSnapshot = 0
	e.lastSnapshotAt = time.Now()
	return nil
}

func toOrderViews(orders []*orderbook.Order) []OrderView {
	out := make([]OrderView, 0, len(orders))
	for _, o := range orders {
		out = append(out, OrderView{OrderID: o.OrderID, Price: o.Price, Quantity: o.Quantity, Offset: o.Offset})
	}
	return out
}
