package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"matching/orderbook"
)

type fakePublisher struct {
	published []orderbook.Execution
	failNext  bool
}

func (p *fakePublisher) Publish(_ context.Context, exec orderbook.Execution) error {
	if p.failNext {
		p.failNext = false
		return errors.New("발행 실패(테스트용)")
	}
	p.published = append(p.published, exec)
	return nil
}

type fakeSnapshotStore struct {
	saved map[string]Snapshot
}

func newFakeSnapshotStore() *fakeSnapshotStore {
	return &fakeSnapshotStore{saved: make(map[string]Snapshot)}
}

func (s *fakeSnapshotStore) Load(_ context.Context, market string) (Snapshot, bool, error) {
	snap, ok := s.saved[market]
	return snap, ok, nil
}

func (s *fakeSnapshotStore) Save(_ context.Context, snap Snapshot) error {
	s.saved[snap.Market] = snap
	return nil
}

func dec(s string) decimal.Decimal {
	if s == "" {
		return decimal.Zero
	}
	v, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return v
}

func newOrderEvent(t OrderEventType, id string, side orderbook.Side, price, qty string, offset int64) OrderEvent {
	return OrderEvent{Type: t, OrderID: id, Market: "KRW-BTC", Side: side, Price: dec(price), Quantity: dec(qty), Offset: offset}
}

func TestEngineApplyPublishesExecutions(t *testing.T) {
	pub := &fakePublisher{}
	store := newFakeSnapshotStore()
	e := New("KRW-BTC", pub, store, 1000, time.Hour)

	if err := e.Apply(context.Background(), newOrderEvent(EventNew, "ask1", orderbook.Sell, "100", "1", 1)); err != nil {
		t.Fatalf("ask1 접수 실패: %v", err)
	}
	if err := e.Apply(context.Background(), newOrderEvent(EventNew, "bid1", orderbook.Buy, "100", "1", 2)); err != nil {
		t.Fatalf("bid1 접수 실패: %v", err)
	}

	if len(pub.published) != 1 {
		t.Fatalf("체결 발행 1건을 기대했으나 %d건", len(pub.published))
	}
	if pub.published[0].BuyOrderID != "bid1" || pub.published[0].SellOrderID != "ask1" {
		t.Errorf("체결 내용이 예상과 다름: %+v", pub.published[0])
	}
}

func TestEngineApplyCancel(t *testing.T) {
	pub := &fakePublisher{}
	store := newFakeSnapshotStore()
	e := New("KRW-BTC", pub, store, 1000, time.Hour)

	if err := e.Apply(context.Background(), newOrderEvent(EventNew, "ask1", orderbook.Sell, "100", "1", 1)); err != nil {
		t.Fatalf("ask1 접수 실패: %v", err)
	}
	if err := e.Apply(context.Background(), newOrderEvent(EventCancel, "ask1", orderbook.Sell, "", "", 2)); err != nil {
		t.Fatalf("ask1 취소 실패: %v", err)
	}
	if err := e.Apply(context.Background(), newOrderEvent(EventNew, "bid1", orderbook.Buy, "100", "1", 3)); err != nil {
		t.Fatalf("bid1 접수 실패: %v", err)
	}

	if len(pub.published) != 0 {
		t.Fatalf("취소된 주문과는 체결이 없어야 하는데 %d건 발행됨", len(pub.published))
	}
}

func TestEnginePublishFailureReturnsErrorAndDoesNotAdvanceOffset(t *testing.T) {
	pub := &fakePublisher{failNext: true}
	store := newFakeSnapshotStore()
	e := New("KRW-BTC", pub, store, 1, time.Hour) // snapshotEvery=1이라 다음 이벤트에서 바로 스냅샷 트리거됨

	if err := e.Apply(context.Background(), newOrderEvent(EventNew, "ask1", orderbook.Sell, "100", "1", 1)); err != nil {
		t.Fatalf("ask1 접수 실패: %v", err)
	}
	if err := e.Apply(context.Background(), newOrderEvent(EventNew, "bid1", orderbook.Buy, "100", "1", 2)); err == nil {
		t.Fatal("체결 발행이 실패했는데 Apply가 에러를 반환하지 않음")
	}

	// 발행 실패한 이벤트(offset=2)의 오프셋은 안전하게 처리된 것으로 기록되면 안 된다.
	if err := e.Apply(context.Background(), newOrderEvent(EventNew, "ask2", orderbook.Sell, "200", "1", 3)); err != nil {
		t.Fatalf("ask2 접수 실패: %v", err)
	}
	snap, ok, _ := store.Load(context.Background(), "KRW-BTC")
	if !ok {
		t.Fatal("스냅샷이 저장되지 않음")
	}
	if snap.Offset == 2 {
		t.Errorf("발행 실패한 이벤트의 오프셋(2)이 체크포인트에 기록됨: %+v", snap)
	}
}

func TestEngineSnapshotTriggersByCount(t *testing.T) {
	pub := &fakePublisher{}
	store := newFakeSnapshotStore()
	e := New("KRW-BTC", pub, store, 2, time.Hour)

	e.Apply(context.Background(), newOrderEvent(EventNew, "ask1", orderbook.Sell, "100", "1", 1))
	if _, ok, _ := store.Load(context.Background(), "KRW-BTC"); ok {
		t.Fatal("아직 스냅샷 주기(2건)에 도달 안 했는데 저장됨")
	}
	e.Apply(context.Background(), newOrderEvent(EventNew, "ask2", orderbook.Sell, "101", "1", 2))
	if _, ok, _ := store.Load(context.Background(), "KRW-BTC"); !ok {
		t.Fatal("2건째에 스냅샷이 저장돼야 함")
	}
}

func TestEngineRecoverRestoresBookAndResumeOffset(t *testing.T) {
	pub := &fakePublisher{}
	store := newFakeSnapshotStore()
	store.saved["KRW-BTC"] = Snapshot{
		Market: "KRW-BTC",
		Offset: 41,
		Bids:   []OrderView{{OrderID: "bid1", Price: dec("100"), Quantity: dec("1"), Offset: 10}},
		Asks:   []OrderView{{OrderID: "ask1", Price: dec("101"), Quantity: dec("2"), Offset: 11}},
	}

	e := New("KRW-BTC", pub, store, 1000, time.Hour)
	resumeFrom, err := e.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover 실패: %v", err)
	}
	if resumeFrom != 42 {
		t.Errorf("resumeFrom = %d, want 42 (스냅샷 offset+1)", resumeFrom)
	}

	// 복구된 매도(101, 수량 2)와 매수(102)가 실제로 매칭되는지 확인 — 복구가 제대로
	// 됐다면 이 신규 주문은 정상적으로 체결돼야 한다.
	if err := e.Apply(context.Background(), newOrderEvent(EventNew, "bid2", orderbook.Buy, "101", "2", 42)); err != nil {
		t.Fatalf("복구 후 신규 주문 접수 실패: %v", err)
	}
	if len(pub.published) != 1 || pub.published[0].SellOrderID != "ask1" {
		t.Fatalf("복구된 매도 주문과 체결돼야 하는데: %+v", pub.published)
	}
}

func TestEngineRecoverNoSnapshotStartsFromZero(t *testing.T) {
	pub := &fakePublisher{}
	store := newFakeSnapshotStore()
	e := New("KRW-BTC", pub, store, 1000, time.Hour)

	resumeFrom, err := e.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover 실패: %v", err)
	}
	if resumeFrom != 0 {
		t.Errorf("스냅샷이 없으면 0부터 시작해야 하는데 resumeFrom = %d", resumeFrom)
	}
}
