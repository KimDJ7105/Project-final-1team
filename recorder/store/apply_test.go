package store

import (
	"context"
	"testing"

	"recorder/events"
)

// fakeStore는 실제 MySQL 없이 apply.go의 오케스트레이션 로직을 검증하기 위한
// Store 구현체입니다.
type fakeStore struct {
	inserted   []NewOrder
	canceled   []string
	execInputs []ExecutionInput
	execResult ExecutionResult
	execErr    error
	assigned   []AssignmentInput
	released   []AssignmentInput
}

func (f *fakeStore) InsertOrder(ctx context.Context, o NewOrder) error {
	f.inserted = append(f.inserted, o)
	return nil
}

func (f *fakeStore) CancelOrder(ctx context.Context, orderID, canceledAt string) error {
	f.canceled = append(f.canceled, orderID)
	return nil
}

func (f *fakeStore) ApplyExecution(ctx context.Context, in ExecutionInput) (ExecutionResult, error) {
	f.execInputs = append(f.execInputs, in)
	return f.execResult, f.execErr
}

func (f *fakeStore) AssignMarket(ctx context.Context, in AssignmentInput) error {
	f.assigned = append(f.assigned, in)
	return nil
}

func (f *fakeStore) ReleaseMarket(ctx context.Context, in AssignmentInput) error {
	f.released = append(f.released, in)
	return nil
}

func TestApplyOrderEventNewInsertsOrder(t *testing.T) {
	s := &fakeStore{}
	err := ApplyOrderEvent(context.Background(), s, events.OrderEvent{
		Type: events.OrderNew, OrderID: "ord_1", Market: "KRW-BTC", Side: "BUY",
		Price: "100", Quantity: "1", Mode: "PAPER_TRADING", AcceptedAt: "2026-08-06T00:00:00.000Z",
		ClientRequestID: "key-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s.inserted) != 1 {
		t.Fatalf("InsertOrder 호출 횟수 = %d, want 1", len(s.inserted))
	}
	got := s.inserted[0]
	if got.OrderID != "ord_1" || got.Mode != "PAPER_TRADING" || got.ClientRequestID != "key-1" {
		t.Errorf("got = %+v", got)
	}
}

func TestApplyOrderEventNewPassesThroughSourceOrderID(t *testing.T) {
	s := &fakeStore{}
	err := ApplyOrderEvent(context.Background(), s, events.OrderEvent{
		Type: events.OrderNew, OrderID: "ord_2", Market: "KRW-BTC", Side: "BUY",
		Price: "100", Quantity: "1", Mode: "REPLAY", AcceptedAt: "2026-08-07T00:00:00.000Z",
		SourceOrderID: "ord_1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := s.inserted[0].SourceOrderID; got != "ord_1" {
		t.Errorf("SourceOrderID = %q, want ord_1", got)
	}
}

func TestApplyOrderEventCancelUpdatesStatus(t *testing.T) {
	s := &fakeStore{}
	err := ApplyOrderEvent(context.Background(), s, events.OrderEvent{
		Type: events.OrderCancel, OrderID: "ord_1", CanceledAt: "2026-08-06T00:00:01.000Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s.canceled) != 1 || s.canceled[0] != "ord_1" {
		t.Errorf("got = %+v", s.canceled)
	}
}

func TestApplyExecutionEventCallsStoreWithDecodedFields(t *testing.T) {
	s := &fakeStore{execResult: ExecutionResult{BuyFound: true, SellFound: true, Mode: "PAPER_TRADING"}}
	err := ApplyExecutionEvent(context.Background(), s, events.ExecutionEvent{
		Market: "KRW-BTC", BuyOrderID: "b1", SellOrderID: "s1", Price: "100", Quantity: "1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s.execInputs) != 1 || s.execInputs[0].BuyOrderID != "b1" || s.execInputs[0].SellOrderID != "s1" {
		t.Errorf("got = %+v", s.execInputs)
	}
}

func TestApplyExecutionEventMissingOrderStillSucceeds(t *testing.T) {
	s := &fakeStore{execResult: ExecutionResult{BuyFound: false, SellFound: true, Mode: "REPLAY"}}
	err := ApplyExecutionEvent(context.Background(), s, events.ExecutionEvent{
		Market: "KRW-BTC", BuyOrderID: "b1", SellOrderID: "s1", Price: "100", Quantity: "1",
	})
	if err != nil {
		t.Fatalf("한쪽 주문을 못 찾아도 execution 자체는 에러 없이 처리돼야 하는데: %v", err)
	}
}

func TestApplyExecutionEventStoreErrorPropagates(t *testing.T) {
	s := &fakeStore{execErr: errTest}
	err := ApplyExecutionEvent(context.Background(), s, events.ExecutionEvent{BuyOrderID: "b1", SellOrderID: "s1"})
	if err == nil {
		t.Fatal("Store가 에러를 반환했는데 ApplyExecutionEvent가 에러를 안 반환함")
	}
}

func TestApplyAssignmentEventAssigned(t *testing.T) {
	s := &fakeStore{}
	err := ApplyAssignmentEvent(context.Background(), s, events.AssignmentEvent{
		Type: events.AssignmentAssigned, Market: "KRW-BTC", EngineInstanceID: "engine_1", At: "2026-08-07T00:00:00.000Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s.assigned) != 1 || s.assigned[0].Market != "KRW-BTC" || s.assigned[0].EngineInstanceID != "engine_1" {
		t.Errorf("got = %+v", s.assigned)
	}
}

func TestApplyAssignmentEventReleased(t *testing.T) {
	s := &fakeStore{}
	err := ApplyAssignmentEvent(context.Background(), s, events.AssignmentEvent{
		Type: events.AssignmentReleased, Market: "KRW-BTC", EngineInstanceID: "engine_1", At: "2026-08-07T00:01:00.000Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s.released) != 1 || s.released[0].Market != "KRW-BTC" {
		t.Errorf("got = %+v", s.released)
	}
}

func TestResolveModeBothFoundSame(t *testing.T) {
	mode, mismatched := ResolveMode("PAPER_TRADING", true, "PAPER_TRADING", true)
	if mode != "PAPER_TRADING" || mismatched {
		t.Errorf("got mode=%s mismatched=%v", mode, mismatched)
	}
}

func TestResolveModeBothFoundDifferentUsesBuySide(t *testing.T) {
	mode, mismatched := ResolveMode("PAPER_TRADING", true, "REPLAY", true)
	if mode != "PAPER_TRADING" || !mismatched {
		t.Errorf("got mode=%s mismatched=%v", mode, mismatched)
	}
}

func TestResolveModeOnlyBuyFound(t *testing.T) {
	mode, mismatched := ResolveMode("REPLAY", true, "", false)
	if mode != "REPLAY" || mismatched {
		t.Errorf("got mode=%s mismatched=%v", mode, mismatched)
	}
}

func TestResolveModeOnlySellFound(t *testing.T) {
	mode, mismatched := ResolveMode("", false, "REPLAY", true)
	if mode != "REPLAY" || mismatched {
		t.Errorf("got mode=%s mismatched=%v", mode, mismatched)
	}
}

func TestResolveModeNeitherFound(t *testing.T) {
	mode, mismatched := ResolveMode("", false, "", false)
	if mode != "" || mismatched {
		t.Errorf("got mode=%q mismatched=%v", mode, mismatched)
	}
}

var errTest = &testError{"테스트 에러"}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }
