package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakePublisher는 실제 Kafka 없이 핸들러를 검증하기 위한 Publisher 구현체입니다.
type fakePublisher struct {
	mu          sync.Mutex
	newCalls    int
	cancelCalls int
	failNext    bool
}

func (f *fakePublisher) PublishNew(ctx context.Context, o *Order) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext {
		f.failNext = false
		return context.DeadlineExceeded
	}
	f.newCalls++
	return nil
}

func (f *fakePublisher) PublishCancel(ctx context.Context, orderID, market string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls++
	return nil
}

func newOrderMux(store *OrderStore, idem *IdempotencyStore, pub Publisher) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders", acceptOrderHandler(store, idem, pub))
	mux.HandleFunc("DELETE /v1/orders/{orderId}", cancelOrderHandler(store, pub))
	return mux
}

func postOrder(mux *http.ServeMux, idempotencyKey, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/orders", strings.NewReader(body))
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestAcceptOrderMissingIdempotencyKey(t *testing.T) {
	mux := newOrderMux(NewOrderStore(), NewIdempotencyStore(), &fakePublisher{})
	rec := postOrder(mux, "", `{"market":"KRW-BTC","side":"BUY","price":"71500000","quantity":"0.015"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var got errorResponse
	json.NewDecoder(rec.Body).Decode(&got)
	if got.ErrorCode != "MISSING_IDEMPOTENCY_KEY" {
		t.Errorf("errorCode = %q, want MISSING_IDEMPOTENCY_KEY", got.ErrorCode)
	}
}

func TestAcceptOrderInvalidMarket(t *testing.T) {
	mux := newOrderMux(NewOrderStore(), NewIdempotencyStore(), &fakePublisher{})
	rec := postOrder(mux, "key-1", `{"market":"KRW-NOTREAL","side":"BUY","price":"100","quantity":"1"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var got errorResponse
	json.NewDecoder(rec.Body).Decode(&got)
	if got.ErrorCode != "INVALID_MARKET" {
		t.Errorf("errorCode = %q, want INVALID_MARKET", got.ErrorCode)
	}
}

func TestAcceptOrderSuccess(t *testing.T) {
	pub := &fakePublisher{}
	mux := newOrderMux(NewOrderStore(), NewIdempotencyStore(), pub)
	rec := postOrder(mux, "key-1", `{"market":"KRW-BTC","side":"BUY","price":"71500000","quantity":"0.015"}`)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202, body=%s", rec.Code, rec.Body.String())
	}
	var got Order
	json.NewDecoder(rec.Body).Decode(&got)
	if got.Status != StatusAccepted || got.Market != "KRW-BTC" || got.OrderID == "" {
		t.Errorf("got = %+v", got)
	}
	if pub.newCalls != 1 {
		t.Errorf("PublishNew 호출 횟수 = %d, want 1", pub.newCalls)
	}
}

func TestAcceptOrderIdempotentRetryReturnsSameResponseWithoutRepublishing(t *testing.T) {
	pub := &fakePublisher{}
	mux := newOrderMux(NewOrderStore(), NewIdempotencyStore(), pub)
	body := `{"market":"KRW-BTC","side":"BUY","price":"71500000","quantity":"0.015"}`

	first := postOrder(mux, "same-key", body)
	second := postOrder(mux, "same-key", body)

	if first.Body.String() != second.Body.String() {
		t.Errorf("같은 Idempotency-Key 재요청 응답이 달라짐:\n1차: %s\n2차: %s", first.Body.String(), second.Body.String())
	}
	if pub.newCalls != 1 {
		t.Errorf("재요청 시 재발행되면 안 되는데 PublishNew 호출 횟수 = %d", pub.newCalls)
	}
}

func TestAcceptOrderPublishFailureNotCachedAndAllowsRetry(t *testing.T) {
	pub := &fakePublisher{failNext: true}
	idem := NewIdempotencyStore()
	mux := newOrderMux(NewOrderStore(), idem, pub)
	body := `{"market":"KRW-BTC","side":"BUY","price":"71500000","quantity":"0.015"}`

	first := postOrder(mux, "retry-key", body)
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("첫 시도는 발행 실패로 500이어야 하는데 %d", first.Code)
	}

	second := postOrder(mux, "retry-key", body)
	if second.Code != http.StatusAccepted {
		t.Fatalf("발행 실패는 캐싱되지 않아 재시도가 성공해야 하는데 %d, body=%s", second.Code, second.Body.String())
	}
}

func TestCancelOrderNotFound(t *testing.T) {
	mux := newOrderMux(NewOrderStore(), NewIdempotencyStore(), &fakePublisher{})
	req := httptest.NewRequest(http.MethodDelete, "/v1/orders/ord_없음", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestCancelOrderSuccessThenIdempotentNoOp(t *testing.T) {
	store := NewOrderStore()
	store.Save(&Order{OrderID: "ord_1", Market: "KRW-BTC", Quantity: "0.015", Status: StatusAccepted})
	pub := &fakePublisher{}
	mux := newOrderMux(store, NewIdempotencyStore(), pub)

	req := httptest.NewRequest(http.MethodDelete, "/v1/orders/ord_1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
	var got cancelResponse
	json.NewDecoder(rec.Body).Decode(&got)
	if got.Status != StatusCanceled || got.CanceledQuantity != "0.015" {
		t.Errorf("got = %+v", got)
	}
	firstCanceledAt := got.CanceledAt

	// 같은 주문을 다시 취소 요청 — 재처리 없이 같은 결과가 나와야 함.
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, httptest.NewRequest(http.MethodDelete, "/v1/orders/ord_1", nil))
	var got2 cancelResponse
	json.NewDecoder(rec2.Body).Decode(&got2)
	if got2.CanceledAt != firstCanceledAt {
		t.Errorf("이미 취소된 주문을 다시 취소하면 같은 canceledAt을 유지해야 하는데 %q != %q", got2.CanceledAt, firstCanceledAt)
	}
	if pub.cancelCalls != 1 {
		t.Errorf("두 번째 취소 요청은 재발행하면 안 되는데 PublishCancel 호출 횟수 = %d", pub.cancelCalls)
	}
}

func TestCancelOrderAlreadyFilled(t *testing.T) {
	store := NewOrderStore()
	store.Save(&Order{OrderID: "ord_1", Market: "KRW-BTC", Status: StatusFilled})
	mux := newOrderMux(store, NewIdempotencyStore(), &fakePublisher{})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/orders/ord_1", nil))

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	var got errorResponse
	json.NewDecoder(rec.Body).Decode(&got)
	if got.ErrorCode != "ORDER_ALREADY_FILLED" {
		t.Errorf("errorCode = %q, want ORDER_ALREADY_FILLED", got.ErrorCode)
	}
}
