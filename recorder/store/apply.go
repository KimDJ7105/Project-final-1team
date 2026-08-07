package store

import (
	"context"
	"fmt"
	"log"

	"recorder/events"
)

// ApplyOrderEvent는 orders 토픽에서 디코딩된 이벤트 하나를 Store에 반영합니다.
func ApplyOrderEvent(ctx context.Context, s Store, ev events.OrderEvent) error {
	switch ev.Type {
	case events.OrderNew:
		return s.InsertOrder(ctx, NewOrder{
			OrderID:         ev.OrderID,
			ClientRequestID: ev.ClientRequestID,
			Market:          ev.Market,
			Side:            ev.Side,
			Price:           ev.Price,
			Quantity:        ev.Quantity,
			Mode:            ev.Mode,
			SubmittedAt:     ev.AcceptedAt,
		})
	case events.OrderCancel:
		return s.CancelOrder(ctx, ev.OrderID, ev.CanceledAt)
	default:
		return fmt.Errorf("알 수 없는 이벤트 타입: %q", ev.Type)
	}
}

// ApplyExecutionEvent는 executions 토픽에서 디코딩된 이벤트 하나를 Store에
// 반영하고, Store가 보고한 결과(주문을 못 찾음/mode 불일치)를 로그로 남깁니다.
func ApplyExecutionEvent(ctx context.Context, s Store, ev events.ExecutionEvent) error {
	result, err := s.ApplyExecution(ctx, ExecutionInput{
		Market:      ev.Market,
		BuyOrderID:  ev.BuyOrderID,
		SellOrderID: ev.SellOrderID,
		Price:       ev.Price,
		Quantity:    ev.Quantity,
	})
	if err != nil {
		return fmt.Errorf("체결 반영 실패 (buyOrderId=%s, sellOrderId=%s): %w", ev.BuyOrderID, ev.SellOrderID, err)
	}

	if !result.BuyFound {
		log.Printf("체결 반영 중 매수 주문을 찾지 못함 (orderId=%s) — execution은 그대로 기록됨", ev.BuyOrderID)
	}
	if !result.SellFound {
		log.Printf("체결 반영 중 매도 주문을 찾지 못함 (orderId=%s) — execution은 그대로 기록됨", ev.SellOrderID)
	}
	if result.ModeMismatched {
		log.Printf("체결 양쪽 주문의 mode가 다름 (buyOrderId=%s, sellOrderId=%s) — 매수측 mode(%s) 사용",
			ev.BuyOrderID, ev.SellOrderID, result.Mode)
	}
	return nil
}

// ResolveMode는 매수/매도 양쪽 trade_order.mode로부터 execution.mode를
// 결정합니다. 둘 다 있고 다르면 매수측이 이기고 mismatched=true를 반환합니다 —
// orderapi/session의 "동시 실행 방지" 덕분에 이 시스템 전체에서 PAPER_TRADING과
// REPLAY가 동시에 살아있을 수는 없으므로, 이 불일치는 "이전 세션이 남긴 미체결
// 주문이 새 세션의 주문과 매칭된" 드문 경우로 예상됩니다 — 조용히 추측하지 않고
// 크게 로그를 남기는 이유입니다. 한쪽만 있으면 그 값을 그대로 쓰고, 둘 다
// 없으면 빈 문자열을 반환합니다(두 주문 다 NEW를 못 본 극단적인 경우).
func ResolveMode(buyMode string, buyFound bool, sellMode string, sellFound bool) (mode string, mismatched bool) {
	switch {
	case buyFound && sellFound:
		return buyMode, buyMode != sellMode
	case buyFound:
		return buyMode, false
	case sellFound:
		return sellMode, false
	default:
		return "", false
	}
}
