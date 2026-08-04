package main

import (
	"context"
	"log"
	"strconv"
)

// Order는 Decision을 실제 주문 형태로 바꾼 것입니다.
// 필드 형태는 docs/api-specification.md의 POST /v1/orders 요청 바디에 맞췄습니다
// (price/quantity를 부동소수점 오차 방지를 위해 문자열로 직렬화하는 규칙 포함).
type Order struct {
	Market   string
	Side     string
	Price    string
	Quantity string
}

// NewOrder는 한 마켓의 Decision을 Order로 변환합니다.
func NewOrder(market string, d Decision) Order {
	return Order{
		Market:   market,
		Side:     d.Side,
		Price:    strconv.FormatFloat(d.Price, 'f', -1, 64),
		Quantity: strconv.FormatFloat(d.Quantity, 'f', -1, 64),
	}
}

// OrderSubmitter는 생성된 주문을 어딘가로 보냅니다. 주문 접수 API(POST /v1/orders)가
// 준비되면 이 인터페이스를 만족하는 HTTP 구현체로 교체하면 되고, 그 전까지는
// LogOnlySubmitter로 파이프라인 자체를 검증합니다.
type OrderSubmitter interface {
	Submit(ctx context.Context, o Order) error
}

// LogOnlySubmitter는 주문 접수 API 연동 전 임시로 쓰는 구현체 — 로그만 남깁니다.
type LogOnlySubmitter struct{}

func (LogOnlySubmitter) Submit(_ context.Context, o Order) error {
	log.Printf("[order] %s %s qty=%s price=%s (주문 API 미연동 — 로그만 남김)", o.Market, o.Side, o.Quantity, o.Price)
	return nil
}
