// Package store는 docs/erd.md의 TRADE_ORDER/EXECUTION 테이블에 대한 쓰기를
// 다룹니다. PostgresStore가 실제 구현이고, 이 인터페이스는 apply.go의 오케스트레이션
// 로직을 실제 DB 없이 테스트하기 위해 존재합니다(orderapi/kafkaclient.Publisher와
// 같은 패턴).
package store

import "context"

// NewOrder는 orders 토픽의 NEW 이벤트에서 저장할 필드입니다.
type NewOrder struct {
	OrderID         string
	ClientRequestID string
	Market          string
	Side            string
	Price           string
	Quantity        string
	Mode            string
	SubmittedAt     string
}

// ExecutionInput은 executions 토픽 이벤트에서 저장할 필드입니다.
type ExecutionInput struct {
	Market      string
	BuyOrderID  string
	SellOrderID string
	Price       string
	Quantity    string
}

// AssignmentInput은 assignments 토픽 이벤트(FR-11)에서 저장할 필드입니다.
type AssignmentInput struct {
	Market           string
	EngineInstanceID string
	At               string
}

// ExecutionResult는 ApplyExecution이 실제로 한 일을 보고합니다 — apply.go가
// 이 값을 보고 경고 로그를 남길지 결정합니다.
type ExecutionResult struct {
	ExecutionID string
	Mode        string
	// BuyFound/SellFound는 각 주문을 실제로 찾았는지입니다(false면 기록기가
	// 그 주문의 NEW 이벤트를 못 봤다는 뜻 — 기록기가 스트림 중간에 시작된 경우 등).
	// execution 행 자체는 이 값과 무관하게 항상 저장됩니다(FR-09 검증 기준:
	// "Kafka 발행 건수와 DB 저장 건수가 일치").
	BuyFound       bool
	SellFound      bool
	ModeMismatched bool
}

// Store는 TRADE_ORDER/EXECUTION에 대한 쓰기를 추상화합니다.
type Store interface {
	// InsertOrder는 신규 주문을 저장합니다. 같은 order_id가 이미 있으면(재시작
	// 후 컨슈머 그룹의 at-least-once 재전달 등) 아무 일도 하지 않습니다.
	InsertOrder(ctx context.Context, o NewOrder) error
	// CancelOrder는 주문을 취소 상태로 바꿉니다. 대상 주문이 없으면(NEW를 못
	// 본 CANCEL) 아무 일도 하지 않습니다 — 에러가 아닙니다.
	CancelOrder(ctx context.Context, orderID, canceledAt string) error
	// ApplyExecution은 하나의 트랜잭션 안에서: 매수/매도 양쪽 주문의
	// remaining_quantity/status를 갱신하고(존재하는 쪽만), execution 행을
	// 저장합니다. 두 주문 다 없어도 execution 행은 항상 저장됩니다.
	ApplyExecution(ctx context.Context, in ExecutionInput) (ExecutionResult, error)
	// AssignMarket은 FR-11 배정 이벤트를 기록합니다(released_at=NULL인 새 행 추가).
	AssignMarket(ctx context.Context, in AssignmentInput) error
	// ReleaseMarket은 해당 마켓·인스턴스의 열려 있는(released_at IS NULL) 배정을
	// 반납 처리합니다. 그런 배정이 없으면(예: ASSIGNED 이벤트를 못 본 경우) 아무
	// 일도 하지 않습니다 — 에러가 아닙니다.
	ReleaseMarket(ctx context.Context, in AssignmentInput) error
}
