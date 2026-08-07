package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"recorder/idgen"
)

// PostgresStore는 Store를 실제 PostgreSQL(RDS)로 구현합니다. 이 파일은 손으로
// 검증합니다(matching/kafkaclient·snapshotstore와 같은 이유) — 실제 DB 왕복이
// 핵심이라 단위 테스트로는 의미 있게 못 검증합니다.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// InsertOrder는 신규 주문을 저장합니다. remaining_quantity는 최초엔 quantity와
// 같습니다. 같은 order_id가 이미 있으면(재전달 등) 조용히 무시합니다.
func (s *PostgresStore) InsertOrder(ctx context.Context, o NewOrder) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO trade_order
			(order_id, client_request_id, market_code, side, price, quantity, remaining_quantity, status, mode, submitted_at)
		VALUES
			($1, $2, $3, $4, $5::numeric, $6::numeric, $6::numeric, 'ACCEPTED', $7, $8::timestamptz)
		ON CONFLICT (order_id) DO NOTHING
	`, o.OrderID, nullIfEmpty(o.ClientRequestID), o.Market, o.Side, o.Price, o.Quantity, o.Mode, o.SubmittedAt)
	if err != nil {
		return fmt.Errorf("주문 저장 실패 (orderId=%s): %w", o.OrderID, err)
	}
	return nil
}

// CancelOrder는 주문을 취소 상태로 바꿉니다. 대상 주문이 없으면(NEW를 못 본
// CANCEL) 영향받는 행이 0개일 뿐, 에러가 아닙니다 — 호출부(apply.go)가 이미
// "찾지 못함"을 에러로 취급하지 않기로 했습니다.
func (s *PostgresStore) CancelOrder(ctx context.Context, orderID, canceledAt string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE trade_order
		SET status = 'CANCELED', canceled_at = $2::timestamptz
		WHERE order_id = $1 AND status <> 'CANCELED'
	`, orderID, canceledAt)
	if err != nil {
		return fmt.Errorf("취소 반영 실패 (orderId=%s): %w", orderID, err)
	}
	return nil
}

// ApplyExecution은 한 트랜잭션 안에서 매수/매도 양쪽 주문의 remaining_quantity/
// status를 갱신하고 execution 행을 저장합니다. updateFill의 UPDATE...RETURNING이
// "잠금+읽기+계산+쓰기"를 한 SQL 문으로 원자적으로 처리하므로, 부분체결이 여러
// 체결 이벤트에 걸쳐 동시에 들어와도(리더가 병렬로 여러 파티션을 읽는 경우)
// remaining_quantity 갱신에 lost-update 레이스가 생기지 않습니다.
func (s *PostgresStore) ApplyExecution(ctx context.Context, in ExecutionInput) (ExecutionResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("트랜잭션 시작 실패: %w", err)
	}
	defer tx.Rollback(ctx)

	buyMode, buyFound, err := updateFill(ctx, tx, in.BuyOrderID, in.Quantity)
	if err != nil {
		return ExecutionResult{}, err
	}
	sellMode, sellFound, err := updateFill(ctx, tx, in.SellOrderID, in.Quantity)
	if err != nil {
		return ExecutionResult{}, err
	}

	mode, mismatched := ResolveMode(buyMode, buyFound, sellMode, sellFound)
	execID := idgen.NewExecutionID()

	_, err = tx.Exec(ctx, `
		INSERT INTO execution (execution_id, market_code, buy_order_id, sell_order_id, price, quantity, mode, executed_at)
		VALUES ($1, $2, $3, $4, $5::numeric, $6::numeric, $7, now())
	`, execID, in.Market, in.BuyOrderID, in.SellOrderID, in.Price, in.Quantity, nullIfEmpty(mode))
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("체결 저장 실패: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return ExecutionResult{}, fmt.Errorf("트랜잭션 커밋 실패: %w", err)
	}

	return ExecutionResult{
		ExecutionID: execID, Mode: mode,
		BuyFound: buyFound, SellFound: sellFound, ModeMismatched: mismatched,
	}, nil
}

// updateFill은 한 주문의 remaining_quantity를 qty만큼 줄이고 status를 그 결과에
// 맞게 바꿉니다(0 이하면 FILLED, 아니면 PARTIALLY_FILLED). RETURNING으로 그
// 주문의 mode를 같이 받아옵니다. 주문이 없으면 found=false(에러 아님) — 기록기가
// 그 주문의 NEW 이벤트를 못 본 경우입니다.
func updateFill(ctx context.Context, tx pgx.Tx, orderID, qty string) (mode string, found bool, err error) {
	err = tx.QueryRow(ctx, `
		UPDATE trade_order
		SET remaining_quantity = remaining_quantity - $2::numeric,
		    status = CASE WHEN remaining_quantity - $2::numeric <= 0 THEN 'FILLED' ELSE 'PARTIALLY_FILLED' END
		WHERE order_id = $1
		RETURNING mode
	`, orderID, qty).Scan(&mode)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("주문 체결 반영 실패 (orderId=%s): %w", orderID, err)
	}
	return mode, true, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
