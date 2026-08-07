package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"recorder/idgen"
)

// isoLayout은 이 repo 전체가 쓰는 ISO-8601 UTC ms-정밀도 시각 형식입니다
// (orderapi/server.go의 nowISO(), matching/kafkaclient/assignment_producer.go).
const isoLayout = "2006-01-02T15:04:05.000Z"

// parseTimestamp는 위 형식의 문자열을 time.Time으로 바꿉니다 — go-sql-driver/mysql은
// DATETIME 컬럼에 문자열을 그대로 바인딩하면 안 됩니다(MySQL 자체 DATETIME
// 리터럴 문법은 'T'/'Z' 없이 공백으로 구분 — ISO-8601 문자열을 곧이곧대로
// 넘기면 "Incorrect datetime value" 에러가 납니다, 실제 검증 중 발견). time.Time
// 값으로 바인딩하면 드라이버가 올바르게 변환해줍니다.
func parseTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(isoLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("시각 파싱 실패 (%q): %w", s, err)
	}
	return t, nil
}

// MySQLStore는 Store를 실제 MySQL(RDS)로 구현합니다. 이 파일은 손으로
// 검증합니다(matching/kafkaclient·snapshotstore와 같은 이유) — 실제 DB 왕복이
// 핵심이라 단위 테스트로는 의미 있게 못 검증합니다.
type MySQLStore struct {
	db *sql.DB
}

func NewMySQLStore(db *sql.DB) *MySQLStore {
	return &MySQLStore{db: db}
}

// InsertOrder는 신규 주문을 저장합니다. remaining_quantity는 최초엔 quantity와
// 같습니다. 같은 order_id가 이미 있으면(재전달 등) INSERT IGNORE로 조용히 무시합니다
// (PostgreSQL의 ON CONFLICT DO NOTHING과 같은 의도).
func (s *MySQLStore) InsertOrder(ctx context.Context, o NewOrder) error {
	submittedAt, err := parseTimestamp(o.SubmittedAt)
	if err != nil {
		return fmt.Errorf("주문 저장 실패 (orderId=%s): %w", o.OrderID, err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT IGNORE INTO trade_order
			(order_id, client_request_id, market_code, side, price, quantity, remaining_quantity, status, mode, submitted_at, source_order_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'ACCEPTED', ?, ?, ?)
	`, o.OrderID, nullIfEmpty(o.ClientRequestID), o.Market, o.Side, o.Price, o.Quantity, o.Quantity, o.Mode, submittedAt, nullIfEmpty(o.SourceOrderID))
	if err != nil {
		return fmt.Errorf("주문 저장 실패 (orderId=%s): %w", o.OrderID, err)
	}
	return nil
}

// CancelOrder는 주문을 취소 상태로 바꿉니다. 대상 주문이 없으면(NEW를 못 본
// CANCEL) 영향받는 행이 0개일 뿐, 에러가 아닙니다 — 호출부(apply.go)가 이미
// "찾지 못함"을 에러로 취급하지 않기로 했습니다.
func (s *MySQLStore) CancelOrder(ctx context.Context, orderID, canceledAt string) error {
	parsed, err := parseTimestamp(canceledAt)
	if err != nil {
		return fmt.Errorf("취소 반영 실패 (orderId=%s): %w", orderID, err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE trade_order
		SET status = 'CANCELED', canceled_at = ?
		WHERE order_id = ? AND status <> 'CANCELED'
	`, parsed, orderID)
	if err != nil {
		return fmt.Errorf("취소 반영 실패 (orderId=%s): %w", orderID, err)
	}
	return nil
}

// ApplyExecution은 한 트랜잭션 안에서 매수/매도 양쪽 주문의 remaining_quantity/
// status를 갱신하고 execution 행을 저장합니다. MySQL은 UPDATE...RETURNING이
// 없어서(PostgreSQL 버전과의 차이) updateFill이 "UPDATE로 잠금+계산+쓰기" 한 뒤
// 같은 트랜잭션 안에서 별도 SELECT로 mode를 읽어옵니다 — UPDATE가 그 행에 걸어둔
// 잠금이 커밋까지 유지되므로, 그 사이 다른 트랜잭션이 값을 바꿀 수 없어 안전합니다.
func (s *MySQLStore) ApplyExecution(ctx context.Context, in ExecutionInput) (ExecutionResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("트랜잭션 시작 실패: %w", err)
	}
	defer tx.Rollback()

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

	_, err = tx.ExecContext(ctx, `
		INSERT INTO execution (execution_id, market_code, buy_order_id, sell_order_id, price, quantity, mode, executed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP())
	`, execID, in.Market, in.BuyOrderID, in.SellOrderID, in.Price, in.Quantity, nullIfEmpty(mode))
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("체결 저장 실패: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return ExecutionResult{}, fmt.Errorf("트랜잭션 커밋 실패: %w", err)
	}

	return ExecutionResult{
		ExecutionID: execID, Mode: mode,
		BuyFound: buyFound, SellFound: sellFound, ModeMismatched: mismatched,
	}, nil
}

// updateFill은 한 주문의 remaining_quantity를 qty만큼 SQL 안에서 직접 줄이고
// (Go에서 decimal 연산을 다시 구현할 필요 없음) status를 그 결과에 맞게 바꿉니다.
// UPDATE의 RowsAffected로 주문 존재 여부를 확인한 뒤(0건이면 못 찾은 것 —
// 기록기가 그 주문의 NEW 이벤트를 못 본 경우, 에러 아님), 같은 트랜잭션에서
// mode를 SELECT로 읽어옵니다.
func updateFill(ctx context.Context, tx *sql.Tx, orderID, qty string) (mode string, found bool, err error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE trade_order
		SET remaining_quantity = remaining_quantity - ?,
		    status = CASE WHEN remaining_quantity - ? <= 0 THEN 'FILLED' ELSE 'PARTIALLY_FILLED' END
		WHERE order_id = ?
	`, qty, qty, orderID)
	if err != nil {
		return "", false, fmt.Errorf("주문 체결 반영 실패 (orderId=%s): %w", orderID, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return "", false, fmt.Errorf("영향받은 행 수 확인 실패 (orderId=%s): %w", orderID, err)
	}
	if affected == 0 {
		return "", false, nil
	}

	if err := tx.QueryRowContext(ctx, `SELECT mode FROM trade_order WHERE order_id = ?`, orderID).Scan(&mode); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("주문 mode 조회 실패 (orderId=%s): %w", orderID, err)
	}
	return mode, true, nil
}

// AssignMarket은 FR-11 배정 이벤트를 새 행으로 기록합니다(released_at은 아직 NULL).
// 먼저 이 마켓에 아직 열려 있는(released_at IS NULL) 배정이 있으면 닫습니다 —
// 정상적인 경우 이전 담당자의 Release가 이미 닫아놔서 여기선 0건에 영향을 줄
// 뿐이지만, 이전 담당자가 강제 종료 등으로 RELEASED를 못 보낸 경우(실제 검증
// 중 발견 — 오래된 open 행이 영원히 남아 "released_at IS NULL로 지금 담당자를
// 알 수 있다"는 이 테이블의 존재 목적 자체가 깨졌었음) 새 ASSIGNED가 유일한
// 진짜 담당자라는 걸 알 수 있습니다 — Kafka 그룹의 Generation.Start 계약("새
// 배정을 받았다는 건 이전 담당자가 이미 완전히 멈췄다")이 이 가정을 보장합니다.
func (s *MySQLStore) AssignMarket(ctx context.Context, in AssignmentInput) error {
	assignedAt, err := parseTimestamp(in.At)
	if err != nil {
		return fmt.Errorf("마켓 배정 기록 실패 (market=%s): %w", in.Market, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("마켓 배정 기록 실패 (market=%s): %w", in.Market, err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		UPDATE matching_engine_assignment
		SET released_at = ?
		WHERE market_code = ? AND released_at IS NULL
	`, assignedAt, in.Market); err != nil {
		return fmt.Errorf("마켓 배정 기록 실패 (이전 배정 정리, market=%s): %w", in.Market, err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO matching_engine_assignment (assignment_id, market_code, engine_instance_id, assigned_at)
		VALUES (?, ?, ?, ?)
	`, idgen.NewAssignmentID(), in.Market, in.EngineInstanceID, assignedAt); err != nil {
		return fmt.Errorf("마켓 배정 기록 실패 (market=%s): %w", in.Market, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("마켓 배정 기록 실패 (market=%s): %w", in.Market, err)
	}
	return nil
}

// ReleaseMarket은 해당 마켓·인스턴스의 열려 있는 배정을 반납 처리합니다. 그런
// 배정이 없으면(ASSIGNED 이벤트를 못 본 경우) 영향받는 행이 0개일 뿐, 에러가
// 아닙니다 — CancelOrder와 같은 이유.
func (s *MySQLStore) ReleaseMarket(ctx context.Context, in AssignmentInput) error {
	releasedAt, err := parseTimestamp(in.At)
	if err != nil {
		return fmt.Errorf("마켓 반납 기록 실패 (market=%s): %w", in.Market, err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE matching_engine_assignment
		SET released_at = ?
		WHERE market_code = ? AND engine_instance_id = ? AND released_at IS NULL
	`, releasedAt, in.Market, in.EngineInstanceID)
	if err != nil {
		return fmt.Errorf("마켓 반납 기록 실패 (market=%s): %w", in.Market, err)
	}
	return nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
