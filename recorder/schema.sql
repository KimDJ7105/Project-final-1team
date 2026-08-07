-- docs/erd.md 기준 스키마. replay_session_id/source_order_id/REPLAY_SESSION은
-- 아직 미해결(erd.md §5)이라 이 스키마에서는 제외한다. 마이그레이션 툴 없이
-- psql로 손으로 적용한다(이 repo의 dev-simple 컨벤션 — docker-compose들도
-- 볼륨/영속성 없이 극단적으로 단순함).
--
-- 로컬: psql "$DATABASE_URL" -f schema.sql (infra/dev-postgres 기준
-- postgres://recorder:recorder@localhost:5432/recorder)

CREATE TABLE IF NOT EXISTS market (
    market_code   TEXT PRIMARY KEY,
    korean_name   TEXT NOT NULL,
    symbol        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trade_order (
    order_id            TEXT PRIMARY KEY,
    client_request_id   TEXT UNIQUE,
    market_code         TEXT NOT NULL REFERENCES market(market_code),
    side                TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    price               NUMERIC NOT NULL,
    quantity            NUMERIC NOT NULL,
    remaining_quantity  NUMERIC NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('ACCEPTED','PARTIALLY_FILLED','FILLED','CANCELED')),
    mode                TEXT NOT NULL CHECK (mode IN ('PAPER_TRADING','REPLAY')),
    submitted_at        TIMESTAMPTZ NOT NULL,
    canceled_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_trade_order_market_status ON trade_order (market_code, status);

CREATE TABLE IF NOT EXISTS execution (
    execution_id   TEXT PRIMARY KEY,
    market_code    TEXT NOT NULL REFERENCES market(market_code),
    -- buy_order_id/sell_order_id는 의도적으로 trade_order에 대한 FK가 아닙니다 —
    -- 기록기는 orders/executions를 서로 독립된 리더로 소비하므로, 아직 NEW를
    -- 못 본 주문의 체결이 먼저 도착할 수 있습니다(위 mode 컬럼과 같은 이유).
    -- FR-09 검증 기준("Kafka 발행 건수와 DB 저장 건수가 일치")이 execution 저장
    -- 자체를 그 경우에도 절대 막지 말라고 요구하므로, FK로 강제하면 안 됩니다.
    buy_order_id   TEXT NOT NULL,
    sell_order_id  TEXT NOT NULL,
    price          NUMERIC NOT NULL,
    quantity       NUMERIC NOT NULL,
    -- mode는 NULL을 허용합니다 — orders/executions 리더가 서로 독립적으로 각자
    -- 뒤처진 만큼 따라잡기 때문에, 기록기가 스트림을 처음부터(또는 재시작 후
    -- 밀린 채로) 따라잡는 동안 아직 두 주문 다 못 본 체결이 실제로 생길 수
    -- 있습니다(recorder/store/apply.go의 ResolveMode가 이 경우 ""를 반환) —
    -- execution 행 자체는 FR-09 검증 기준 때문에 항상 저장해야 하므로, "모른다"를
    -- 유효한 상태로 표현해야 합니다.
    mode           TEXT CHECK (mode IN ('PAPER_TRADING','REPLAY')),
    executed_at    TIMESTAMPTZ NOT NULL
);
-- FR-13 "최근 순으로 거래 내역을 조회" 페이지네이션용.
CREATE INDEX IF NOT EXISTS idx_execution_market_executed_at ON execution (market_code, executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_buy_order  ON execution (buy_order_id);
CREATE INDEX IF NOT EXISTS idx_execution_sell_order ON execution (sell_order_id);

-- 20개 마켓 마스터 데이터 시드 (backend/upbit.TargetMarkets, orderapi/validate.TargetMarkets와
-- 같은 20개 목록 — 모듈 독립 원칙에 따라 이 파일에도 값을 그대로 적어둔다).
INSERT INTO market (market_code, korean_name, symbol) VALUES
    ('KRW-USDT', '테더', 'USDT'),
    ('KRW-BTC',  '비트코인', 'BTC'),
    ('KRW-XRP',  '리플', 'XRP'),
    ('KRW-ETH',  '이더리움', 'ETH'),
    ('KRW-ONDO', '온도파이낸스', 'ONDO'),
    ('KRW-LA',   '라이덱스', 'LA'),
    ('KRW-SHIB', '시바이누', 'SHIB'),
    ('KRW-RE',   '리버스', 'RE'),
    ('KRW-DOGE', '도지코인', 'DOGE'),
    ('KRW-SLX',  '솔렛수', 'SLX'),
    ('KRW-KAITO','카이토', 'KAITO'),
    ('KRW-SOL',  '솔라나', 'SOL'),
    ('KRW-XLM',  '스텔라루멘', 'XLM'),
    ('KRW-WLD',  '월드코인', 'WLD'),
    ('KRW-MIRA', '미라', 'MIRA'),
    ('KRW-ERA',  '에라', 'ERA'),
    ('KRW-ADA',  '에이다', 'ADA'),
    ('KRW-AI',   '에이아이', 'AI'),
    ('KRW-NEAR', '니어프로토콜', 'NEAR'),
    ('KRW-ARX',  '아크스', 'ARX')
ON CONFLICT (market_code) DO NOTHING;
