// Package session은 "트레이더/시뮬레이터는 동시에 하나만 실행돼야 한다"는 팀
// 결정(2026-08-06)을 강제하는 배타적 세션 락입니다 — 두 개 이상의 트레이더, 또는
// 트레이더와 리플레이 엔진이 동시에 실행되면 같은 매칭 엔진 호가창에 서로 다른
// 실행의 주문이 섞여 들어가는 상황(undefined로 결론)을 막습니다.
//
// 세션은 실행이 시작될 때 딱 한 번만 클레임합니다 — 주문 하나하나가 오가는
// POST /v1/orders 경로에는 이 패키지가 전혀 관여하지 않으므로 NFR-01(초당
// 10,000건) 처리량에 영향이 없습니다. 클레임 후에는 하트비트로 TTL을 갱신하고,
// 끝나면 명시적으로 반납합니다 — 크래시로 반납을 못 하면 TTL이 지나 자동으로
// 풀립니다(자기치유, matching/kafkaclient의 컨슈머 그룹 세션 타임아웃과 같은 설계
// 철학).
package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// activeKey는 지금 활성 세션의 ID만 담습니다(배타성 보장은 이 키 하나로만
// 이루어집니다 — SETNX/Lua 비교-후-변경이 전부 이 키를 대상으로 합니다).
// metaKey는 활성 세션의 보조 정보(누가, 언제 클레임했는지)를 담아 충돌 메시지를
// 사람이 읽기 좋게 만드는 용도뿐입니다 — activeKey와 원자적으로 묶여 있지 않아도
// 안전합니다(정확성은 activeKey만으로 보장됨).
const (
	activeKey = "orderapi:session:active"
	metaKey   = "orderapi:session:meta"
)

// ErrNotActive는 주어진 sessionID가 지금 활성 세션이 아닐 때(만료됐거나, 처음부터
// 존재한 적이 없거나, 다른 세션으로 교체됐을 때) 반환됩니다.
var ErrNotActive = errors.New("해당 세션은 현재 활성 상태가 아닙니다")

// Info는 클레임 성공 결과 또는(충돌 시) 현재 활성 세션에 대한 정보입니다.
type Info struct {
	SessionID string
	Owner     string
	ClaimedAt time.Time
	TTL       time.Duration
}

// ConflictError는 이미 다른 세션이 활성 상태일 때 Claim이 반환합니다.
type ConflictError struct {
	Current Info
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("이미 활성 세션이 있습니다 (owner=%s, claimedAt=%s)", e.Current.Owner, e.Current.ClaimedAt.Format(time.RFC3339))
}

// Store는 세션 클레임/하트비트/반납을 다룹니다. RedisStore가 실제 구현이고,
// 이 인터페이스는 orderapi의 HTTP 핸들러를 실제 Redis 없이 테스트하기 위해
// 존재합니다(orderapi/kafkaclient.Publisher와 같은 패턴).
type Store interface {
	Claim(ctx context.Context, owner string) (Info, error)
	Heartbeat(ctx context.Context, sessionID string) error
	Release(ctx context.Context, sessionID string) error
}

// RedisStore는 Store를 Redis로 구현합니다.
type RedisStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisStore는 ttl마다 하트비트로 갱신돼야 하는 세션 락을 만듭니다.
func NewRedisStore(client *redis.Client, ttl time.Duration) *RedisStore {
	return &RedisStore{client: client, ttl: ttl}
}

type metaRecord struct {
	Owner     string    `json:"owner"`
	ClaimedAt time.Time `json:"claimedAt"`
}

// Claim은 owner 이름으로 세션을 배타적으로 잡습니다. 이미 다른 세션이 활성
// 상태면 *ConflictError를 반환합니다.
func (s *RedisStore) Claim(ctx context.Context, owner string) (Info, error) {
	id := newSessionID()
	now := time.Now().UTC()

	ok, err := s.client.SetNX(ctx, activeKey, id, s.ttl).Result()
	if err != nil {
		return Info{}, fmt.Errorf("세션 클레임 실패: %w", err)
	}
	if !ok {
		return Info{}, &ConflictError{Current: s.currentInfo(ctx)}
	}

	// meta는 참고 정보뿐이라 만료 시간을 안 둡니다 — 다음 클레임이 성공할 때
	// 그대로 덮어쓰이고, Release가 명시적으로 지웁니다.
	if body, err := json.Marshal(metaRecord{Owner: owner, ClaimedAt: now}); err == nil {
		s.client.Set(ctx, metaKey, body, 0)
	}

	return Info{SessionID: id, Owner: owner, ClaimedAt: now, TTL: s.ttl}, nil
}

// heartbeatScript/releaseScript는 "지금 활성 세션 ID가 내가 갖고 있는 ID와
// 같을 때만" 동작하도록 원자적으로 비교-후-실행합니다 — 그 사이 다른 세션이
// 클레임했다면(예: 내 세션이 만료된 뒤) 잘못된 세션을 갱신/삭제하지 않습니다.
const heartbeatScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
`

const releaseScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
	redis.call('DEL', KEYS[2])
	return redis.call('DEL', KEYS[1])
end
return 0
`

// Heartbeat는 세션의 TTL을 갱신합니다. sessionID가 더는 활성 세션이 아니면
// ErrNotActive를 반환합니다.
func (s *RedisStore) Heartbeat(ctx context.Context, sessionID string) error {
	res, err := s.client.Eval(ctx, heartbeatScript, []string{activeKey}, sessionID, int(s.ttl.Seconds())).Result()
	if err != nil {
		return fmt.Errorf("세션 하트비트 실패: %w", err)
	}
	if n, _ := res.(int64); n == 0 {
		return ErrNotActive
	}
	return nil
}

// Release는 세션을 명시적으로 반납합니다. sessionID가 이미 활성 세션이 아니면
// (이미 반납됐거나 만료됨) ErrNotActive를 반환합니다.
func (s *RedisStore) Release(ctx context.Context, sessionID string) error {
	res, err := s.client.Eval(ctx, releaseScript, []string{activeKey, metaKey}, sessionID).Result()
	if err != nil {
		return fmt.Errorf("세션 반납 실패: %w", err)
	}
	if n, _ := res.(int64); n == 0 {
		return ErrNotActive
	}
	return nil
}

// currentInfo는 충돌 에러 메시지를 사람이 읽기 좋게 만들기 위한 최선의 노력(best
// effort) 조회입니다 — meta 조회가 실패해도 최소한 SessionID는 채워서 돌려줍니다.
func (s *RedisStore) currentInfo(ctx context.Context) Info {
	id, err := s.client.Get(ctx, activeKey).Result()
	if err != nil {
		return Info{}
	}
	info := Info{SessionID: id}

	body, err := s.client.Get(ctx, metaKey).Bytes()
	if err != nil {
		return info
	}
	var m metaRecord
	if err := json.Unmarshal(body, &m); err != nil {
		return info
	}
	info.Owner = m.Owner
	info.ClaimedAt = m.ClaimedAt
	return info
}

func newSessionID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("sess_%d", time.Now().UnixNano())
	}
	return "sess_" + hex.EncodeToString(buf)
}
