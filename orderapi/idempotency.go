package main

import "sync"

// cachedResponse는 한 Idempotency-Key에 대해 처음 만들어진 응답(상태 코드+본문)입니다.
type cachedResponse struct {
	status int
	body   []byte
}

// IdempotencyStore는 Idempotency-Key별로 최초 응답을 인메모리로 캐싱합니다
// (docs/api-specification.md 2.2 — 동일 키로 재요청 시 재검증·재발행 없이 최초 응답을
// 그대로 반환). 성공/실패 응답 모두 캐싱합니다 — 실패도 결과가 결정돼 있으니 같은 키로
// 재요청하면 항상 같은 결과가 나와야 "멱등"이라는 이름에 맞습니다.
type IdempotencyStore struct {
	mu    sync.Mutex
	cache map[string]cachedResponse
}

// NewIdempotencyStore는 빈 IdempotencyStore를 만듭니다.
func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{cache: make(map[string]cachedResponse)}
}

// Get은 key에 대해 캐싱된 응답이 있으면 반환합니다.
func (s *IdempotencyStore) Get(key string) (cachedResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.cache[key]
	return r, ok
}

// Put은 key에 대한 응답을 캐싱합니다. 이미 있으면 덮어쓰지 않습니다(최초 응답 유지).
func (s *IdempotencyStore) Put(key string, status int, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.cache[key]; exists {
		return
	}
	s.cache[key] = cachedResponse{status: status, body: body}
}
