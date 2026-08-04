package main

import (
	"sync"
	"time"
)

// Decision은 봇 하나가 한 판단 주기에 내린 결정입니다.
type Decision struct {
	Side     string // "BUY" 또는 "SELL"
	Price    float64
	Quantity float64
}

// MarketState는 재생 중 계속 갱신되는, 봇들이 판단에 참고하는 최신 가격입니다.
type MarketState struct {
	mu    sync.RWMutex
	price float64
	has   bool
}

// Update는 가장 최근에 관측된 가격으로 상태를 갱신합니다.
func (s *MarketState) Update(price float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.price = price
	s.has = true
}

// Price는 현재 알고 있는 가격을 반환합니다. 아직 가격을 한 번도 못 받았으면 ok=false.
func (s *MarketState) Price() (price float64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.price, s.has
}

// Bot은 자기 판단 주기(Interval)마다 MarketState를 보고 주문 여부를 결정합니다.
// requirements.md 1.2.2의 "판단 로직과 주문 생성 로직 분리" 원칙에 따라, Bot은 판단(Decide)만
// 담당하고 실제 주문 구성·제출은 호출부(replay.go)와 order.go가 담당합니다.
type Bot interface {
	Name() string
	Interval() time.Duration
	Decide(state *MarketState) []Decision
}
