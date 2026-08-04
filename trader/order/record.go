package order

import (
	"context"
	"sort"
	"sync"
)

// RecordedOrder는 주문 기록 파일(FR-17) 안의 이벤트 하나입니다. json 태그는
// trader/orderstore가 이 값을 그대로 마샬링해서 저장하는 데 씁니다.
type RecordedOrder struct {
	TS       int64  `json:"ts"`
	Side     string `json:"side"`
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

// OrderRecorder는 주문 하나를 기록에 남깁니다.
type OrderRecorder interface {
	Record(o Order)
}

// InMemoryRecorder는 트레이더 실행 한 번 동안 마켓별로 주문을 메모리에 누적합니다.
// 동시에 여러 고루틴(마켓별 봇 + 전체 조망형 봇)에서 호출될 수 있어 락으로 보호합니다.
type InMemoryRecorder struct {
	mu       sync.Mutex
	byMarket map[string][]RecordedOrder
}

// NewInMemoryRecorder는 빈 InMemoryRecorder를 만듭니다.
func NewInMemoryRecorder() *InMemoryRecorder {
	return &InMemoryRecorder{byMarket: make(map[string][]RecordedOrder)}
}

func (r *InMemoryRecorder) Record(o Order) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byMarket[o.Market] = append(r.byMarket[o.Market], RecordedOrder{
		TS:       o.TS,
		Side:     o.Side,
		Price:    o.Price,
		Quantity: o.Quantity,
	})
}

// Snapshot은 market에 대해 지금까지 기록된 주문들을 ts 오름차순으로 정렬한 복사본으로
// 반환합니다. Record는 여러 고루틴(마켓별 봇 + 전체 조망형 봇)에서 동시에 호출될 수 있어
// 실제 호출(append) 순서가 ts 순서와 정확히 일치한다는 보장이 없으므로, 여기서 명시적으로
// 정렬합니다 — dataset.BuildStream이 이벤트를 ts로 정렬하는 것과 같은 이유입니다.
func (r *InMemoryRecorder) Snapshot(market string) []RecordedOrder {
	r.mu.Lock()
	defer r.mu.Unlock()

	src := r.byMarket[market]
	out := make([]RecordedOrder, len(src))
	copy(out, src)

	sort.Slice(out, func(i, j int) bool { return out[i].TS < out[j].TS })
	return out
}

// RecordingSubmitter는 다른 OrderSubmitter를 감싸서, 제출이 성공한 주문만 Recorder에
// 기록합니다 — FR-17 검증 기준("기록 건수와 접수 건수 일치")에 맞춰 제출 실패한 주문은
// 기록하지 않습니다. replay 패키지는 이 타입도 그냥 OrderSubmitter로만 다루므로 바뀔 게 없습니다.
type RecordingSubmitter struct {
	Next     OrderSubmitter
	Recorder OrderRecorder
}

func (s RecordingSubmitter) Submit(ctx context.Context, o Order) error {
	if err := s.Next.Submit(ctx, o); err != nil {
		return err
	}
	s.Recorder.Record(o)
	return nil
}
