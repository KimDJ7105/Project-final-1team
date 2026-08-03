package dataset

// Range는 데이터가 커버하는 기간을 나타냅니다.
type Range struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// CandleOHLCV는 batch/stream 공통으로 쓰이는 정규화된 캔들 필드입니다.
type CandleOHLCV struct {
	TS     int64   `json:"ts"`
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
}

// BatchCandles는 일/주/월/년 단위 캔들을 단위별로 묶어 담습니다.
type BatchCandles struct {
	Days   []CandleOHLCV `json:"days"`
	Weeks  []CandleOHLCV `json:"weeks"`
	Months []CandleOHLCV `json:"months"`
	Years  []CandleOHLCV `json:"years"`
}

// BatchFile은 배치 전달용(일/주/월/년봉) JSON 파일 구조입니다.
type BatchFile struct {
	Market  string       `json:"market"`
	Range   Range        `json:"range"`
	Candles BatchCandles `json:"candles"`
}

// StreamEvent는 스트림 파일의 이벤트 하나(초/분봉 또는 개별 체결)입니다.
// 캔들 이벤트는 Open/High/Low/Close/Volume을, 체결 이벤트는 Price/Volume/Side를 채웁니다.
type StreamEvent struct {
	Type   string  `json:"type"`
	TS     int64   `json:"ts"`
	Open   float64 `json:"open,omitempty"`
	High   float64 `json:"high,omitempty"`
	Low    float64 `json:"low,omitempty"`
	Close  float64 `json:"close,omitempty"`
	Volume float64 `json:"volume,omitempty"`
	Price  float64 `json:"price,omitempty"`
	Side   string  `json:"side,omitempty"`
}

// StreamFile은 실시간 전달용(초/분봉 + 체결) JSON 파일 구조입니다.
// events는 ts 오름차순으로 정렬되어 있어, 트레이더 앱은 순서대로 읽으며 재생하면 됩니다.
type StreamFile struct {
	Market string        `json:"market"`
	Range  Range         `json:"range"`
	Events []StreamEvent `json:"events"`
}
