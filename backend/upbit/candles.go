package upbit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Candle 구조체는 업비트 캔들(시가/고가/저가/종가/거래량) API의 공통 응답 필드를 담습니다.
// 일부 필드는 캔들 단위(초/분/일/주/월)에 따라 존재하지 않을 수 있습니다.
type Candle struct {
	Market               string  `json:"market"`
	CandleDateTimeUTC    string  `json:"candle_date_time_utc"`
	CandleDateTimeKST    string  `json:"candle_date_time_kst"`
	OpeningPrice         float64 `json:"opening_price"`
	HighPrice            float64 `json:"high_price"`
	LowPrice             float64 `json:"low_price"`
	TradePrice           float64 `json:"trade_price"` // 종가
	Timestamp            int64   `json:"timestamp"`
	CandleAccTradePrice  float64 `json:"candle_acc_trade_price"`
	CandleAccTradeVolume float64 `json:"candle_acc_trade_volume"`
	Unit                 int     `json:"unit,omitempty"`               // 분봉 전용
	PrevClosingPrice     float64 `json:"prev_closing_price,omitempty"` // 일봉 전용
	ChangePrice          float64 `json:"change_price,omitempty"`
	ChangeRate           float64 `json:"change_rate,omitempty"`
	FirstDayOfPeriod     string  `json:"first_day_of_period,omitempty"` // 주/월봉 전용
}

const candleBaseURL = "https://api.upbit.com/v1/candles"

// FetchCandlesInRange는 초/분봉처럼 촘촘한 단위를 [start, end) UTC 구간 전체에 대해
// 커서(to) 기반으로 과거 방향으로 페이지네이션하며 모두 수집합니다.
// path 예: "seconds", "minutes/1", "minutes/5" 등 (candleBaseURL 뒤에 붙는 경로)
func FetchCandlesInRange(path, market string, start, end time.Time) ([]Candle, error) {
	var result []Candle
	to := end

	for {
		page, err := fetchCandlePage(path, market, to, 200)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}

		reachedStart := false
		oldest := to
		for _, c := range page {
			t, err := parseCandleTimeUTC(c.CandleDateTimeUTC)
			if err != nil {
				return nil, err
			}
			if t.Before(oldest) {
				oldest = t
			}
			if t.Before(start) {
				reachedStart = true
				continue
			}
			if t.Before(end) {
				result = append(result, c)
			}
		}

		fmt.Printf("...(%s) %d건 누적 수집 중\n", path, len(result))

		if reachedStart || len(page) < 200 {
			break
		}

		to = oldest.Add(-1 * time.Second)
	}

	return result, nil
}

// FetchRecentCandles는 일/주/월봉처럼 큰 단위의 캔들을 기준 시각(to) 이전 count개만 조회합니다.
// 하루 범위 안에 있는 일/주/월 캔들을 가져올 때 사용합니다 (예: count=1).
func FetchRecentCandles(path, market string, to time.Time, count int) ([]Candle, error) {
	return fetchCandlePage(path, market, to, count)
}

func fetchCandlePage(path, market string, to time.Time, count int) ([]Candle, error) {
	reqURL := fmt.Sprintf("%s/%s", candleBaseURL, path)

	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("market", market)
	q.Set("count", fmt.Sprintf("%d", count))
	q.Set("to", to.UTC().Format("2006-01-02T15:04:05Z"))
	req.URL.RawQuery = q.Encode()

	resp, err := doRequest(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("업비트 캔들 API 응답 오류(%s): status=%d", path, resp.StatusCode)
	}

	var page []Candle
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, fmt.Errorf("캔들 응답 파싱 실패: %w", err)
	}

	return page, nil
}

func parseCandleTimeUTC(s string) (time.Time, error) {
	t, err := time.Parse("2006-01-02T15:04:05", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("캔들 시각 파싱 실패: %w", err)
	}
	return t.UTC(), nil
}
