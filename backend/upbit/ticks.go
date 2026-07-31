package upbit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// TradeTick 구조체는 업비트 REST API(/v1/trades/ticks)가 반환하는 개별 체결 데이터를 담습니다.
type TradeTick struct {
	Market           string  `json:"market"`
	TradeDateUTC     string  `json:"trade_date_utc"`
	TradeTimeUTC     string  `json:"trade_time_utc"`
	Timestamp        int64   `json:"timestamp"`
	TradePrice       float64 `json:"trade_price"`
	TradeVolume      float64 `json:"trade_volume"`
	PrevClosingPrice float64 `json:"prev_closing_price"`
	ChangePrice      float64 `json:"change_price"`
	AskBid           string  `json:"ask_bid"`
	SequentialID     int64   `json:"sequential_id"`
}

const ticksURL = "https://api.upbit.com/v1/trades/ticks"

// FetchTradeTicksForDate는 지정한 UTC 날짜(00:00:00 ~ 다음날 00:00:00 직전) 하루치의
// 전체 체결 내역을 커서 기반 페이지네이션으로 모두 수집하여 반환합니다.
// 업비트 체결 내역 조회는 최근 7일 이내만 지원합니다.
func FetchTradeTicksForDate(market string, date time.Time) ([]TradeTick, error) {
	startOfDay := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24 * time.Hour)

	daysAgo := int(time.Now().UTC().Truncate(24 * time.Hour).Sub(startOfDay).Hours() / 24)
	if daysAgo < 0 || daysAgo > 7 {
		return nil, fmt.Errorf("업비트 체결 내역은 최근 7일 이내만 조회할 수 있습니다 (daysAgo=%d)", daysAgo)
	}

	var result []TradeTick
	cursor := ""

	for {
		req, err := http.NewRequest(http.MethodGet, ticksURL, nil)
		if err != nil {
			return nil, err
		}

		q := req.URL.Query()
		q.Set("market", market)
		q.Set("count", "500")
		if cursor == "" {
			// 첫 요청: 대상 날짜의 마지막 시각부터 과거 방향으로 조회를 시작합니다.
			q.Set("to", "23:59:59")
			q.Set("daysAgo", fmt.Sprintf("%d", daysAgo))
		} else {
			// 이후 요청: 이전 응답의 마지막 sequential_id를 커서로 사용해 더 과거 데이터를 이어서 조회합니다.
			q.Set("cursor", cursor)
		}
		req.URL.RawQuery = q.Encode()

		resp, err := doRequest(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("업비트 API 응답 오류: status=%d", resp.StatusCode)
		}

		var page []TradeTick
		err = json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("응답 파싱 실패: %w", err)
		}

		if len(page) == 0 {
			break
		}

		reachedStart := false
		for _, tick := range page {
			tickTime := time.UnixMilli(tick.Timestamp).UTC()
			if tickTime.Before(startOfDay) {
				reachedStart = true
				continue
			}
			if tickTime.Before(endOfDay) {
				result = append(result, tick)
			}
		}

		fmt.Printf("...%d건 누적 수집 중\n", len(result))

		if reachedStart || len(page) < 500 {
			break
		}

		cursor = fmt.Sprintf("%d", page[len(page)-1].SequentialID)
	}

	return result, nil
}
