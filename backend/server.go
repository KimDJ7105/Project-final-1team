package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"backend/dataset"
	"backend/upbit"
)

// collectRequest는 POST /v1/collect의 요청 본문입니다.
type collectRequest struct {
	Date string `json:"date"` // YYYY-MM-DD (UTC 기준 하루)
}

// collectResponse는 POST /v1/collect의 정상 응답 본문입니다.
type collectResponse struct {
	Date    string          `json:"date"`
	Range   dataset.Range   `json:"range"`
	Results []CollectResult `json:"results"`
}

// collectHandler는 요청받은 날짜(UTC 00:00~다음날 00:00) 구간의 데이터를
// upbit.TargetMarkets 전체에 대해 수집해 storage에 저장합니다.
func collectHandler(storage dataset.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req collectRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "요청 본문 JSON 파싱 실패")
			return
		}

		start, err := time.Parse("2006-01-02", req.Date)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "date는 YYYY-MM-DD 형식이어야 합니다")
			return
		}
		start = start.UTC()
		end := start.Add(24 * time.Hour)

		results := collectAllMarkets(storage, start, end)

		writeJSON(w, http.StatusOK, collectResponse{
			Date: req.Date,
			Range: dataset.Range{
				Start: start.Format(time.RFC3339),
				End:   end.Format(time.RFC3339),
			},
			Results: results,
		})
	}
}

// marketManifestEntry는 한 마켓의 batch/stream 파일을 받아올 수 있는 URL입니다.
type marketManifestEntry struct {
	Market    string `json:"market"`
	BatchURL  string `json:"batchUrl"`
	StreamURL string `json:"streamUrl"`
}

// manifestResponse는 GET /v1/markets/data의 응답 본문입니다.
type manifestResponse struct {
	Date    string                `json:"date"`
	Markets []marketManifestEntry `json:"markets"`
}

// manifestHandler는 요청받은 날짜에 대해 upbit.TargetMarkets 전체의
// batch/stream 파일 URL 목록(매니페스트)을 돌려줍니다. 파일 내용 자체는
// GET /v1/markets/{market}/{batch|stream}에서 다루므로 여기서는 저장소를
// 건드리지 않고 URL만 만들어 반환합니다.
func manifestHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		date := r.URL.Query().Get("date")
		if _, err := time.Parse("2006-01-02", date); err != nil {
			writeJSONError(w, http.StatusBadRequest, "date는 YYYY-MM-DD 형식이어야 합니다")
			return
		}

		markets := make([]marketManifestEntry, 0, len(upbit.TargetMarkets))
		for _, market := range upbit.TargetMarkets {
			markets = append(markets, marketManifestEntry{
				Market:    market,
				BatchURL:  fmt.Sprintf("/v1/markets/%s/batch?date=%s", market, date),
				StreamURL: fmt.Sprintf("/v1/markets/%s/stream?date=%s", market, date),
			})
		}

		writeJSON(w, http.StatusOK, manifestResponse{Date: date, Markets: markets})
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
