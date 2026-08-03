package main

import (
	"encoding/json"
	"net/http"
	"time"

	"backend/dataset"
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

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
