package main

import (
	"fmt"
	"log"
	"time"

	"backend/dataset"
	"backend/upbit"
)

func main() {
	start := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	for _, market := range upbit.TargetMarkets {
		if err := collectMarket(market, start, end); err != nil {
			log.Printf("[%s] 수집 실패: %v", market, err)
			continue
		}
	}
}

// collectMarket은 한 마켓에 대해 업비트 데이터를 전부 수집하여
// batch/stream JSON 파일로 저장합니다.
func collectMarket(market string, start, end time.Time) error {
	fmt.Printf("=== %s 수집 시작 ===\n", market)

	ticks, err := upbit.FetchTradeTicksForDate(market, start)
	if err != nil {
		return fmt.Errorf("체결 내역 조회 실패: %w", err)
	}

	seconds, err := upbit.FetchCandlesInRange("seconds", market, start, end)
	if err != nil {
		return fmt.Errorf("초봉 조회 실패: %w", err)
	}

	minutes, err := upbit.FetchCandlesInRange("minutes/1", market, start, end)
	if err != nil {
		return fmt.Errorf("분봉 조회 실패: %w", err)
	}

	days, err := upbit.FetchRecentCandles("days", market, end, 1)
	if err != nil {
		return fmt.Errorf("일봉 조회 실패: %w", err)
	}

	weeks, err := upbit.FetchRecentCandles("weeks", market, end, 1)
	if err != nil {
		return fmt.Errorf("주봉 조회 실패: %w", err)
	}

	months, err := upbit.FetchRecentCandles("months", market, end, 1)
	if err != nil {
		return fmt.Errorf("월봉 조회 실패: %w", err)
	}

	years, err := upbit.FetchRecentCandles("years", market, end, 1)
	if err != nil {
		return fmt.Errorf("연봉 조회 실패: %w", err)
	}

	batch := dataset.BuildBatch(market, start, end, days, weeks, months, years)
	batchPath, err := dataset.WriteBatch(batch, start, end)
	if err != nil {
		return fmt.Errorf("batch 저장 실패: %w", err)
	}

	stream := dataset.BuildStream(market, start, end, seconds, minutes, ticks)
	streamPath, err := dataset.WriteStream(stream, start, end)
	if err != nil {
		return fmt.Errorf("stream 저장 실패: %w", err)
	}

	fmt.Printf("[%s] 저장 완료 -> %s, %s\n\n", market, batchPath, streamPath)
	return nil
}
