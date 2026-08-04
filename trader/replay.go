package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"
)

// ReplayMarket은 한 마켓의 batch/stream을 받아와 이벤트를 ts 간격에 맞춰 순서대로 재생합니다.
// 기본형에서는 각 이벤트를 로그로만 출력합니다 — 주문 생성은 다음 단계에서 붙입니다.
func ReplayMarket(ctx context.Context, client *http.Client, baseURL string, entry ManifestEntry, speed float64) error {
	batch, err := FetchBatch(ctx, client, baseURL, entry.BatchURL)
	if err != nil {
		return fmt.Errorf("batch 조회 실패: %w", err)
	}
	log.Printf("[%s] batch 수신: days=%d weeks=%d months=%d years=%d",
		entry.Market, len(batch.Candles.Days), len(batch.Candles.Weeks), len(batch.Candles.Months), len(batch.Candles.Years))

	stream, err := FetchStream(ctx, client, baseURL, entry.StreamURL)
	if err != nil {
		return fmt.Errorf("stream 조회 실패: %w", err)
	}
	log.Printf("[%s] stream 수신: events=%d, 재생 시작 (배속 %.0fx)", entry.Market, len(stream.Events), speed)

	var lastTS int64
	for i, event := range stream.Events {
		if i > 0 {
			gap := time.Duration(event.TS-lastTS) * time.Millisecond
			if wait := time.Duration(float64(gap) / speed); wait > 0 {
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		lastTS = event.TS
		logEvent(entry.Market, event)
	}

	log.Printf("[%s] 재생 완료 (이벤트 %d건)", entry.Market, len(stream.Events))
	return nil
}

func logEvent(market string, e StreamEvent) {
	if e.Type == "trade_tick" {
		log.Printf("[%s] %s ts=%d price=%.0f volume=%.6f side=%s", market, e.Type, e.TS, e.Price, e.Volume, e.Side)
		return
	}
	log.Printf("[%s] %s ts=%d open=%.0f high=%.0f low=%.0f close=%.0f volume=%.6f",
		market, e.Type, e.TS, e.Open, e.High, e.Low, e.Close, e.Volume)
}
