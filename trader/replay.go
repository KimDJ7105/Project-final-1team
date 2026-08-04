package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// bots는 한 마켓에서 항상 함께 도는 5종 봇입니다(FR-16).
// 모멘텀/평균회귀는 AI(Bedrock) 연결 통로만 있는 스텁이라 지금은 주문을 내지 않습니다.
func bots() []Bot {
	return []Bot{
		MarketMakerBot{},
		NoiseBot{},
		LargeOrderBot{},
		MomentumAIBot{},
		MeanReversionAIBot{},
	}
}

// ReplayMarket은 한 마켓의 batch/stream을 받아와 이벤트를 ts 간격에 맞춰 순서대로 재생하면서,
// 동시에 5종 봇을 각자의 판단 주기로 돌려 주문을 생성·제출합니다.
func ReplayMarket(ctx context.Context, client *http.Client, baseURL string, entry ManifestEntry, speed float64, submitter OrderSubmitter) error {
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

	// 봇 고루틴들은 이벤트 재생과 같은 생애주기를 갖습니다 — 재생이 끝나면 같이 멈춥니다.
	botCtx, cancelBots := context.WithCancel(ctx)
	defer cancelBots()

	state := &MarketState{}
	var botWG sync.WaitGroup
	for _, bot := range bots() {
		botWG.Add(1)
		go func(bot Bot) {
			defer botWG.Done()
			runBot(botCtx, bot, entry.Market, state, speed, submitter)
		}(bot)
	}

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
		state.Update(currentPrice(event))
	}

	cancelBots()
	botWG.Wait()

	log.Printf("[%s] 재생 완료 (이벤트 %d건)", entry.Market, len(stream.Events))
	return nil
}

// runBot은 bot을 자기 판단 주기(배속 적용)마다 실행해, 나온 Decision들을 주문으로 제출합니다.
func runBot(ctx context.Context, bot Bot, market string, state *MarketState, speed float64, submitter OrderSubmitter) {
	interval := time.Duration(float64(bot.Interval()) / speed)
	if interval <= 0 {
		interval = time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, d := range bot.Decide(state) {
				if err := submitter.Submit(ctx, NewOrder(market, d)); err != nil {
					log.Printf("[%s] %s 주문 제출 실패: %v", market, bot.Name(), err)
				}
			}
		}
	}
}

// currentPrice는 이벤트 타입에 맞는 "현재가"를 뽑아냅니다 — 체결은 Price, 캔들은 종가(Close).
func currentPrice(e StreamEvent) float64 {
	if e.Type == "trade_tick" {
		return e.Price
	}
	return e.Close
}

func logEvent(market string, e StreamEvent) {
	if e.Type == "trade_tick" {
		log.Printf("[%s] %s ts=%d price=%.0f volume=%.6f side=%s", market, e.Type, e.TS, e.Price, e.Volume, e.Side)
		return
	}
	log.Printf("[%s] %s ts=%d open=%.0f high=%.0f low=%.0f close=%.0f volume=%.6f",
		market, e.Type, e.TS, e.Open, e.High, e.Low, e.Close, e.Volume)
}
