package main

import (
	"fmt"
	"log"
	"time"

	"backend/upbit"
)

func main() {
	market := "KRW-BTC"
	start := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	fmt.Printf("=== %s / %s ~ %s (UTC) 과거 데이터 조회 ===\n\n",
		market, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))

	// 1) 개별 체결 내역 (시각/가격/수량/매수·매도 방향)
	fmt.Println("--- 개별 체결 내역 (tick) ---")
	ticks, err := upbit.FetchTradeTicksForDate(market, start)
	if err != nil {
		log.Fatalf("체결 내역 조회 실패: %v", err)
	}
	for _, t := range ticks {
		fmt.Printf("[%s %s UTC] %s 체결가: %.0f KRW | 체결량: %.8f\n",
			t.TradeDateUTC, t.TradeTimeUTC, t.AskBid, t.TradePrice, t.TradeVolume)
	}
	fmt.Printf("총 %d건의 체결 내역\n\n", len(ticks))

	// 2) 초봉 (1초 단위 캔들)
	fmt.Println("--- 초봉 (1초) ---")
	seconds, err := upbit.FetchCandlesInRange("seconds", market, start, end)
	if err != nil {
		log.Fatalf("초봉 조회 실패: %v", err)
	}
	printCandles(seconds)
	fmt.Printf("총 %d개의 초봉\n\n", len(seconds))

	// 3) 분봉 (1분 단위 캔들)
	fmt.Println("--- 분봉 (1분) ---")
	minutes, err := upbit.FetchCandlesInRange("minutes/1", market, start, end)
	if err != nil {
		log.Fatalf("분봉 조회 실패: %v", err)
	}
	printCandles(minutes)
	fmt.Printf("총 %d개의 분봉\n\n", len(minutes))

	// 4) 일봉
	fmt.Println("--- 일봉 ---")
	days, err := upbit.FetchRecentCandles("days", market, end, 1)
	if err != nil {
		log.Fatalf("일봉 조회 실패: %v", err)
	}
	printCandles(days)
	fmt.Println()

	// 5) 주봉
	fmt.Println("--- 주봉 ---")
	weeks, err := upbit.FetchRecentCandles("weeks", market, end, 1)
	if err != nil {
		log.Fatalf("주봉 조회 실패: %v", err)
	}
	printCandles(weeks)
	fmt.Println()

	// 6) 월봉
	fmt.Println("--- 월봉 ---")
	months, err := upbit.FetchRecentCandles("months", market, end, 1)
	if err != nil {
		log.Fatalf("월봉 조회 실패: %v", err)
	}
	printCandles(months)
	fmt.Println()

	// 7) 연봉
	fmt.Println("--- 연봉 ---")
	years, err := upbit.FetchRecentCandles("years", market, end, 1)
	if err != nil {
		log.Fatalf("연봉 조회 실패: %v", err)
	}
	printCandles(years)
}

func printCandles(candles []upbit.Candle) {
	for _, c := range candles {
		fmt.Printf("[%s UTC] 시가: %.0f | 고가: %.0f | 저가: %.0f | 종가: %.0f | 거래량: %.8f\n",
			c.CandleDateTimeUTC, c.OpeningPrice, c.HighPrice, c.LowPrice, c.TradePrice, c.CandleAccTradeVolume)
	}
}
