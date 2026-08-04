package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"time"

	"trader/bot"
	"trader/client"
	"trader/order"
	"trader/replay"
)

func main() {
	backend := flag.String("backend", "http://localhost:8080", "백엔드 base URL")
	date := flag.String("date", "", "재생할 날짜 (YYYY-MM-DD, 필수)")
	speed := flag.Float64("speed", 60, "재생 배속 (이벤트 간 대기 시간을 이 값으로 나눔)")
	flag.Parse()

	if *date == "" {
		log.Fatal("-date는 필수입니다 (YYYY-MM-DD)")
	}
	if _, err := time.Parse("2006-01-02", *date); err != nil {
		log.Fatalf("-date 형식이 올바르지 않습니다: %v", err)
	}

	httpClient := client.NewHTTPClient()

	manifest, err := client.FetchManifest(context.Background(), httpClient, *backend, *date)
	if err != nil {
		log.Fatalf("매니페스트 조회 실패: %v", err)
	}
	log.Printf("매니페스트 수신: %d개 마켓", len(manifest.Markets))

	// 주문 접수 API(POST /v1/orders)가 준비됐는지 아직 모르므로, 지금은 로그만 남기는
	// 구현체를 씁니다. API가 준비되면 같은 OrderSubmitter 인터페이스의 HTTP 구현체로 교체합니다.
	var submitter order.OrderSubmitter = order.LogOnlySubmitter{}

	// 마켓별 상태를 미리 만들어둡니다 — 마켓별 알고리즘 봇(ReplayMarket 안)과
	// 전체 조망형 AI 봇(RunGlobalBots)이 같은 MarketState를 공유해서 봅니다.
	states := make(map[string]*bot.MarketState, len(manifest.Markets))
	for _, entry := range manifest.Markets {
		states[entry.Market] = bot.NewMarketState(bot.PriceHistorySize)
	}

	// 전체 마켓 재생이 다 끝나면(아래 wg.Wait()) 전체 조망형 봇도 같이 멈춥니다.
	ctx, cancel := context.WithCancel(context.Background())

	var globalWG sync.WaitGroup
	globalWG.Go(func() {
		replay.RunGlobalBots(ctx, states, *speed, submitter)
	})

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed []string

	// 마켓당 고루틴 1개, 전부 NewHTTPClient가 만든 단일 클라이언트를 공유합니다.
	// 한 마켓의 실패가 다른 마켓 재생을 막지 않도록 에러는 로그로만 남기고 계속 진행합니다.
	for _, entry := range manifest.Markets {
		wg.Go(func() {
			if err := replay.ReplayMarket(ctx, httpClient, *backend, entry, *speed, submitter, states[entry.Market]); err != nil {
				log.Printf("[%s] 재생 실패: %v", entry.Market, err)
				mu.Lock()
				failed = append(failed, entry.Market)
				mu.Unlock()
			}
		})
	}

	wg.Wait()
	cancel()
	globalWG.Wait()

	if len(failed) > 0 {
		log.Printf("전체 재생 완료 — 실패한 마켓(%d개): %v", len(failed), failed)
		return
	}
	log.Printf("전체 재생 완료 — %d개 마켓 전부 성공", len(manifest.Markets))
}
