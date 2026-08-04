package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// globalBots는 20개 마켓을 한 번에 조망하며 판단하는 봇입니다(AI 트레이더, FR-16).
func globalBots() []GlobalBot {
	return []GlobalBot{
		MomentumAIBot{},
		MeanReversionAIBot{},
	}
}

// RunGlobalBots는 globalBots를 각자 판단 주기로 돌립니다. ctx가 취소되면(전체 마켓
// 재생 종료) 멈춥니다. states는 main.go가 마켓별로 미리 만들어 각 ReplayMarket과
// 공유하는 것과 동일한 맵으로, 여기서는 읽기만 합니다.
func RunGlobalBots(ctx context.Context, states map[string]*MarketState, speed float64, submitter OrderSubmitter) {
	var wg sync.WaitGroup
	for _, bot := range globalBots() {
		wg.Go(func() {
			runGlobalBot(ctx, bot, states, speed, submitter)
		})
	}
	wg.Wait()
}

func runGlobalBot(ctx context.Context, bot GlobalBot, states map[string]*MarketState, speed float64, submitter OrderSubmitter) {
	// TODO(Bedrock 연동 시 재검토): 다른 봇들과 동일하게 배속으로 주기를 나누고 있지만,
	// 실제 Bedrock 호출 지연은 배속에 따라 줄어들지 않는다. 배속을 올리면 이전 호출이
	// 끝나기 전에 다음 틱이 오는 상황이 생길 수 있어, 연동 시점에 "고정 실주기로 돌리기"
	// 또는 "이전 호출 진행 중이면 이번 틱 스킵" 중 하나로 다시 설계해야 한다.
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
			for _, gd := range bot.Decide(states) {
				if err := submitter.Submit(ctx, NewOrder(gd.Market, gd.Decision)); err != nil {
					log.Printf("[global] %s 주문 제출 실패 (%s): %v", bot.Name(), gd.Market, err)
				}
			}
		}
	}
}
