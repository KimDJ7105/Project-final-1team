package main

import "time"

// 아래 두 봇은 requirements.md FR-16의 모멘텀 추종/평균회귀 봇입니다.
// 팀 결정: 이 둘은 규칙 기반이 아니라 AWS Bedrock으로 판단합니다. 아직 Bedrock 연동 전이라
// Bot 인터페이스만 만족하는 자리(연결 통로)만 둡니다 — Decide는 항상 주문 없음을 반환합니다.
// 나중에 이 파일만 실제 Bedrock 호출로 바꾸면 되고, 나머지(주문 생성·제출 파이프라인)는
// 그대로 재사용됩니다(requirements.md 1.2.2 "판단 로직과 주문 생성 로직 분리" 원칙).
const aiJudgeInterval = 5 * time.Second

// MomentumAIBot은 "상승 추종 매수" 봇입니다(FR-16).
type MomentumAIBot struct{}

func (MomentumAIBot) Name() string            { return "momentum_ai" }
func (MomentumAIBot) Interval() time.Duration { return aiJudgeInterval }

func (MomentumAIBot) Decide(state *MarketState) []Decision {
	// TODO: AWS Bedrock 호출 — 최근 시세 이력을 보내 상승/하락 방향 판단을 받아온다.
	return nil
}

// MeanReversionAIBot은 "과열 시 매도" 봇입니다(FR-16).
type MeanReversionAIBot struct{}

func (MeanReversionAIBot) Name() string            { return "mean_reversion_ai" }
func (MeanReversionAIBot) Interval() time.Duration { return aiJudgeInterval }

func (MeanReversionAIBot) Decide(state *MarketState) []Decision {
	// TODO: AWS Bedrock 호출 — 최근 시세 이력을 보내 과열 여부 판단을 받아온다.
	return nil
}
