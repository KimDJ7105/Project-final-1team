package bot

import "time"

// 아래 두 봇은 requirements.md FR-16의 모멘텀 추종/평균회귀 봇입니다.
// 팀 결정: 규칙 기반이 아니라 AWS Bedrock으로 판단하고, 마켓별로 따로 두지 않고
// "20개 마켓을 한 번에 조망해서 그중 어디를 얼마나 살지/팔지" 판단하는 전체 조망형
// (포트폴리오형)으로 만든다 — 비용 면에서도(호출 20회 대신 2회), "AI 트레이더"라는
// 이름에도 이 편이 더 맞다는 판단.
//
// 아직 Bedrock 연동 전이라 GlobalBot 인터페이스만 만족하는 자리(연결 통로)만 둡니다.
// Decide는 항상 빈 슬라이스를 반환합니다. 나중에 이 파일만 실제 Bedrock 호출로 바꾸면
// 되고, 나머지(주문 생성·제출 파이프라인)는 그대로 재사용됩니다.
const aiJudgeInterval = 5 * time.Second

// MomentumAIBot은 "상승 추종 매수" 봇입니다(FR-16).
type MomentumAIBot struct{}

func (MomentumAIBot) Name() string            { return "momentum_ai" }
func (MomentumAIBot) Interval() time.Duration { return aiJudgeInterval }

func (MomentumAIBot) Decide(states map[string]*MarketState) []GlobalDecision {
	// TODO: AWS Bedrock 호출 — states의 마켓마다 MarketState.History()로 최근 가격
	// 흐름을 뽑아 한 프롬프트에 담아 보내고, 상승세인 마켓이 있으면 매수 판단을 받아온다.
	// 배속(speed)과 실제 Bedrock 응답 지연이 안 맞는 문제는 아직 미해결 — 연동 시점에
	// replay 패키지의 티커 계산 방식을 다시 봐야 한다 (replay/globalbot.go 주석 참고).
	return nil
}

// MeanReversionAIBot은 "과열 시 매도" 봇입니다(FR-16).
type MeanReversionAIBot struct{}

func (MeanReversionAIBot) Name() string            { return "mean_reversion_ai" }
func (MeanReversionAIBot) Interval() time.Duration { return aiJudgeInterval }

func (MeanReversionAIBot) Decide(states map[string]*MarketState) []GlobalDecision {
	// TODO: AWS Bedrock 호출 — states의 마켓마다 최근 가격 이력을 평균/이동평균과 비교해
	// 과열된 마켓이 있으면 매도 판단을 받아온다.
	return nil
}
