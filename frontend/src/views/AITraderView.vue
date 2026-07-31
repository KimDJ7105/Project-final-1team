<script setup lang="ts">
import { ref, computed } from 'vue'

// Defaults
const defaultScenarioName = 'BTC 급등락 부하 시나리오'
const defaultMarket = 'BTC/KRW'
const defaultSnapshot = 'upbit-btc-2026-07-snapshot.jsonl'
const defaultTotalOrders = 1800000
const defaultGenerationTime = 60 // seconds

const scenarioName = ref(defaultScenarioName)
const market = ref(defaultMarket)
const snapshot = ref(defaultSnapshot)
const startTime = ref('')
const endTime = ref('')
const totalOrders = ref(defaultTotalOrders)
const generationTime = ref(defaultGenerationTime)

const markets = ['BTC/KRW', 'ETH/KRW', 'XRP/KRW', 'SOL/KRW', 'DOGE/KRW']
const snapshots = [
  'upbit-btc-2026-07-snapshot.jsonl',
  'upbit-eth-2026-07-snapshot.jsonl',
  'multi-market-20-v1.jsonl',
]

// Trader type sliders
const mm = ref(52)
const momentum = ref(14)
const meanReversion = ref(13)
const noise = ref(16)
const whale = ref(5)

const traderSum = computed(
  () => mm.value + momentum.value + meanReversion.value + noise.value + whale.value,
)

const buyPercent = computed(() => 60) // static for mock
const sellPercent = computed(() => 100 - buyPercent.value)

const targetThroughput = computed(() => {
  const secs = Number(generationTime.value) || 1
  const total = Number(totalOrders.value) || 0
  const val = Math.round(total / secs)
  return `${val.toLocaleString()} orders/sec`
})

// Right-side dummy graph (fixed array)
const graphData = [12, 18, 22, 20, 26, 30, 28, 34, 30, 24, 20, 18, 22, 26, 30, 32, 28, 24]

// Creation result (mock)
const creating = ref(false)
const created = ref(false)
const result = ref({
  status: '',
  id: '',
  file: '',
  orders: 0,
})

const canCreate = computed(() => {
  if (!scenarioName.value) return false
  if (!market.value) return false
  if (!snapshot.value) return false
  if (!startTime.value || !endTime.value) return false
  if (new Date(startTime.value) >= new Date(endTime.value)) return false
  if (!(totalOrders.value > 0)) return false
  if (!(generationTime.value > 0)) return false
  if (traderSum.value !== 100) return false
  return true
})

const createScenario = async () => {
  if (!canCreate.value) return
  creating.value = true
  created.value = false

  // TODO: POST /api/trader/scenarios 연동
  setTimeout(() => {
    result.value = {
      status: '생성 완료',
      id: 'SCN-20260731-001',
      file: 'ai-orders-btc-20260731-v1.jsonl',
      orders: totalOrders.value,
    }
    creating.value = false
    created.value = true
  }, 800)
}

const reset = () => {
  scenarioName.value = defaultScenarioName
  market.value = defaultMarket
  snapshot.value = defaultSnapshot
  startTime.value = ''
  endTime.value = ''
  totalOrders.value = defaultTotalOrders
  generationTime.value = defaultGenerationTime
  mm.value = 52
  momentum.value = 14
  meanReversion.value = 13
  noise.value = 16
  whale.value = 5
  creating.value = false
  created.value = false
  result.value = { status: '', id: '', file: '', orders: 0 }
}
</script>

<template>
  <div>
    <header class="page-header">
      <h2>AI 트레이더</h2>
      <p class="subtitle">과거 시세를 기반으로 매수·매도 주문 패턴과 부하 시나리오를 생성합니다</p>
      <hr />
    </header>

    <div class="content-grid">
      <section class="panel left-panel">
        <h3 class="panel-title">AI 주문 시나리오 생성</h3>
        <p class="panel-sub">과거 시세 스냅샷과 생성 조건을 설정합니다</p>

        <div class="form-field">
          <label>시나리오 이름</label>
          <input v-model="scenarioName" type="text" />
        </div>

        <div class="form-field">
          <label>대상 마켓</label>
          <select v-model="market">
            <option v-for="m in markets" :key="m">{{ m }}</option>
          </select>
        </div>

        <div class="form-field">
          <label>시세 스냅샷</label>
          <select v-model="snapshot">
            <option v-for="s in snapshots" :key="s">{{ s }}</option>
          </select>
        </div>

        <div class="form-field two-cols">
          <div>
            <label>과거 데이터 시작 시각</label>
            <input v-model="startTime" type="datetime-local" />
          </div>
          <div>
            <label>과거 데이터 종료 시각</label>
            <input v-model="endTime" type="datetime-local" />
          </div>
        </div>

        <div class="form-field two-cols">
          <div>
            <label>목표 주문 수</label>
            <input v-model.number="totalOrders" type="number" />
          </div>
          <div>
            <label>생성 시간 (sec)</label>
            <input v-model.number="generationTime" type="number" />
          </div>
        </div>

        <div class="form-field">
          <label>목표 처리량</label>
          <div class="readonly-input">{{ targetThroughput }}</div>
        </div>

        <h4 class="section-title">트레이더 유형 비율</h4>

        <div
          class="trader-row"
          v-for="(t, idx) in [
            { name: '마켓메이커', ref: mm },
            { name: '모멘텀 추종', ref: momentum },
            { name: '평균회귀', ref: meanReversion },
            { name: '노이즈', ref: noise },
            { name: '대량 주문자', ref: whale },
          ]"
          :key="idx"
        >
          <div class="trader-label">{{ t.name }}</div>
          <input type="range" min="0" max="100" v-model.number="t.ref.value" />
          <div class="trader-value">{{ t.ref.value }}%</div>
        </div>

        <div class="ratio-sum" :class="{ valid: traderSum === 100, invalid: traderSum !== 100 }">
          <span v-if="traderSum === 100">트레이더 비율 합계 100%</span>
          <span v-else>트레이더 비율의 합계가 100%여야 합니다</span>
        </div>

        <div class="actions">
          <button class="btn-primary" :disabled="!canCreate || creating" @click="createScenario">
            AI 시나리오 생성
          </button>
          <button class="btn-dark" @click="reset">초기화</button>
        </div>

        <div v-if="created" class="created-box">
          <div class="created-left">
            <span class="status-dot"></span>
            <div>
              <div class="created-status">{{ result.status }}</div>
              <div class="created-sub">시나리오 ID: {{ result.id }}</div>
            </div>
          </div>
          <div class="created-right">
            <div>저장 파일: {{ result.file }}</div>
            <div>생성 주문: {{ result.orders.toLocaleString() }} orders</div>
          </div>
          <div class="created-note">시나리오 저장 완료 · 주문 재생 준비됨</div>
        </div>
      </section>

      <aside class="panel right-panel">
        <h3 class="panel-title">주문 패턴 미리보기</h3>
        <p class="panel-sub">설정값을 기반으로 예상 주문 분포를 표시합니다</p>

        <div class="bar-chart">
          <div class="bars">
            <div
              v-for="(v, i) in graphData"
              :key="i"
              class="bar"
              :class="{ sell: i % 3 === 0 }"
              :style="{ height: v * 3 + 'px' }"
            ></div>
          </div>
        </div>

        <div class="summary-cards">
          <div class="summary-item">
            <div class="summary-title">전체 주문</div>
            <div class="summary-value">{{ totalOrders.toLocaleString() }}</div>
          </div>

          <div class="summary-item">
            <div class="summary-title">예상 매수 주문</div>
            <div class="summary-value">
              {{ Math.round(totalOrders * (buyPercent / 100)).toLocaleString() }} ·
              {{ buyPercent }}%
            </div>
          </div>

          <div class="summary-item">
            <div class="summary-title">예상 매도 주문</div>
            <div class="summary-value">
              {{ Math.round(totalOrders * (sellPercent / 100)).toLocaleString() }} ·
              {{ sellPercent }}%
            </div>
          </div>

          <div class="summary-item">
            <div class="summary-title">목표 처리량</div>
            <div class="summary-value">{{ targetThroughput }}</div>
          </div>

          <div class="summary-item">
            <div class="summary-title">대상 마켓</div>
            <div class="summary-value">{{ market }}</div>
          </div>

          <div class="summary-item">
            <div class="summary-title">데이터 기간</div>
            <div class="summary-value">{{ startTime || '-' }} → {{ endTime || '-' }}</div>
          </div>
        </div>

        <h4 class="section-title">유형별 분포</h4>
        <div class="ratios">
          <div class="ratio-row">
            <div class="ratio-label">마켓메이커</div>
            <div class="ratio-bar">
              <div class="ratio-fill" :style="{ width: mm + '%', background: '#3478f6' }"></div>
            </div>
            <div class="ratio-value">{{ mm }}%</div>
          </div>

          <div class="ratio-row">
            <div class="ratio-label">모멘텀 추종</div>
            <div class="ratio-bar">
              <div
                class="ratio-fill"
                :style="{ width: momentum + '%', background: '#20c8e8' }"
              ></div>
            </div>
            <div class="ratio-value">{{ momentum }}%</div>
          </div>

          <div class="ratio-row">
            <div class="ratio-label">평균회귀</div>
            <div class="ratio-bar">
              <div
                class="ratio-fill"
                :style="{ width: meanReversion + '%', background: '#2ed39a' }"
              ></div>
            </div>
            <div class="ratio-value">{{ meanReversion }}%</div>
          </div>

          <div class="ratio-row">
            <div class="ratio-label">노이즈</div>
            <div class="ratio-bar">
              <div class="ratio-fill" :style="{ width: noise + '%', background: '#8b5cf6' }"></div>
            </div>
            <div class="ratio-value">{{ noise }}%</div>
          </div>

          <div class="ratio-row">
            <div class="ratio-label">대량 주문자</div>
            <div class="ratio-bar">
              <div class="ratio-fill" :style="{ width: whale + '%', background: '#fbbf24' }"></div>
            </div>
            <div class="ratio-value">{{ whale }}%</div>
          </div>
        </div>

        <div class="mock-note">TRUSS 내부 부하 테스트용 모의 주문 · 실제 자산 거래 없음</div>
      </aside>
    </div>
  </div>
</template>

<style scoped>
.page-header h2 {
  margin: 0 0 6px 0;
  font-size: 22px;
  color: #ffffff;
}
.subtitle {
  margin: 0 0 14px 0;
  color: #9fb0c2;
}
.page-header hr {
  border: 0;
  height: 1px;
  background: #0f2636;
  margin-bottom: 20px;
}
.content-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 24px;
}
.panel {
  background: #0d1b2a;
  border: 1px solid #172a3e;
  border-radius: 12px;
  padding: 20px;
}
.panel-title {
  font-size: 16px;
  margin: 0 0 6px 0;
}
.panel-sub {
  color: #9fb0c2;
  margin: 0 0 16px 0;
}
.form-field {
  margin-bottom: 12px;
}
.form-field label {
  display: block;
  font-size: 12px;
  color: #9fb0c2;
  margin-bottom: 8px;
}
.form-field input,
.form-field select {
  width: 100%;
  padding: 12px 14px;
  background: #072037;
  border: 1px solid #163247;
  color: #e6eef8;
  border-radius: 8px;
  outline: none;
}
.two-cols {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
.readonly-input {
  padding: 12px 14px;
  background: #072037;
  border: 1px solid #163247;
  color: #cfe6ff;
  border-radius: 8px;
}
.section-title {
  margin: 16px 0 8px 0;
  color: #d7e8fb;
}
.trader-row {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
}
.trader-label {
  width: 120px;
  color: #c6d6e6;
}
.trader-row input[type='range'] {
  flex: 1;
}
.trader-value {
  width: 48px;
  text-align: right;
  color: #c6d6e6;
}
.ratio-sum {
  margin-top: 8px;
  font-weight: 700;
}
.ratio-sum.valid {
  color: #2ed39a;
}
.ratio-sum.invalid {
  color: #ff6b6b;
}
.actions {
  display: flex;
  gap: 12px;
  margin-top: 16px;
}
.btn-primary {
  flex: 1.4;
  background: #3f86ff;
  color: #fff;
  border: 0;
  padding: 12px 18px;
  border-radius: 10px;
  cursor: pointer;
}
.btn-primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.btn-dark {
  flex: 0.8;
  background: #18324a;
  color: #e6eef8;
  border: 0;
  padding: 12px 18px;
  border-radius: 10px;
  cursor: pointer;
}
.created-box {
  margin-top: 14px;
  background: #081826;
  border-radius: 10px;
  padding: 12px;
  color: #cfe6ff;
}
.created-left {
  display: flex;
  align-items: center;
  gap: 12px;
}
.created-status {
  font-weight: 700;
}
.created-sub {
  color: #9fb0c2;
  font-size: 13px;
}
.created-right {
  margin-top: 8px;
  color: #c6d6e6;
}
.created-note {
  margin-top: 8px;
  color: #2ed39a;
  font-weight: 700;
}
.status-dot {
  width: 10px;
  height: 10px;
  background: #2ed39a;
  border-radius: 50%;
}
.bar-chart .bars {
  display: flex;
  align-items: flex-end;
  gap: 6px;
  height: 140px;
  padding: 12px 6px;
}
.bar-chart .bar {
  width: 14px;
  border-radius: 6px 6px 0 0;
}
.bar-chart .bar.sell {
  background: #8b5cf6;
}
.bar-chart .bar:not(.sell) {
  background: #20c8e8;
}
.summary-cards {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
  margin-top: 12px;
}
.summary-item {
  background: #071a28;
  padding: 10px;
  border-radius: 8px;
  border: 1px solid #122a3d;
  color: #cfe6ff;
}
.summary-title {
  font-size: 12px;
  color: #9fb0c2;
}
.summary-value {
  font-weight: 700;
  margin-top: 4px;
}
.ratios {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-top: 10px;
}
.ratio-row {
  display: flex;
  align-items: center;
  gap: 12px;
}
.ratio-label {
  width: 120px;
  color: #c6d6e6;
}
.ratio-bar {
  flex: 1;
  height: 12px;
  background: #072b45;
  border-radius: 8px;
  overflow: hidden;
}
.ratio-fill {
  height: 100%;
}
.ratio-value {
  width: 48px;
  text-align: right;
  color: #c6d6e6;
}
.mock-note {
  margin-top: 12px;
  color: #9fb0c2;
  font-size: 13px;
}

@media (max-width: 900px) {
  .content-grid {
    grid-template-columns: 1fr;
  }
}
</style>
