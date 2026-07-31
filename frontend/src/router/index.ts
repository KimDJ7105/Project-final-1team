import { createRouter, createWebHistory } from 'vue-router'
import DefaultLayout from '../components/DefaultLayout.vue'
import DashboardView from '../views/DashboardView.vue'
import OrderManagementView from '../views/OrderManagementView.vue'
import MatchingEngineView from '../views/MatchingEngineView.vue'
import MarketOrderBookView from '../views/MarketOrderBookView.vue'
import LoadTestReplayView from '../views/LoadTestReplayView.vue'

const routes = [
  {
    path: '/',
    component: DefaultLayout,
    children: [
      { path: '', name: 'dashboard', component: DashboardView },
      { path: 'orders', name: 'order-management', component: OrderManagementView },
      { path: 'matching-engine', name: 'matching-engine', component: MatchingEngineView },
      { path: 'market-orderbook', name: 'market-orderbook', component: MarketOrderBookView },
      { path: 'load-test/replay', name: 'load-test-replay', component: LoadTestReplayView },
    ],
  },
]

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes,
})

export default router
