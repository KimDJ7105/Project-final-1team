import { createRouter, createWebHistory } from 'vue-router'
import DashboardView from '../views/DashboardView.vue'
import OrderManagementView from '../views/OrderManagementView.vue'

const routes = [
  { path: '/', name: 'Dashboard', component: DashboardView },
  { path: '/orders', name: 'order-management', component: OrderManagementView },
]

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes,
})

export default router
