<script setup>
import { ref } from 'vue'
import { useRouter, useRoute } from 'vue-router'

const router = useRouter()
const route = useRoute()

const openMenu = ref('overview')

const toggleMenu = (menu) => {
  openMenu.value = openMenu.value === menu ? '' : menu
}

const go = (path) => {
  router.push(path)
}

const isActive = (path) => route.path === path
</script>

<template>
  <div class="app-layout">
    <aside class="sidebar">
      <div class="logo-area">
        <h1>TRUSS</h1>
        <p>K8s Trading Lab</p>
      </div>

      <nav class="navigation">
        <button class="menu-button" type="button" @click="toggleMenu('overview')">
          <span>종합 현황</span>
          <span class="arrow" :class="{ open: openMenu === 'overview' }">›</span>
        </button>

        <div v-if="openMenu === 'overview'" class="submenu">
          <button class="submenu-item selected" type="button" @click="go('/')">
            <span class="menu-dot"></span>
            시스템 종합 현황
          </button>
        </div>

        <button class="menu-button" type="button" @click="toggleMenu('trading')">
          <span>거래 처리</span>
          <span class="arrow" :class="{ open: openMenu === 'trading' }">›</span>
        </button>

        <div v-if="openMenu === 'trading'" class="submenu">
          <button
            class="submenu-item"
            :class="{ selected: isActive('/orders') }"
            type="button"
            @click.prevent="go('/orders')"
          >
            <span class="menu-dot"></span>
            주문 API 검증
          </button>

          <button class="submenu-item" type="button">
            <span class="menu-dot"></span>
            매칭 엔진
          </button>

          <button class="submenu-item" type="button">
            <span class="menu-dot"></span>
            마켓·호가창
          </button>
        </div>

        <button class="menu-button" type="button">
          <span>부하 테스트</span>
          <span class="arrow">›</span>
        </button>

        <button class="menu-button" type="button">
          <span>관찰·검증</span>
          <span class="arrow">›</span>
        </button>

        <button class="menu-button" type="button">
          <span>데이터·운영</span>
          <span class="arrow">›</span>
        </button>
      </nav>

      <div class="system-badge">
        <span class="status-dot"></span>
        시스템 정상
      </div>
    </aside>

    <main class="main-content">
      <slot />
    </main>
  </div>
</template>

<style>
/* Sidebar and layout styles (moved from DashboardView to be shared) */
* {
  box-sizing: border-box;
}

body {
  margin: 0;
  min-width: 1200px;
  min-height: 100vh;
  background: #07111f;
  color: #f3f7fc;
  font-family:
    Inter,
    'Noto Sans KR',
    -apple-system,
    BlinkMacSystemFont,
    'Segoe UI',
    sans-serif;
}

button {
  font: inherit;
}

.app-layout {
  display: flex;
  min-height: 100vh;
  background: #07111f;
}

.sidebar {
  position: fixed;
  inset: 0 auto 0 0;
  display: flex;
  width: 230px;
  padding: 30px 20px 24px;
  flex-direction: column;
  background: #091625;
  border-right: 1px solid #172a3e;
}

.logo-area {
  padding: 0 8px 28px;
}

.logo-area h1 {
  margin: 0;
  font-size: 25px;
  letter-spacing: 1px;
}

.logo-area p {
  margin: 5px 0 0;
  color: #20c8e8;
  font-size: 12px;
}

.navigation {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.menu-button {
  display: flex;
  width: 100%;
  padding: 12px;
  align-items: center;
  justify-content: space-between;
  color: #8ea2b8;
  background: transparent;
  border: 0;
  border-radius: 10px;
  cursor: pointer;
}

.menu-button:hover,
.menu-button.active {
  color: #f3f7fc;
  background: #0d1b2a;
}

.arrow {
  font-size: 20px;
  transition: transform 0.2s ease;
}

.arrow.open {
  color: #3478f6;
  transform: rotate(90deg);
}

.submenu {
  padding: 0 0 5px 8px;
}

.submenu-item {
  display: flex;
  width: 100%;
  padding: 11px 12px;
  align-items: center;
  gap: 10px;
  color: #f3f7fc;
  background: #11243a;
  border: 0;
  border-radius: 9px;
  cursor: pointer;
}

.submenu-item.selected {
  background: #0d1b2a;
}

.menu-dot {
  width: 8px;
  height: 8px;
  background: #3478f6;
  border-radius: 50%;
}

.system-badge {
  display: flex;
  margin-top: auto;
  padding: 10px 13px;
  align-items: center;
  gap: 8px;
  color: #2ed39a;
  background: #11243a;
  border-radius: 20px;
  font-size: 12px;
  font-weight: 700;
}

.status-dot,
.live-dot {
  width: 8px;
  height: 8px;
  background: #2ed39a;
  border-radius: 50%;
}

.main-content {
  width: calc(100% - 230px);
  max-width: 1500px;
  margin-left: 230px;
  padding: 32px;
}

/* Simple helpers for panels to match existing dashboard */
.panel {
  background: #0d1b2a;
  border: 1px solid #172a3e;
  border-radius: 15px;
  padding: 20px;
}

.metric-card {
  padding: 18px;
}
</style>
