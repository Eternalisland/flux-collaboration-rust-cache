<template>
  <n-config-provider :theme="theme">
    <n-message-provider>
      <div class="app-shell">
        <header class="app-header">
          <div class="title-block">
            <n-icon size="28" class="title-icon">
              <IconStatsChartOutline />
            </n-icon>
            <div class="title-text">
              <h1>Datahub Rust Jni Stats Dashboard</h1>
              <p>查看 datahub Rust Jni 运行时状态</p>
            </div>
          </div>
<!--          <div class="header-actions">-->
<!--            <n-button quaternary type="primary" @click="toggleTheme">-->
<!--              {{ isDark ? 'Light Mode' : 'Dark Mode' }}-->
<!--            </n-button>-->
<!--          </div>-->
        </header>
        <main class="app-content">
          <HighPerfMmapDashboard />
        </main>
      </div>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import {
  NConfigProvider,
  NMessageProvider,
  NButton,
  NIcon,
  darkTheme,
  lightTheme
} from 'naive-ui';
import { StatsChartOutline } from '@vicons/ionicons5';
import HighPerfMmapDashboard from './views/HighPerfMmapDashboard.vue';

const isDark = ref(false);
const theme = computed(() => (isDark.value ? darkTheme : lightTheme));

function toggleTheme() {
  isDark.value = !isDark.value;
}

const IconStatsChartOutline = StatsChartOutline;
</script>

<style scoped>
.app-shell {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
  background: #f5f7fb;
}

.app-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 18px 28px;
  background: #ffffff;
  border-bottom: 1px solid rgba(30, 60, 114, 0.08);
  box-shadow: 0 10px 24px rgba(45, 78, 142, 0.08);
}

.title-block {
  display: flex;
  align-items: center;
  gap: 16px;
}

.title-icon {
  color: #4a80e0;
}

.title-text h1 {
  margin: 0;
  font-size: 1.6rem;
  font-weight: 600;
  color: #1d2d50;
}

.title-text p {
  margin: 2px 0 0;
  font-size: 0.9rem;
  color: rgba(29, 45, 80, 0.6);
}

.app-content {
  flex: 1 1 auto;
  padding: 32px;
}

.header-actions {
  display: flex;
  gap: 12px;
}
</style>
