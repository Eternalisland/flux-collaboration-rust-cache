<template>
  <div class="instance-summary">
    <div class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-label">总记录数</div>
        <div class="kpi-value">{{ stats.totalRecords.toLocaleString() }}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">写入次数</div>
        <div class="kpi-value">{{ stats.totalWrites.toLocaleString() }}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">读取次数</div>
        <div class="kpi-value">{{ stats.totalReads.toLocaleString() }}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">删除次数</div>
        <div class="kpi-value">{{ stats.totalDeletes.toLocaleString() }}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">写入总量</div>
        <div class="kpi-value">{{ formatBytes(stats.totalWriteBytes) }}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">读取总量</div>
        <div class="kpi-value">{{ formatBytes(stats.totalReadBytes) }}</div>
      </div>
    </div>
    <div class="insight-row">
      <div class="insight-card insight-card--progress">
        <div class="insight-label">L1 命中率</div>
        <n-progress
          type="line"
          :percentage="l1HitRate * 100"
          :show-indicator="false"
          class="insight-progress"
        />
        <div class="insight-value">{{ (l1HitRate * 100).toFixed(1) }}%</div>
      </div>
      <div class="insight-card">
        <div class="insight-label">平均写延迟</div>
        <div class="insight-value">{{ stats.avgWriteLatencyUs.toLocaleString() }} μs</div>
      </div>
      <div class="insight-card">
        <div class="insight-label">平均读延迟</div>
        <div class="insight-value">{{ stats.avgReadLatencyUs.toLocaleString() }} μs</div>
      </div>
      <div class="insight-card">
        <div class="insight-label">堆内存</div>
        <div class="insight-value">{{ formatBytes(memory.totalHeapBytes) }}</div>
        <div class="insight-sub">MMAP：{{ formatBytes(memory.mmapBytes) }}</div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { NProgress } from 'naive-ui';
import type { HighPerfMmapStats, MemoryStats } from '../types/highPerfMmap';
import { formatBytes } from '../utils/formatters';

const props = defineProps<{
  stats: HighPerfMmapStats;
  memory: MemoryStats;
}>();

const l1HitRate = computed(() => {
  const total = props.stats.l1CacheHits + props.stats.l1CacheMisses;
  if (!total) return 0;
  return props.stats.l1CacheHits / total;
});
</script>

<style scoped>
.instance-summary {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.kpi-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 10px;
}

.kpi-card {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding: 10px;
  border: 1px solid rgba(74, 128, 224, 0.08);
  border-radius: 10px;
  background: #f9fbff;
  box-shadow: 0 2px 8px rgba(74, 128, 224, 0.06);
}

.kpi-label {
  font-size: 0.72rem;
  color: rgba(29, 45, 80, 0.55);
}

.kpi-value {
  font-size: 1.02rem;
  font-weight: 600;
  color: #1d2d50;
}

.insight-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 10px;
}

.insight-card {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 10px;
  border: 1px dashed rgba(74, 128, 224, 0.18);
  border-radius: 10px;
  background: rgba(247, 250, 255, 0.85);
}

.insight-card--progress {
  background: linear-gradient(135deg, rgba(82, 133, 235, 0.1) 0%, rgba(255, 255, 255, 0.95) 100%);
}

.insight-label {
  font-size: 0.72rem;
  color: rgba(29, 45, 80, 0.55);
}

.insight-value {
  font-size: 0.95rem;
  font-weight: 600;
  color: #1d2d50;
}

.insight-sub {
  font-size: 0.7rem;
  color: rgba(29, 45, 80, 0.45);
}

.insight-progress :deep(.n-progress-graph-line) {
  height: 4px;
}
</style>
