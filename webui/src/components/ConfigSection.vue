<template>
  <n-form label-placement="left" label-width="118" size="small">
    <n-grid :cols="24" :x-gap="24">
      <n-form-item-gi :span="12" label="初始文件大小">
        <span class="form-value">{{ formatBytes(config.initialFileSize) }}</span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="扩容步长">
        <span class="form-value">{{ formatBytes(config.growthStep) }}</span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="扩容预留步数">
        <span class="form-value">{{ config.growthReserveSteps }}</span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="最大文件大小">
        <span class="form-value">{{ formatBytes(config.maxFileSize) }}</span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="L1 缓存上限">
        <span class="form-value">
          {{ formatBytes(config.l1CacheSizeLimit) }} / {{ config.l1CacheEntryLimit }} 条
        </span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="L2 缓存上限">
        <span class="form-value">
          {{ formatBytes(config.l2CacheSizeLimit) }} / {{ config.l2CacheEntryLimit }} 条
        </span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="预读队列大小">
        <span class="form-value">{{ config.prefetchQueueSize }}</span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="内存压力阈值">
        <span class="form-value">{{ formatPercent(config.memoryPressureThreshold, 0) }}</span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="降级阈值">
        <span class="form-value">{{ formatPercent(config.cacheDegradationThreshold, 0) }}</span>
      </n-form-item-gi>
    </n-grid>
  </n-form>
</template>

<script setup lang="ts">
import { NForm, NFormItemGi, NGrid } from 'naive-ui';
import type { HighPerfMmapConfig } from '../types/highPerfMmap';
import { formatBytes, formatPercent } from '../utils/formatters';

defineProps<{
  config: HighPerfMmapConfig;
}>();
</script>

<style scoped>
.form-value {
  display: inline-flex;
  min-height: 24px;
  align-items: center;
  padding: 2px 8px;
  border-radius: 6px;
  background: rgba(74, 128, 224, 0.08);
  color: #1d2d50;
  font-size: 0.8rem;
  font-weight: 500;
}

:deep(.n-form-item) {
  margin-bottom: 6px;
}

:deep(.n-form-item-label) {
  font-size: 0.78rem;
  padding-right: 8px;
  color: rgba(29, 45, 80, 0.65);
}

:deep(.n-form-item-feedback) {
  font-size: 0.75rem;
}
</style>
