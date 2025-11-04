<template>
  <div class="memory-section">
    <div class="memory-overview">
      <div>
        <div class="summary-label">堆占用</div>
        <div class="summary-value">{{ formatBytes(memory.totalHeapBytes) }}</div>
      </div>
      <div>
        <div class="summary-label">MMAP 文件大小</div>
        <div class="summary-value">{{ formatBytes(memory.mmapBytes) }}</div>
      </div>
      <div>
        <div class="summary-label">内存压力</div>
        <div class="summary-value">
          <n-tag size="small" :type="memory.underMemoryPressure ? 'error' : 'success'">
            {{ memory.underMemoryPressure ? '压力中' : '正常' }}
          </n-tag>
        </div>
      </div>
    </div>
    <n-progress
      class="memory-progress"
      type="line"
      :status="memory.underMemoryPressure ? 'error' : 'success'"
      :percentage="Math.min(memory.memoryUsageRatio * 100, 100)"
    />
    <n-form label-placement="left" label-width="120" size="small">
      <n-grid :cols="24" :x-gap="24" :y-gap="8">
        <n-form-item-gi :span="12" label="L1 缓存占用">
          <span class="form-value">
            {{ formatBytes(memory.l1CacheBytes) }} / {{ memory.l1CacheEntries }} 条
          </span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="索引占用">
          <span class="form-value">
            {{ formatBytes(memory.indexBytes) }} / {{ memory.indexEntries }} 条
          </span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="有序索引占用">
          <span class="form-value">{{ formatBytes(memory.orderedIndexBytes) }}</span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="惰性删除占用">
          <span class="form-value">
            {{ formatBytes(memory.lazyDeletedBytes) }} / {{ memory.lazyDeletedEntries }} 条
          </span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="预读队列">
          <span class="form-value">
            {{ formatBytes(memory.prefetchQueueBytes) }} / {{ memory.prefetchQueueSize }} 条
          </span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="缓存驱逐">
          <span class="form-value">
            {{ memory.cacheEvictions }}（强制 {{ memory.forcedEvictions }}）
          </span>
        </n-form-item-gi>
      </n-grid>
    </n-form>
  </div>
</template>

<script setup lang="ts">
import { NForm, NFormItemGi, NGrid, NProgress, NTag } from 'naive-ui';
import type { MemoryStats } from '../types/highPerfMmap';
import { formatBytes } from '../utils/formatters';

defineProps<{
  memory: MemoryStats;
}>();
</script>

<style scoped>
.memory-section {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.memory-overview {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 16px;
}

.memory-progress {
  max-width: 420px;
}

.summary-label {
  font-size: 0.85rem;
  color: rgba(29, 45, 80, 0.65);
}

.summary-value {
  font-weight: 600;
  font-size: 1.1rem;
  margin-top: 4px;
  color: #1d2d50;
}

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
</style>
