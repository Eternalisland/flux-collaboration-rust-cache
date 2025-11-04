<template>
  <div class="stats-section">
    <div class="hit-rate-row">
      <div class="hit-rate-card">
        <n-progress
          type="circle"
          :percentage="Math.min(Number((l1HitRate * 100).toFixed(1)), 100)"
          :stroke-width="10"
          :height="120"
          color="#4a80e0"
        />
        <div class="hit-rate-card__label">L1 命中率</div>
<!--        <div class="hit-rate-card__value">{{ (l1HitRate * 100).toFixed(1) }}%</div>-->
      </div>
      <div class="hit-rate-card">
        <n-progress
          type="circle"
          :percentage="Math.min(Number((l2HitRate * 100).toFixed(1)), 100)"
          :stroke-width="10"
          :height="120"
          color="#4a80e0"
        />
        <div class="hit-rate-card__label">L2 命中率</div>
<!--        <div class="hit-rate-card__value">{{ (l2HitRate * 100).toFixed(1) }}%</div>-->
      </div>
      <div class="summary-chip">
        <div class="summary-chip__label">文件扩容</div>
        <div class="summary-chip__value">
          {{ stats.fileExpansions.toLocaleString() }}
        </div>
      </div>
      <div class="summary-chip">
        <div class="summary-chip__label">GC 次数</div>
        <div class="summary-chip__value">
          {{ stats.garbageCollectionRuns.toLocaleString() }}
        </div>
      </div>
      <div class="summary-chip">
        <div class="summary-chip__label">删除次数</div>
        <div class="summary-chip__value">
          {{ stats.totalDeletes.toLocaleString() }}
        </div>
      </div>
    </div>

    <n-form label-placement="left" label-width="130" size="small">
      <n-grid :cols="24" :x-gap="24" :y-gap="8">
        <n-form-item-gi :span="12" label="读路径平均延迟">
          <span class="form-value">{{ stats.avgReadLatencyUs.toLocaleString() }} μs</span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="写路径平均延迟">
          <span class="form-value">{{ stats.avgWriteLatencyUs.toLocaleString() }} μs</span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="写路径 EMA">
          <span class="form-value">{{ stats.avgWritePathUs.toLocaleString() }} μs</span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="维护扩容耗时">
          <span class="form-value">{{ stats.expandUsTotal.toLocaleString() }} μs</span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="惰性删除条目">
          <span class="form-value">{{ stats.lazyDeletedEntries }}</span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="释放磁盘空间">
          <span class="form-value">{{ formatBytes(stats.reclaimedDiskSpace) }}</span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="预读命中">
          <span class="form-value">
            {{ stats.prefetchHits }} / {{ stats.prefetchServedHits }}
          </span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="MMAP 重映射">
          <span class="form-value">{{ stats.mmapRemaps }}</span>
        </n-form-item-gi>
        <n-form-item-gi :span="12" label="错误次数">
          <span class="form-value">
            {{ stats.errorCount }}
            <template v-if="stats.lastErrorCode">
              （最后一次：{{ stats.lastErrorCode }}）
            </template>
          </span>
        </n-form-item-gi>
      </n-grid>
    </n-form>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { NForm, NFormItemGi, NGrid, NProgress } from 'naive-ui';
import type { HighPerfMmapStats } from '../types/highPerfMmap';
import { formatBytes } from '../utils/formatters';

const props = defineProps<{
  stats: HighPerfMmapStats;
}>();

const l1HitRate = computed(() => {
  const denom = props.stats.l1CacheHits + props.stats.l1CacheMisses;
  if (!denom) return 0;
  return props.stats.l1CacheHits / denom;
});

const l2HitRate = computed(() => {
  const denom = props.stats.l2CacheHits + props.stats.l2CacheMisses;
  if (!denom) return 0;
  return props.stats.l2CacheHits / denom;
});
</script>

<style scoped>
.stats-section {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.hit-rate-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 16px;
  align-items: stretch;
}

.hit-rate-card {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 8px;
  padding: 16px;
  background: linear-gradient(135deg, #f0f4ff 0%, #ffffff 100%);
  border: 1px solid rgba(74, 128, 224, 0.16);
  border-radius: 14px;
  box-shadow: 0 6px 24px rgba(58, 98, 176, 0.12);
}

.hit-rate-card__label {
  font-size: 0.95rem;
  color: rgba(29, 45, 80, 0.65);
}

.hit-rate-card__value {
  font-size: 1.25rem;
  font-weight: 600;
  color: #1d2d50;
}

.summary-chip {
  display: flex;
  flex-direction: column;
  justify-content: center;
  padding: 16px;
  background: #ffffff;
  border: 1px solid rgba(46, 74, 117, 0.08);
  border-radius: 14px;
  box-shadow: 0 6px 20px rgba(31, 73, 135, 0.1);
}

.summary-chip__label {
  font-size: 0.85rem;
  color: rgba(29, 45, 80, 0.6);
}

.summary-chip__value {
  margin-top: 4px;
  font-size: 1.2rem;
  font-weight: 600;
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
