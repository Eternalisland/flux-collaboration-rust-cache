<template>
  <n-form label-placement="left" label-width="120" size="small">
    <n-grid :cols="24" :x-gap="24" :y-gap="8">
      <n-form-item-gi :span="12" label="是否开启">
        <n-tag size="small" :type="compression.enabled ? 'success' : 'default'">
          {{ compression.enabled ? '启用' : '关闭' }}
        </n-tag>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="压缩算法">
        <span class="form-value">{{ compression.algorithm }}</span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="最小压缩体积">
        <span class="form-value">{{ formatBytes(compression.minCompressSize) }}</span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="最小压缩比">
        <n-progress
          type="line"
          :percentage="Math.min(compression.minCompressRatio * 100, 100)"
          :indicator-placement="'outside'"
          :height="8"
        />
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="异步压缩">
        <n-tag size="small" :type="compression.asyncCompression ? 'success' : 'default'">
          {{ compression.asyncCompression ? '启用' : '关闭' }}
        </n-tag>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="压缩级别">
        <span class="form-value">{{ compression.compressionLevel }}</span>
      </n-form-item-gi>
      <n-form-item-gi :span="12" label="可压缩性检测">
        <n-tag
          size="small"
          :type="compression.enableCompressibilityCheck ? 'success' : 'default'"
        >
          {{ compression.enableCompressibilityCheck ? '启用' : '关闭' }}
        </n-tag>
      </n-form-item-gi>
    </n-grid>
  </n-form>
</template>

<script setup lang="ts">
import { NForm, NFormItemGi, NGrid, NProgress, NTag } from 'naive-ui';
import type { CompressionConfig } from '../types/highPerfMmap';
import { formatBytes } from '../utils/formatters';

defineProps<{
  compression: CompressionConfig;
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
</style>
