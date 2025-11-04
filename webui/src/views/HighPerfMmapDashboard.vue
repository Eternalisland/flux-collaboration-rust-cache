<template>
  <div class="dashboard">
    <section class="dashboard-toolbar">
      <div class="toolbar-left">
        <n-space :size="12" align="center">
          <n-button type="primary" :loading="loading" @click="loadInstances" size="small">
            刷新数据
          </n-button>
<!--          <n-button quaternary @click="toggleMock">-->
<!--            {{ useFallback ? '关闭示例数据' : '使用示例数据' }}-->
<!--          </n-button>-->
          <n-input
            v-model:value="filterText"
            style="min-width: 380px"
            clearable
            size="small"
            placeholder="按名称或 ID 过滤实例"
          />
        </n-space>
      </div>
      <div class="toolbar-right" v-if="lastFetched">
        <n-tag type="info">
          最近刷新：{{ formatTimestamp(lastFetched) }}
        </n-tag>
      </div>
    </section>

    <n-alert v-if="error" type="error" closable @close="error = null">
      {{ error }}
    </n-alert>

    <n-empty v-if="!instances.length && !loading" description="暂无实例信息">
      <template #extra>
        <n-button @click="loadInstances">再次尝试</n-button>
      </template>
    </n-empty>

    <n-spin :show="loading">
      <n-grid v-if="filteredInstances.length" cols="1 m:2" :x-gap="18" :y-gap="18">
        <n-grid-item v-for="instance in filteredInstances" :key="instance.id">
          <n-card class="instance-card" size="large">
            <template #header>
              <div class="instance-card__header">
                <div class="instance-card__title">
                  <n-icon size="26" class="instance-card__title-icon">
                    <ServerOutline />
                  </n-icon>
                  <div>
                    <h2>{{ instance.displayName }}</h2>
                    <p>
                      L1 上限 {{ formatBytes(instance.config.l1CacheSizeLimit) }} ·
                      预读队列 {{ instance.config.prefetchQueueSize }} 条
                    </p>
                  </div>
                </div>
                <n-space align="center" :size="8">
                  <n-tag :type="instance.config.enableCompression ? 'success' : 'default'">
                    压缩：{{ instance.config.enableCompression ? '启用' : '关闭' }}
                  </n-tag>
                  <n-tag :type="instance.config.enablePrefetch ? 'success' : 'default'">
                    预读：{{ instance.config.enablePrefetch ? '启用' : '关闭' }}
                  </n-tag>
                  <n-tag type="info">
                    文件上限：{{ formatBytes(instance.config.maxFileSize) }}
                  </n-tag>
                  <n-button type="primary" size="small" @click="toggleExpand(instance.id)">

                    <template #icon v-if="isExpanded(instance.id)">
                      <NIcon>
                        <ArrowUpCircleOutline />
                      </NIcon>
                    </template>
                    <template #icon v-else>
                      <NIcon>
                        <ArrowDownCircleOutline />
                      </NIcon>
                    </template>
                    {{ isExpanded(instance.id) ? '收起详情' : '展开详情' }}
                  </n-button>
                </n-space>
              </div>
            </template>

            <n-space vertical :size="18">
              <SectionCard
                title="实例概览"
                description="关键吞吐、延迟与缓存表现"
                :icon="icons.summary"
                highlight
              >
                <InstanceSummary
                  :stats="instance.stats"
                  :memory="instance.memoryStats"
                />
              </SectionCard>

              <n-collapse-transition>
                <div v-if="isExpanded(instance.id)">
                  <n-grid cols="1 l:2" :x-gap="16" :y-gap="16" responsive="screen" class="instance-card__grid">
                    <n-grid-item class="instance-card__grid-item">
                      <SectionCard
                        title="容量与缓存配置"
                        description="文件扩容策略与分层缓存限制"
                        :icon="icons.config"
                      >
                        <ConfigSection :config="instance.config" />
                      </SectionCard>
                    </n-grid-item>
                    <n-grid-item class="instance-card__grid-item">
                      <SectionCard
                        title="压缩配置"
                        description="存储压缩策略与阈值"
                        :icon="icons.compression"
                      >
                        <CompressionSection :compression="instance.config.compression" />
                      </SectionCard>
                    </n-grid-item>
                    <n-grid-item class="instance-card__grid-item">
                      <SectionCard
                        title="内存占用与压力"
                        description="堆使用、MMAP 映射与驱逐情况"
                        :icon="icons.memory"
                      >
                        <MemorySection :memory="instance.memoryStats" />
                      </SectionCard>
                    </n-grid-item>
                    <n-grid-item class="instance-card__grid-item">
                      <SectionCard
                        title="读写统计详情"
                        description="命中率、扩容与错误指标"
                        :icon="icons.stats"
                      >
                        <StatsSection :stats="instance.stats" />
                      </SectionCard>
                    </n-grid-item>
                  </n-grid>
                </div>
              </n-collapse-transition>
            </n-space>
          </n-card>
        </n-grid-item>
      </n-grid>
      <n-empty
        v-else-if="!loading"
        description="当前筛选条件下没有匹配的实例"
      />
    </n-spin>
  </div>
</template>

<script setup lang="ts">
import {  onMounted, onBeforeUnmount, computed, reactive, ref, watch } from 'vue'
import {
  NAlert,
  NButton,
  NCard,
  NCollapseTransition,
  NGrid,
  NGridItem,
  NEmpty,
  NSpin,
  NSpace,
  NTag,
  NIcon,
  NInput
} from 'naive-ui';
import {
  SpeedometerOutline,
  SettingsOutline,
  ConstructOutline,
  HardwareChipOutline,
  AnalyticsOutline,
  ServerOutline,
  ArrowUpCircleOutline,
  ArrowDownCircleOutline
} from '@vicons/ionicons5';
import InstanceSummary from '../components/InstanceSummary.vue';
import ConfigSection from '../components/ConfigSection.vue';
import CompressionSection from '../components/CompressionSection.vue';
import MemorySection from '../components/MemorySection.vue';
import StatsSection from '../components/StatsSection.vue';
import SectionCard from '../components/SectionCard.vue';
import type {
  CompressionConfig,
  HighPerfMmapConfig,
  HighPerfMmapStats,
  InstanceViewModel,
  MemoryStats
} from '../types/highPerfMmap';
import {
  formatBytes,
  formatTimestamp,
  numberish
} from '../utils/formatters';

interface RawInstancePayload {
  id: string;
  name?: string;
  label?: string;
  config: Record<string, unknown>;
  status?: Record<string, unknown>;
  memoryStats?: Record<string, unknown>;
  memory_stats?: Record<string, unknown>;
  stats?: Record<string, unknown>;
  high_perf_mmap_stats?: Record<string, unknown>;
  updatedAt?: string;
  updated_at?: string;
}

const instances = ref<InstanceViewModel[]>([]);
const loading = ref(false);
const lastFetched = ref<Date | null>(null);
const error = ref<string | null>(null);
const useFallback = ref(false);
const filterText = ref('');
const expanded = reactive(new Set<string>());
let timerHandle: any = null

const fallbackData: InstanceViewModel[] = reactive([
  {
    id: 'mmap-a',
    displayName: 'Instance mmap-a',
    updatedAt: new Date().toISOString(),
    config: {
      initialFileSize: 100 * 1024 * 1024,
      growthStep: 50 * 1024 * 1024,
      growthReserveSteps: 3,
      maxFileSize: 10 * 1024 * 1024 * 1024,
      enableCompression: true,
      l1CacheSizeLimit: 50 * 1024 * 1024,
      l1CacheEntryLimit: 500,
      l2CacheSizeLimit: 200 * 1024 * 1024,
      l2CacheEntryLimit: 2000,
      enablePrefetch: true,
      prefetchQueueSize: 100,
      memoryPressureThreshold: 0.8,
      cacheDegradationThreshold: 0.9,
      autoPurgeAfterSecs: 8 * 3600,
      autoPurgeCheckIntervalSecs: 300,
      compression: {
        enabled: true,
        algorithm: 'Lz4',
        minCompressSize: 512,
        minCompressRatio: 0.9,
        asyncCompression: false,
        compressionLevel: 1,
        enableCompressibilityCheck: true
      }
    },
    memoryStats: {
      l1CacheBytes: 18_500_000,
      l1CacheEntries: 375,
      indexBytes: 6_500_000,
      indexEntries: 1800,
      orderedIndexBytes: 2_200_000,
      lazyDeletedBytes: 320_000,
      lazyDeletedEntries: 90,
      prefetchQueueBytes: 120_000,
      prefetchQueueSize: 60,
      mmapBytes: 180_000_000,
      totalHeapBytes: 27_640_000,
      memoryUsageRatio: 0.14,
      underMemoryPressure: false,
      cacheEvictions: 24,
      forcedEvictions: 3
    },
    stats: {
      totalWrites: 120_000,
      totalReads: 420_000,
      totalDeletes: 36_500,
      totalWriteBytes: 19_800_000_000,
      totalReadBytes: 68_200_000_000,
      l1CacheHits: 380_000,
      l1CacheMisses: 18_000,
      l2CacheHits: 12_500,
      l2CacheMisses: 5_500,
      prefetchHits: 4_400,
      fileExpansions: 8,
      avgWriteLatencyUs: 210,
      avgReadLatencyUs: 95,
      mmapRemaps: 3,
      errorCount: 1,
      lastErrorCode: null,
      lazyDeletedEntries: 90,
      reclaimedDiskSpace: 3_400_000,
      garbageCollectionRuns: 12,
      avgWritePathUs: 180,
      maintenanceExpandEvents: 2,
      expandUsTotal: 540_000,
      prefetchServedHits: 2_700
    }
  },
  {
    id: 'mmap-b',
    displayName: 'Instance mmap-b',
    updatedAt: new Date().toISOString(),
    config: {
      initialFileSize: 200 * 1024 * 1024,
      growthStep: 80 * 1024 * 1024,
      growthReserveSteps: 4,
      maxFileSize: 5 * 1024 * 1024 * 1024,
      enableCompression: false,
      l1CacheSizeLimit: 80 * 1024 * 1024,
      l1CacheEntryLimit: 800,
      l2CacheSizeLimit: 250 * 1024 * 1024,
      l2CacheEntryLimit: 2500,
      enablePrefetch: false,
      prefetchQueueSize: 0,
      memoryPressureThreshold: 0.7,
      cacheDegradationThreshold: 0.85,
      autoPurgeAfterSecs: null,
      autoPurgeCheckIntervalSecs: 0,
      compression: {
        enabled: false,
        algorithm: 'None',
        minCompressSize: 512,
        minCompressRatio: 0.9,
        asyncCompression: false,
        compressionLevel: 1,
        enableCompressibilityCheck: false
      }
    },
    memoryStats: {
      l1CacheBytes: 45_800_000,
      l1CacheEntries: 620,
      indexBytes: 9_100_000,
      indexEntries: 2600,
      orderedIndexBytes: 3_200_000,
      lazyDeletedBytes: 1_400_000,
      lazyDeletedEntries: 180,
      prefetchQueueBytes: 0,
      prefetchQueueSize: 0,
      mmapBytes: 220_000_000,
      totalHeapBytes: 59_500_000,
      memoryUsageRatio: 0.3,
      underMemoryPressure: false,
      cacheEvictions: 120,
      forcedEvictions: 14
    },
    stats: {
      totalWrites: 82_000,
      totalReads: 220_000,
      totalDeletes: 18_200,
      totalWriteBytes: 12_800_000_000,
      totalReadBytes: 40_500_000_000,
      l1CacheHits: 180_000,
      l1CacheMisses: 28_000,
      l2CacheHits: 9_400,
      l2CacheMisses: 6_000,
      prefetchHits: 0,
      fileExpansions: 5,
      avgWriteLatencyUs: 260,
      avgReadLatencyUs: 130,
      mmapRemaps: 4,
      errorCount: 0,
      lastErrorCode: null,
      lazyDeletedEntries: 180,
      reclaimedDiskSpace: 6_800_000,
      garbageCollectionRuns: 20,
      avgWritePathUs: 240,
      maintenanceExpandEvents: 3,
      expandUsTotal: 780_000,
      prefetchServedHits: 0
    }
  }
]);


onMounted(() => {
  loadInstances()

  timerHandle = setInterval(
      async () => {
        await loadInstances();
      },
      10000,
  ) // 使用类型断言
})
onBeforeUnmount(() => {
  if (timerHandle) clearInterval(timerHandle)
})

import { fetchDatahubRustJniStatus } from '../api/api'
async function loadInstances() {
  // if (useFallback.value) {
  //   error.value = null;
  //   instances.value = [...fallbackData];
  //   lastFetched.value = new Date();
  //   expanded.clear();
  //   return;
  // }

  loading.value = true;
  error.value = null;
  try {
    const snapshot = await fetchDatahubRustJniStatus();
    const data = ( snapshot ) as RawInstancePayload[];
    instances.value = data.map(normalizeInstance);
    lastFetched.value = new Date();
    // expanded.clear();
  } catch (err) {
    console.error(err);
    error.value =
      err instanceof Error ? err.message : '拉取实例信息失败，已切换至示例数据。';
    // instances.value = [...fallbackData];
    // useFallback.value = true;
    lastFetched.value = new Date();
  } finally {
    loading.value = false;
  }
}

function toggleMock() {
  useFallback.value = !useFallback.value;
  loadInstances();
}

watch(useFallback, (next) => {
  if (next) error.value = null;
});

const filteredInstances = computed(() => {
  const keyword = filterText.value.trim().toLowerCase();
  if (!keyword) {
    return instances.value;
  }
  return instances.value.filter((instance) =>
    instance.displayName.toLowerCase().includes(keyword) ||
    instance.id.toLowerCase().includes(keyword)
  );
});

function toggleExpand(id: string) {
  if (expanded.has(id)) {
    expanded.delete(id);
  } else {
    expanded.add(id);
  }
}

function isExpanded(id: string) {
  return expanded.has(id);
}

function normalizeInstance(raw: RawInstancePayload): InstanceViewModel {
  const config = normalizeConfig(raw.config ?? {});
  const statusPayload = raw.status ?? (raw as Record<string, unknown>)['status'];
  const statusRecord = (statusPayload ?? {}) as Record<string, unknown>;
  const memoryPayload =
    raw.memoryStats ??
    raw.memory_stats ??
    (statusRecord.memory as Record<string, unknown> | undefined) ??
    (statusRecord['memory'] as Record<string, unknown> | undefined) ??
    {};
  const statsPayload =
    raw.stats ??
    raw.high_perf_mmap_stats ??
    (statusRecord.stats as Record<string, unknown> | undefined) ??
    (statusRecord['stats'] as Record<string, unknown> | undefined) ??
    {};
  return {
    id: raw.id,
    displayName: raw.label ?? raw.name ?? raw.id,
    config,
    memoryStats: normalizeMemory(memoryPayload),
    stats: normalizeStats(statsPayload),
    updatedAt: raw.updatedAt ?? raw.updated_at
  };
}

function normalizeConfig(payload: Record<string, unknown>): HighPerfMmapConfig {
  const compressionPayload =
    (payload.compression ?? payload['compression_config']) as
      | Record<string, unknown>
      | undefined;
  return {
    initialFileSize: numberish(payload.initialFileSize ?? payload['initial_file_size']),
    growthStep: numberish(payload.growthStep ?? payload['growth_step']),
    growthReserveSteps: numberish(
      payload.growthReserveSteps ?? payload['growth_reserve_steps']
    ),
    maxFileSize: numberish(payload.maxFileSize ?? payload['max_file_size']),
    enableCompression: Boolean(payload.enableCompression ?? payload['enable_compression']),
    l1CacheSizeLimit: numberish(
      payload.l1CacheSizeLimit ?? payload['l1_cache_size_limit']
    ),
    l1CacheEntryLimit: numberish(
      payload.l1CacheEntryLimit ?? payload['l1_cache_entry_limit']
    ),
    l2CacheSizeLimit: numberish(
      payload.l2CacheSizeLimit ?? payload['l2_cache_size_limit']
    ),
    l2CacheEntryLimit: numberish(
      payload.l2CacheEntryLimit ?? payload['l2_cache_entry_limit']
    ),
    enablePrefetch: Boolean(payload.enablePrefetch ?? payload['enable_prefetch']),
    prefetchQueueSize: numberish(
      payload.prefetchQueueSize ?? payload['prefetch_queue_size']
    ),
    memoryPressureThreshold: numberish(
      payload.memoryPressureThreshold ?? payload['memory_pressure_threshold']
    ),
    cacheDegradationThreshold: numberish(
      payload.cacheDegradationThreshold ?? payload['cache_degradation_threshold']
    ),
    compression: normalizeCompression(compressionPayload),
    autoPurgeAfterSecs: (() => {
      const raw =
        payload.autoPurgeAfterSecs ?? payload['auto_purge_after_secs'] ?? null;
      return raw == null ? null : numberish(raw);
    })(),
    autoPurgeCheckIntervalSecs: numberish(
      payload.autoPurgeCheckIntervalSecs ?? payload['auto_purge_check_interval_secs'] ?? 0
    )
  };
}

function normalizeCompression(
  payload?: Record<string, unknown>
): CompressionConfig {
  return {
    enabled: Boolean(payload?.enabled ?? payload?.['enabled']),
    algorithm: (payload?.algorithm ?? 'None') as CompressionConfig['algorithm'],
    minCompressSize: numberish(
      payload?.minCompressSize ?? payload?.['min_compress_size']
    ),
    minCompressRatio: numberish(
      payload?.minCompressRatio ?? payload?.['min_compress_ratio']
    ),
    asyncCompression: Boolean(
      payload?.asyncCompression ?? payload?.['async_compression']
    ),
    compressionLevel: numberish(
      payload?.compressionLevel ?? payload?.['compression_level']
    ),
    enableCompressibilityCheck: Boolean(
      payload?.enableCompressibilityCheck ??
        payload?.['enable_compressibility_check']
    )
  };
}

function normalizeMemory(payload: Record<string, unknown>): MemoryStats {
  return {
    l1CacheBytes: numberish(payload.l1CacheBytes ?? payload['l1_cache_bytes']),
    l1CacheEntries: numberish(payload.l1CacheEntries ?? payload['l1_cache_entries']),
    indexBytes: numberish(payload.indexBytes ?? payload['index_bytes']),
    indexEntries: numberish(payload.indexEntries ?? payload['index_entries']),
    orderedIndexBytes: numberish(
      payload.orderedIndexBytes ?? payload['ordered_index_bytes']
    ),
    lazyDeletedBytes: numberish(
      payload.lazyDeletedBytes ?? payload['lazy_deleted_bytes']
    ),
    lazyDeletedEntries: numberish(
      payload.lazyDeletedEntries ?? payload['lazy_deleted_entries']
    ),
    prefetchQueueBytes: numberish(
      payload.prefetchQueueBytes ?? payload['prefetch_queue_bytes']
    ),
    prefetchQueueSize: numberish(
      payload.prefetchQueueSize ?? payload['prefetch_queue_size']
    ),
    mmapBytes: numberish(payload.mmapBytes ?? payload['mmap_bytes']),
    totalHeapBytes: numberish(payload.totalHeapBytes ?? payload['total_heap_bytes']),
    memoryUsageRatio: numberish(
      payload.memoryUsageRatio ?? payload['memory_usage_ratio']
    ),
    underMemoryPressure: Boolean(
      payload.underMemoryPressure ?? payload['under_memory_pressure']
    ),
    cacheEvictions: numberish(payload.cacheEvictions ?? payload['cache_evictions']),
    forcedEvictions: numberish(payload.forcedEvictions ?? payload['forced_evictions'])
  };
}

function normalizeStats(payload: Record<string, unknown>): HighPerfMmapStats {
  return {
    totalWrites: numberish(payload.totalWrites ?? payload['total_writes']),
    totalReads: numberish(payload.totalReads ?? payload['total_reads']),
    totalRecords: numberish(payload.totalRecords ?? payload['total_records']),
    totalDeletes: numberish(payload.totalDeletes ?? payload['total_deletes']),
    totalWriteBytes: numberish(
      payload.totalWriteBytes ?? payload['total_write_bytes']
    ),
    totalReadBytes: numberish(
      payload.totalReadBytes ?? payload['total_read_bytes']
    ),
    l1CacheHits: numberish(payload.l1CacheHits ?? payload['l1_cache_hits']),
    l1CacheMisses: numberish(payload.l1CacheMisses ?? payload['l1_cache_misses']),
    l2CacheHits: numberish(payload.l2CacheHits ?? payload['l2_cache_hits']),
    l2CacheMisses: numberish(payload.l2CacheMisses ?? payload['l2_cache_misses']),
    prefetchHits: numberish(payload.prefetchHits ?? payload['prefetch_hits']),
    fileExpansions: numberish(payload.fileExpansions ?? payload['file_expansions']),
    avgWriteLatencyUs: numberish(
      payload.avgWriteLatencyUs ?? payload['avg_write_latency_us']
    ),
    avgReadLatencyUs: numberish(
      payload.avgReadLatencyUs ?? payload['avg_read_latency_us']
    ),
    mmapRemaps: numberish(payload.mmapRemaps ?? payload['mmap_remaps']),
    errorCount: numberish(payload.errorCount ?? payload['error_count']),
    lastErrorCode: (payload.lastErrorCode ?? payload['last_error_code'] ?? null) as
      | string
      | null,
    lazyDeletedEntries: numberish(
      payload.lazyDeletedEntries ?? payload['lazy_deleted_entries']
    ),
    reclaimedDiskSpace: numberish(
      payload.reclaimedDiskSpace ?? payload['reclaimed_disk_space']
    ),
    garbageCollectionRuns: numberish(
      payload.garbageCollectionRuns ?? payload['garbage_collection_runs']
    ),
    avgWritePathUs: numberish(
      payload.avgWritePathUs ?? payload['avg_write_path_us']
    ),
    maintenanceExpandEvents: numberish(
      payload.maintenanceExpandEvents ?? payload['maintenance_expand_events']
    ),
    expandUsTotal: numberish(payload.expandUsTotal ?? payload['expand_us_total']),
    prefetchServedHits: numberish(
      payload.prefetchServedHits ?? payload['prefetch_served_hits']
    )
  };
}

const icons = {
  summary: SpeedometerOutline,
  config: SettingsOutline,
  compression: ConstructOutline,
  memory: HardwareChipOutline,
  stats: AnalyticsOutline
};
</script>

<style scoped>
.dashboard {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.dashboard-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}

.toolbar-left,
.toolbar-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.instance-card {
  background: #ffffff;
  border: 1px solid rgba(46, 74, 117, 0.08);
  border-radius: 18px;
  box-shadow: 0 18px 42px rgba(31, 73, 135, 0.12);
}

.instance-card__header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}

.instance-card__title {
  display: flex;
  align-items: center;
  gap: 16px;
}

.instance-card__title-icon {
  color: #4a80e0;
}

.instance-card__title h2 {
  margin: 0;
  font-size: 1.4rem;
  font-weight: 600;
  color: #1d2d50;
}

.instance-card__title p {
  margin: 2px 0 0;
  font-size: 0.85rem;
  color: rgba(29, 45, 80, 0.55);
}

.instance-card__grid {
  width: 100%;
}

.instance-card__grid-item {
  display: flex;
}

.instance-card__grid-item :deep(.section-card) {
  width: 100%;
}
</style>
