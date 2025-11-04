export type CompressionAlgorithm = 'None' | 'Lz4' | 'Zstd' | 'Snappy';

export interface CompressionConfig {
  enabled: boolean;
  algorithm: CompressionAlgorithm;
  minCompressSize: number;
  minCompressRatio: number;
  asyncCompression: boolean;
  compressionLevel: number;
  enableCompressibilityCheck: boolean;
}

export interface HighPerfMmapConfig {
  initialFileSize: number;
  growthStep: number;
  growthReserveSteps: number;
  maxFileSize: number;
  enableCompression: boolean;
  l1CacheSizeLimit: number;
  l1CacheEntryLimit: number;
  l2CacheSizeLimit: number;
  l2CacheEntryLimit: number;
  enablePrefetch: boolean;
  prefetchQueueSize: number;
  memoryPressureThreshold: number;
  cacheDegradationThreshold: number;
  compression: CompressionConfig;
}

export interface MemoryStats {
  l1CacheBytes: number;
  l1CacheEntries: number;
  indexBytes: number;
  indexEntries: number;
  orderedIndexBytes: number;
  lazyDeletedBytes: number;
  lazyDeletedEntries: number;
  prefetchQueueBytes: number;
  prefetchQueueSize: number;
  mmapBytes: number;
  totalHeapBytes: number;
  memoryUsageRatio: number;
  underMemoryPressure: boolean;
  cacheEvictions: number;
  forcedEvictions: number;
}

export interface HighPerfMmapStats {
  totalWrites: number;
  totalReads: number;
  totalDeletes: number;
  totalWriteBytes: number;
  totalReadBytes: number;
  l1CacheHits: number;
  l1CacheMisses: number;
  l2CacheHits: number;
  l2CacheMisses: number;
  prefetchHits: number;
  fileExpansions: number;
  avgWriteLatencyUs: number;
  avgReadLatencyUs: number;
  mmapRemaps: number;
  errorCount: number;
  lastErrorCode: string | null;
  lazyDeletedEntries: number;
  reclaimedDiskSpace: number;
  garbageCollectionRuns: number;
  avgWritePathUs: number;
  maintenanceExpandEvents: number;
  expandUsTotal: number;
  prefetchServedHits: number;
}

export interface InstanceViewModel {
  id: string;
  displayName: string;
  config: HighPerfMmapConfig;
  memoryStats: MemoryStats;
  stats: HighPerfMmapStats;
  updatedAt?: string;
}
