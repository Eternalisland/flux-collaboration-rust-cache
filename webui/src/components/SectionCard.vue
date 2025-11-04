<template>
  <n-card size="large" :embedded="embedded" :class="cardClass">
    <template #header>
      <div class="section-card__header">
        <n-icon v-if="icon" :component="icon" size="22" class="section-card__icon" />
        <div class="section-card__heading">
          <span class="section-card__title">{{ title }}</span>
          <span v-if="description" class="section-card__subtitle">{{ description }}</span>
        </div>
      </div>
    </template>
    <slot />
  </n-card>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { NCard, NIcon } from 'naive-ui';
import type { Component } from 'vue';

const props = defineProps<{
  title: string;
  description?: string;
  icon?: Component;
  embedded?: boolean;
  highlight?: boolean;
}>();

const cardClass = computed(() => [
  'section-card',
  props.highlight ? 'section-card--highlight' : null
]);
</script>

<style scoped>
.section-card {
  border-radius: 16px;
  background: #fff;
  border: 1px solid rgba(46, 74, 117, 0.08);
  box-shadow: 0 10px 32px rgba(15, 35, 71, 0.12);
}

.section-card--highlight {
  background: linear-gradient(135deg, #f2f6ff 0%, #ffffff 100%);
  border-color: rgba(63, 123, 216, 0.22);
  box-shadow: 0 14px 40px rgba(74, 128, 224, 0.18);
}

.section-card__header {
  display: flex;
  align-items: center;
  gap: 12px;
  padding-bottom: 12px;
  border-bottom: 1px solid rgba(46, 74, 117, 0.12);
}

.section-card__icon {
  color: #4a80e0;
}

.section-card__heading {
  display: flex;
  flex-direction: column;
}

.section-card__title {
  font-size: 1.1rem;
  font-weight: 600;
}

.section-card__subtitle {
  font-size: 0.85rem;
  color: rgba(60, 72, 88, 0.65);
}

.section-card :deep(.n-card__content) {
  padding-top: 18px;
  border-top: 1px solid rgba(46, 74, 117, 0.08);
}

.section-card--highlight :deep(.n-card__content) {
  border-top: 1px solid rgba(74, 128, 224, 0.18);
}
</style>
