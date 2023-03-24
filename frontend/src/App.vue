<template>
  <n-config-provider :theme="theme">
  <n-config-provider :theme-overrides="themeOverrides">
    <n-global-style />
    <div class="w-full lg:w-screen-lg mx-auto h-screen flex flex-col">
      <div class="my-4">
        <PageHeader class="mx-2 lg:mx-0" />
      </div>
      <router-view></router-view>
    </div>
  </n-config-provider>
  </n-config-provider>

</template>

<script setup lang="ts">
import PageHeader from './components/PageHeader.vue';
import { darkTheme, GlobalThemeOverrides } from 'naive-ui'
import { useAppStore } from './store';
import { computed } from 'vue';

const appStore = useAppStore();

const themeOverrides: GlobalThemeOverrides = {
    common: {
    primaryColor: '#005bab',
    primaryColorHover: '#0083f7',
    primaryColorPressed: '#0067c3',
    primaryColorSuppl: '#005bab'
    }
  }

const theme = computed(() => {
  if (appStore.theme == 'dark') {
    return darkTheme
  } else {
    return {}
  }
})
</script>
