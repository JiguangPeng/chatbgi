<template>
  <div class="flex justify-center items-center mt-20">
    <n-form ref="formRef" :model="formValue" :rules="loginRules" :label-col="{ span: 8 }" :wrapper-col="{ span: 16 }">
      <n-form-item :label="$t('commons.username')" path="username">
        <n-input v-model:value="formValue.username" :placeholder="$t('tips.pleaseEnterUsername')" :input-props="{
          autoComplete: 'username'
        }" />
      </n-form-item>
      <n-form-item :label="$t('commons.password')" path="password">
        <n-input type="password" show-password-on="click" v-model:value="formValue.password" :placeholder="$t('tips.pleaseEnterPassword')" :input-props="{
          autoComplete: 'current-password'
        }" @keyup.enter="login" />
      </n-form-item>
      <n-form-item wrapper-col="{ span: 16, offset: 8 }">
        <n-button type="primary" @click="login" :enabled="loading">{{ $t("commons.login") }}</n-button>
      </n-form-item>
    </n-form>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive } from 'vue';
import { useI18n } from 'vue-i18n';
import { FormValidationError } from 'naive-ui/es/form';
import { FormInst } from 'naive-ui'


const { t } = useI18n();
const formRef = ref<FormInst>();

const formValue = reactive({
  username: '',
  password: ''
});

const loginRules = {
  username: { required: true, message: t("tips.pleaseEnterUsername"), trigger: 'blur' },
  password: { required: true, message: t("tips.pleaseEnterPassword"), trigger: 'blur' }
}
const loading = ref(false);

const login = async () => {
  if (loading.value) return;
}


</script>
