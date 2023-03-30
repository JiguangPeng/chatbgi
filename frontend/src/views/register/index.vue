<template>
  <n-space justify="center">
      <n-form ref="formRef" :model="formValue" :rules="loginRules" style="min-width: 400px;">
        <n-form-item :label="$t('commons.username')" path="username">
            <n-input v-model:value="formValue.username" :placeholder="$t('tips.pleaseEnterUsername')" :input-props="{autoComplete: 'username'}" />
          </n-form-item>
        <n-form-item :label="$t('commons.email')" path="username">
        <n-input type="email" show-password-on="click" v-model:value="formValue.email" :placeholder="$t('tips.pleaseEnterEmail')"/>
      </n-form-item>
      <n-form-item :label="$t('commons.nickname')" path="nickname">
        <n-input v-model:value="formValue.nickname" :placeholder="$t('tips.pleaseEnterNickname')" />
      </n-form-item>
      <n-form-item :label="$t('commons.password')" path="password">
        <n-input type="password" show-password-on="click" v-model:value="formValue.password" :placeholder="$t('tips.pleaseEnterPassword')" />
      </n-form-item>
      <n-form-item :label="$t('commons.passwordAgain')" path="password">
        <n-input type="password" show-password-on="click" v-model:value="formValue.repassword" :placeholder="$t('tips.enterPasswordAgain')" @keyup.enter="register" />
      </n-form-item>
      <n-form-item :label="$t('commons.requestCode')" path="requestcode">
        <n-input type="requestCode" v-model:value="formValue.requestcode" :placeholder="$t('tips.enterRequestCode')" @keyup.enter="register" />
      </n-form-item>
      <n-space justify="center">
        <n-form-item wrapper-col="{ span: 16, offset: 8 }">
            <n-button type="primary" @click="register" :enabled="loading" style="min-width: 100px;">{{ $t("commons.register") }}</n-button>
          </n-form-item>
      </n-space>
    </n-form>
  </n-space>
</template>

<script setup lang="ts">
import { ref, reactive } from 'vue';
import { useI18n } from 'vue-i18n';
import { FormValidationError } from 'naive-ui/es/form';
import { FormInst } from 'naive-ui';
import { Message } from '@/utils/tips';
import { UserCreate } from '@/types/schema';
import { useRouter } from "vue-router";
import { useUserStore } from '@/store';
import { registerApi, LoginData, loginApi } from '@/api/user';


const { t } = useI18n();
const formRef = ref<FormInst>();
const router = useRouter();
const userStore = useUserStore();

const formValue = reactive({
  username: '',
  password: '',
  repassword:'',
  nickname:'',
  email:'',
  requestcode:''
});

const loginRules = {
  username: { required: true, message: t("tips.pleaseEnterPassword"), trigger: 'blur' },
  password: { required: true, message: t("tips.pleaseEnterPassword"), trigger: 'blur' },
  requestcode: { required: true, message: t("tips.pleaseEnterPassword"), trigger: 'blur' }
}
const loading = ref(false);

const filtering = (args:any) => {
  const reg = /@genomics.cn|@bgi.com/;
  return reg.test(args);
}

const register = async () => {
  if(formValue.username===""){
    Message.error(t('tips.pleaseEnterUsername'));
    formValue.username = "";
  }
  else if (formValue.email==="") {
    Message.error(t('tips.pleaseEnterEmail'));
    formValue.email = "";
  }
  else if(formValue.requestcode===""){
    Message.error(t('tips.enterRequestCode'));
    formValue.requestcode = "";
  }
  else if(formValue.password!==formValue.repassword){
    Message.error(t('tips.passwordException'))
    formValue.password = '';
    formValue.repassword = '';
  }
  else {
    if (filtering(formValue.email)==false) {
      Message.warning(t('tips.wrongEmail'));
      formValue.email = "";
    }else {
      const registerForm = ref<UserCreate>({
        username:'',
        nickname: '',
        email: '',
        password: '',
        requestcode: "",
        is_superuser: false
      });
      const useForm = ref<LoginData>({
        username: "",
        password: ""
      })
      registerForm.value.username = formValue.username;
      registerForm.value.email = formValue.email;
      if(formValue.nickname===""){
        registerForm.value.nickname = formValue.username;
      }
      registerForm.value.requestcode = formValue.requestcode;
      registerForm.value.password = formValue.password;
      useForm.value.username = formValue.username;
      useForm.value.password = formValue.password;
      let register = new Promise((resolve, reject) => {
        registerApi(registerForm.value).then(res => {
          Message.success(t("commons.createUserSuccess"));
          loginApi(useForm.value as LoginData).then(res => {
            router.push({ name: 'conversation' });
          });
          resolve(true);
        }).catch(err => { reject(err);});
      })
    }
  }

}


</script>
