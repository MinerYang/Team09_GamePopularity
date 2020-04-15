<template>
<q-page-container>
<q-page class="q-pa-xl q-gutter-xl justify-evenly">
  <q-card class="row items-center">
     <q-card-section class="col-6 col-md-4">
      <q-form
      @submit="onSubmit"
      @reset="onReset"
      class="q-gutter-md"
      style = "">
      <q-input
        filled
        v-model="name"
        label="User"
        lazy-rules
        :rules="[ val => val && val.length > 0 || 'Please type your username']">
        <template v-slot:before>
          <q-icon name="account_circle" class="text-orange"/>
        </template>
      </q-input>

      <q-input
        filled
        v-model="password"
        label="Password"
        lazy-rules
        :rules="[
          val => val !== null && val !== '' || 'Please type your password']">
        <template v-slot:before>
          <q-icon name="vpn_key" class="text-orange"/>
        </template>
      </q-input>

      <div style="text-align:center;">
        <q-btn label="login" type="submit" color="orange" class="q-ml-sm"/>
        <q-btn label="Reset" type="reset" color="orange" flat class="q-ml-sm" />
      </div>
      </q-form>
     </q-card-section>
     <q-card-section class="col col-md-8" style="text-align:center;">
      <img alt="Quasar logo" src="~assets/welcome2.png" style="width:70%; object-fit: contain">
     </q-card-section>
  </q-card>
</q-page>
</q-page-container>
</template>

<script>
import Vue from 'vue'
import VueRouter from 'vue-router'
Vue.use(VueRouter)

export default {
  name: 'Login',
  data () {
    return {
      name: null,
      password: '',
      isPwd: true,
      usertype: null
    }
  },

  methods: {
    verifyUser () {
      if (this.name === 'miner') {
        if (this.password === '123') {
          this.loginSuccess('admin')
          // redirect to client page
          this.$router.replace('/admin')
        } else {
          this.loginError()
        }
      } else if (this.name === 'yang') {
        if (this.password === '123') {
          this.loginSuccess('client')
          // redirect to client page
          this.$router.replace('/client')
        } else {
          this.loginError()
        }
      } else {
        alert('please input your user name and password correctly')
      }
    },
    loginSuccess (actor) {
      alert(`You are login as a ${actor}`)
    },
    loginError () {
      alert('password not correct')
    },

    onSubmit () {
      this.verifyUser()
    },

    onReset () {
      this.name = null
      this.age = null
      this.accept = false
    }
  }

}
</script>
