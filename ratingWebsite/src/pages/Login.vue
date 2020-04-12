<template>
<q-page class="flex flex-center">
    <q-form
      @submit="onSubmit"
      @reset="onReset"
      class="q-gutter-md"
    >
      <q-input
        filled
        v-model="name"
        label="User"
        lazy-rules
        :rules="[ val => val && val.length > 0 || 'Please type your username']"
      />

      <q-input
        filled
        v-model="password"
        label="Password"
        lazy-rules
        :rules="[
          val => val !== null && val !== '' || 'Please type your password']"
      />

      <div>
        <q-btn label="Submit" type="submit" color="primary"/>
        <q-btn label="Reset" type="reset" color="primary" flat class="q-ml-sm" />
      </div>
    </q-form>
</q-page>
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
