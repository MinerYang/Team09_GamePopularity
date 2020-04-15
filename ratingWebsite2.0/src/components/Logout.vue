<template>
  <div>
      <q-btn @click="quit()" label="Logout" type="submit" icon="exit_to_app" flat round dense/>
  </div>
</template>

<script>
export default {
  name: 'Logout',
  methods: {
    quit () {
      this.$router.replace('/')
    }
  }
}
</script>

<style>
/* .div{
  color:rgb(255, 255, 255)
} */
</style>
