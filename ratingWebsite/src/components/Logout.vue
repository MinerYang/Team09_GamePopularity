<template>
  <div>
      <q-btn @click="quit()" label="Logout" type="submit" color="primary"/>
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

</style>
