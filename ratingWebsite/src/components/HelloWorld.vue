<template>
    <!-- <v-layout text-xs-center wrap>
      <v-flex> -->
    <div>
        <q-icon name="accessibility" />
        <form @submit="formSubmit">
          <input
            v-model="sepalLength"
            label="Sepal Length"
            required>

          <input
            type="text"
            v-model="sepalWidth"
            label="Sepal Width"
            required>

          <input
            type="text"
            v-model="petalLength"
            label="Petal Length"
            required>

          <input
            type="text"
            v-model="petalWidth"
            label="Petal Width"
            required>

          <button v-on:click="formSubmit" type='button'>Submit</button>
          <button @click="clear">clear</button>

          <p> result: {{info}}</p>
        </form>
    </div>

      <!-- </v-flex>
    </v-layout> -->
</template>

<script>
import axios from 'axios'

export default {
  name: 'HelloWorld',
  data: () => ({
    sepalLength: '',
    sepalWidth: '',
    petalLength: '',
    petalWidth: '',
    info: ''
  }),

  // mounted(){
  //   axios.get('http://127.0.0.1:5000/predict')
  //   .then(response =>{
  //     alert("inresponse:" + response);
  //     //this.info = response.data

  //   })
  // },

  methods: {
    mytest: function (obj) {
      alert(obj.data)
      // console.log(obj.data)
    },
    formSubmit: function () {
      const params = {
        sepal_length: this.sepalLength,
        sepal_width: this.sepalWidth,
        petal_length: this.petalLength,
        petal_width: this.petalWidth
      }
      axios.post('http://127.0.0.1:5000/predict', params)
        .then(response => {
          this.info = response.data
          console.log(this.info)
        })
        .catch(error => {
          alert('error:' + error.response.data)
        })
    },

    clear: function () {
      this.sepalLength = ''
      this.sepalWidth = ''
      this.petalLength = ''
      this.petalWidth = ''
      this.info = ''
    }
  }

}
</script>

<style>
</style>
