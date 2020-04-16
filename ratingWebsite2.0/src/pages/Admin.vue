<template>
<q-page-container>
  <div class="q-pa-md">
    <div class="bg-purple text-white">
      <q-toolbar>
        <q-btn flat round dense icon="assignment_ind" style="font-size: 1.8em;" />
        <q-toolbar-title>Manager</q-toolbar-title>
        <logout/>
      </q-toolbar>
   </div>

   <q-page class="row justify-evenly q-pa-md q-gutter-lg">
    <q-card class="col">
      <q-card-section>
        <div class="text-h6">Model Logs</div>
      </q-card-section>
      <q-card-section>
        <div class="q-pa-md">
          <q-table
            title="Training history"
            dense
            :data="tabledata"
            :columns="columns"
            row-key="id"
          />
        </div>
      </q-card-section>
    </q-card>
    <q-card class="col">
       <q-card-section>
        <div class="text-h6">Training New Model</div>
      </q-card-section>
      <q-card-section>
        <q-card class="my-card" flat bordered>
          <q-form
            @submit="onSubmit"
            @reset="onReset"
            class="q-gutter-md"
            >
            <q-input
              filled
              readonly
              label="Bernoulli Naive Bayes"
              hint="algorithm to train our model "
            />
            <q-input
              filled
              v-model="p1"
              label="p1"
              hint="split dataset "
            />
            <q-input
              filled
              v-model="p2"
              label="p2"
              hint="split dataset"
            />
            <div>
              <q-btn label="Reset" type="reset" color="purple"  class="q-ml-sm" outline/>
            </div>
            <q-separator />
            <div style = "text-align:center">
              <q-btn label="Start" type="submit" color="purple" icon="update" rounded/>
            </div>
            <q-space/>
          </q-form>
        </q-card>
      </q-card-section>
    </q-card>
   </q-page>
  </div>

   <q-dialog v-model="isDialog">
     <q-card class="bg-grey-3 relative-position" style="width: 300px;height:230px">
      <q-card-section class="q-pb-none">
        <div class="text-h6 text-primary">New Model</div>
      </q-card-section>
      <q-separator />
      <q-card-section>
        <transition
          appear
          enter-active-class="animated fadeIn"
          leave-active-class="animated fadeOut"
        >
          <div v-show="showSimulatedReturnData" style="text-align:center;">
            <q-icon name='done' color="positive" style="font-size: 80px;"/>
            <div class="text-purple">
              accuracy: {{info}}
            </div>
            <q-card-actions align="right">
              <q-btn flat label="Got it" color="primary" v-close-popup />
            </q-card-actions>
          </div>
        </transition>
      </q-card-section>
      <q-inner-loading :showing="visible">
        <q-spinner-gears size="100px" color="purple" />
      </q-inner-loading>
    </q-card>
   </q-dialog>
</q-page-container>

</template>

<script>
import Logout from 'src/components/Logout.vue'
import axios from 'axios'

export default {
  name: 'Admin',
  components: {
    Logout
  },
  data () {
    return {
      p1: 0.6,
      p2: 0.4,
      isDialog: false,
      visible: false,
      showSimulatedReturnData: false,
      info: '',
      columns: [
        {
          name: 'id',
          required: true,
          label: 'id',
          align: 'left',
          field: row => row.id,
          format: val => `${val}`,
          sortable: true
        },
        { name: 'type', align: 'center', label: 'type', field: 'type', sortable: true },
        { name: 'accuracy', label: 'accuracy', field: 'accuracy' },
        { name: 'time', label: 'time', field: 'time' }
      ],
      tabledata: [
        {
          id: 0,
          type: 'Bernoulli Naive Bayes',
          accuracy: 0.6994,
          time: '2020-4-12 12:3:26'
        }
      ]

    }
  },

  methods: {
    onSubmit: function () {
      this.isDialog = true
      this.visible = true
      this.showSimulatedReturnData = false
      var param = {
        command: 'true',
        p1: this.p1,
        p2: this.p2
      }
      console.log('start training new model')
      axios.post('http://127.0.0.1:5000/train', param)
        .then(response => {
          console.log('training completed')
          this.info = response.data
          var acc = Number.parseFloat(this.info).toFixed(4)
          this.addToList(acc)
          this.visible = false
          this.showSimulatedReturnData = true
          console.log('accurcy:' + this.info)
        })
        .catch(error => {
          alert('error:' + error.response.data)
        })
    },

    addToList: function (acc) {
      var lastid = this.tabledata.length - 1
      var item = {
        id: lastid + 1,
        type: 'Bernoulli Naive Bayes',
        accuracy: acc,
        time: this.getformatDate()
      }
      return this.tabledata.push(item)
    },

    getformatDate: function () {
      var today = new Date()
      var date = today.getFullYear() + '-' + (today.getMonth() + 1) + '-' + today.getDate()
      var time = today.getHours() + ':' + today.getMinutes() + ':' + today.getSeconds()
      return date + ' ' + time
    },

    onReset: function () {
      this.p1 = 0.6
      this.p2 = 0.4
    }
  }
}
</script>

<style>

</style>
