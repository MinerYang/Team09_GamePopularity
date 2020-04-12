<template>
<div>
    <q-page class="flex flex-center">
        <!-- <Logout /> -->
    <q-form @submit="onSubmit" class="q-gutter-md">
      <q-select
        name="developer"
        v-model="dev_sel"
        :options="dev_options"
        @filter="filter_dev"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="developer"
      />

      <q-select
        name="publisher"
        v-model="pub_sel"
        :options="pub_options"
        @filter="filter_pub"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="publisher"
      />

      <q-select
        name="platforms"
        v-model="plt_sel"
        multiple
        use-chips
        :options="plt_options"
        color="primary"
        filled
        clearable
        label="platforms"
      />

      <q-select
        name="categories"
        v-model="cat_sel"
        multiple
        use-chips
        :options="cat_options"
        @filter="filter_cat"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="categories"
      />

      <q-select
        name="tags"
        v-model="tag_sel"
        multiple
        use-chips
        :options="tag_options"
        @filter="filter_tag"
        color="primary"
        filled
        clearable
        use-input
        input-debounce="0"
        label="tags"
      />
      <q-input outlined v-model="price" label="price"
        mask="#.##"
        fill-mask="0"
        reverse-fill-mask/>

      <p class="q-mt-md"></p>
      <div class="q-gutter-lg btn" style="text-align:center">
         <q-btn label="Submit" type="submit" color="primary"/>
         <q-btn @click="clear" label="clear" color="primary"/>
      </div>
    </q-form>
    <Logout />
    </q-page>

    <q-dialog v-model="isDialog">
      <q-card>
        <q-img src = "https://steamcdn-a.akamaihd.net/steam/apps/10/header.jpg?t=1528733245"/>
        <q-card-section>
            <div class="row no-wrap items-center">
            <div class="col text-h6 ellipsis">
              Prediction result
            </div>
            <div class="col-auto ellipsis text-teal" style="font-size: 1.2em;">
              <q-icon name="done_outline "/>
              score:{{info}}
            </div>
          </div>
          <q-rating v-model="stars" :max="5" size="32px" />
        </q-card-section>
        <q-separator />
        <q-card-section>
            <div class="row no-wrap items-center">
            <div class="col text-h6 ellipsis">
              Probability
            </div>
            <div class="col-auto ellipsis text-teal" style="font-size: 1.2em;">
              <q-icon name="info "/>
              about
            </div>
          </div>
          <p>77.56%</p>
        </q-card-section>
        <q-separator />
        <q-card-section>
            <div class="row no-wrap items-center">
            <div class="col text-h6 ellipsis">
              Popularity Level
            </div>
            <div class="col-auto ellipsis text-teal" style="font-size: 1.2em;">
              <q-icon name="info "/>
              about
            </div>
          </div>
          <p> positive</p>
        </q-card-section>
        <q-separator />
        <q-card-section class="q-pt-none">
          <div class="text-orange ellipsis" style="font-size: 1.0em;">
            Team9 production
          </div>
          <div class="text-caption text-grey">
            This prediction result is for reference only & all rights reserved.
          </div>
        </q-card-section>

        <q-card-actions align="right">
          <q-btn flat label="Got it" color="primary" v-close-popup />

        </q-card-actions>
      </q-card>
    </q-dialog>
</div>
</template>

<script>
import Logout from 'src/components/Logout.vue'
// import Result from 'src/components/Result.vue'
import axios from 'axios'

const dev = ['Valve', 'Artifex', 'Wadjet Eye Games', 'Team 6 Studios', 'VisualArts/Key']
const pub = ['Valve', 'Artifex']
const cat = ['Multi-player', 'Online Multi-Player', 'Local Multi-Player', 'Valve Anti-Cheat enabled']
const tag = ['Multi-player', 'Online Multi-Player', 'Local Multi-Player', 'Valve Anti-Cheat enabled']

function initData () {
  return {
    // selection options
    dev_options: dev,
    pub_options: pub,
    plt_options: ['linux', 'mac', 'windows'],
    cat_options: cat,
    tag_options: tag,
    // input field
    price: null,

    // to store selections data
    dev_sel: '',
    pub_sel: '',
    plt_sel: [],
    cat_sel: [],
    tag_sel: [],

    // response info
    info: '',
    isDialog: false,
    stars: 0
  }
}

export default {
  name: 'Client',
  components: {
    Logout
  },

  data () {
    return initData()
  },

  methods: {
    onSubmit: function () {
      this.consolePrint()
      const inputdata = this.sendDataConstruct()
      console.log(inputdata)
      axios.post('http://127.0.0.1:5000/predict', inputdata)
        .then(response => {
          this.info = response.data
          this.stars = this.info
          this.isDialog = true
          console.log(this.info)
        })
        .catch(error => {
          alert('error:' + error.response.data)
        })
    },

    sendDataConstruct: function () {
      var param = {
        developer: this.dev_sel,
        publisher: this.pub_sel,
        platforms: this.parseData(this.plt_sel),
        categories: this.parseData(this.cat_sel),
        tags: this.parseData(this.tag_sel),
        price: this.price
      }
      return param
    },

    consolePrint: function () {
      console.log(this.dev_sel)
      console.log(this.pub_sel)
      console.log(this.parseData(this.plt_sel))
      console.log(this.cat_sel)
      console.log(this.tag_sel)
      console.log(this.price)
    },

    parseData: function (arr) {
      var text = ''
      for (var i = 0; i < arr.length; i++) {
        text += arr[i] + ';'
      }
      return text
    },

    clear: function () {
      Object.assign(this.$data, initData())
    },

    filter_dev (val, update) {
      if (val === '') {
        update(() => {
          this.dev_options = dev
        })
        return
      }
      update(() => {
        const needle = val.toLowerCase()
        this.dev_options = dev.filter(v => v.toLowerCase().indexOf(needle) > -1)
      })
    },

    filter_pub (val, update) {
      if (val === '') {
        update(() => {
          this.pub_options = pub
        })
        return
      }
      update(() => {
        const needle = val.toLowerCase()
        this.pub_options = pub.filter(v => v.toLowerCase().indexOf(needle) > -1)
      })
    },

    filter_cat (val, update) {
      if (val === '') {
        update(() => {
          this.cat_options = cat
        })
        return
      }
      update(() => {
        const needle = val.toLowerCase()
        this.cat_options = cat.filter(v => v.toLowerCase().indexOf(needle) > -1)
      })
    },

    filter_tag (val, update) {
      if (val === '') {
        update(() => {
          this.tag_options = tag
        })
        return
      }
      update(() => {
        const needle = val.toLowerCase()
        this.tag_options = tag.filter(v => v.toLowerCase().indexOf(needle) > -1)
      })
    }

  }
}
</script>

<style>

</style>
