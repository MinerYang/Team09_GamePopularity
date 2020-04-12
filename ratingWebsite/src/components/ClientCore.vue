<template>
  <div>
    <q-form @submit="onSubmit" @reset="onReset" class="q-gutter-md">
        <div class="developer">
            <q-card class="my-card">
                <q-card-section>
                    <div class="text-subtitle2">developer</div>
                </q-card-section>
                <q-separator dark inset />
                <!-- <q-card-section>
                    <q-chip :selected.sync="developer.Valve" color="primary" text-color="white" icon="cake">
                      Valve
                    </q-chip>
                    <q-chip :selected.sync="developer.Artifex" color="primary" text-color="white" icon="cake">
                      Artifex Mundi
                    </q-chip>
                    <q-chip :selected.sync="developer.We_Games" color="primary" text-color="white" icon="cake">
                      Wadjet Eye Games
                    </q-chip>
                    <q-chip :selected.sync="developer.th_Expansion" color="primary" text-color="white" icon="cake">
                      07th Expansion
                    </q-chip>
                <div class="q-mt-sm">Your pick: {{ selection }} </div>
                </q-card-section> -->
                <q-card-section>
                    <q-select outlined v-model="other_selct" multiple use-chips :options="options" label="other_selct" />
                </q-card-section>
                <q-badge color="secondary" multi-line>
                    you selected : {{ other_selct  || ''}}
                </q-badge>
            </q-card>
        </div>
        <q-btn label="Submit" type="submit" color="primary"/>
    </q-form>
  </div>
</template>

<script>
export default {
  name: 'ClientCore',
  data () {
    return {
    //   model: null,
      totalSelction: '',
      developer: {
        Valve: false,
        Artifex: false,
        We_Games: false,
        th_Expansion: false
      },
      options: [
        'Team 6 Studios', 'VisualArts/Key'
      ],
      other_selct: null
    }
  },

  computed: {
    selection () {
      return Object.keys(this.developer)
        .filter(type => this.developer[type] === true)
        .join(';')
    }
  },

  methods: {
    onSubmit: function () {
      // this.other_selct
      alert(this.other_selct[0])
    }
  }
}
</script>

<style>
/* .mycard{
    background: radial-gradient(circle, #35a2ff 0%, #014a88 100%)
} */

</style>
