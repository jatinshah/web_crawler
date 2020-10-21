<template>
  <v-app>

    <v-main>
    <h1>Web Crawler</h1>

    <div class="text-center">
      <v-form @submit.prevent="handleSubmit" ref="form">
     <v-text-field v-model="url"
      label="URL"
      required
    ></v-text-field>
         <v-text-field v-model="depth"
      label="Depth"
      required
    ></v-text-field>
    <v-btn
      rounded
      color="primary"
      dark
      v-on:click="startCrawl()"
    >
      Start Crawl
    </v-btn>

    </v-form>

    </div>

    <br/>
    <br/>
    <br/>

    <v-textarea v-model="crawledUrls" auto-grow rows="20" background-color="grey lighten-2">
    </v-textarea>

    </v-main>
  </v-app>
</template>

<script>

export default {
  name: 'App',
  data: function() {
    return {
      crawledUrls: "",
      connection: null,
      url: "",
      depth: ""
    }
  },
  methods: {
    startCrawl: function() {
      console.log(this.url, this.depth);
      var data = {'action':'default', 'url':this.url, 'depth':this.depth};
      this.crawledUrls = "";
      this.connection.send(JSON.stringify(data));
    }
  },
  created: function() {
    console.log('Starting connection to WSS');
    this.connection = new WebSocket("wss://2hjidiewad.execute-api.us-east-1.amazonaws.com/dev");

    this.connection.onmessage = (event) => {
      console.log(event['data']);
      var data = JSON.parse(event['data']);
      delete data['depth'];
      delete data['parent_url'];
      this.crawledUrls += JSON.stringify(data) + '\n';
    }
    this.connection.onopen = function(event) {
      console.log(event);
      console.log("Successfully connected to the websocket server...");
    }
  }
};
</script>
