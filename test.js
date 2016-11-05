var mqtt = require('mqtt')
var kafka = require('kafka-node')
var kafkaProducer = kafka.Producer
var kafkaConsumer = kafka.Consumer
var client = new kafka.Client('127.0.0.1:2181')
var producer = new kafkaProducer(client)
var offset = new kafka.Offset(client);
var fs = require('fs')
//var Mqttclient2  = mqtt.connect('mqtt://127.0.0.1:1883')
var payloads = [
    {
        // topic:'event',
        messages: '',
        partition: 0
    }
]

var options = {
  key: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.key'),
  cert: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.crt'),
  rejectUnauthorized: true,
  // The CA list will be used to determine if server is authorized
  ca: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/ca.crt')
}

console.log(fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/ca.crt'));


var Mqttclient  = mqtt.connect('mqtt://127.0.0.1:8883',options)


offset.fetchLatestOffsets(['feed'], function (error, offsets) {
    if (error)
    return handleError(error);

    var feedLatestOffset = offsets['feed'][0];

    consumer = new kafkaConsumer(
        client,
        [
            {
                topic: 'feed',
                partition: 0,
                offset: feedLatestOffset
            }
        ],
        {
            autoCommit: false,
            fromOffset: true
        }
    );
    // when kafka message came from topic:'log'.

    consumer.on('message', function (message) {
      console.log(message);
      if(message.topic == 'feed'){
        if(message.value == '2'){
          console.log("2 feed came");
          Mqttclient.end()

          a = function(){
            Mqttclient2  = mqtt.connect('mqtt://127.0.0.1:1883')
            console.log("start 2");
            Mqttclient2.publish('enow/server0/a/2/feed', 'Hello mqtt')
            Mqttclient2.subscribe('enow/server0/a/2/feed')


            Mqttclient2.on('message', function (topic, message) {
              // message is Buffer
              console.log(message.toString())
            })
          }

          a();
        }else if(message.value == '1'){
          console.log("1 feed came");
          Mqttclient2.end()

          a = function(){
            Mqttclient  = mqtt.connect('mqtt://127.0.0.1:1883')
            console.log("start 1");
            Mqttclient.publish('enow/server0/a/1/feed', 'Hello mqtt')
            Mqttclient.subscribe('enow/server0/a/1/feed')


            Mqttclient.on('message', function (topic, message) {
              // message is Buffer
              console.log(message.toString())
            })
          }

          a();
        }
      }
    })
});


var a = function(){
  console.log("start1");
  Mqttclient.publish('test', 'Hello mqtt')
  Mqttclient.subscribe('test')


  Mqttclient.on('message', function (topic, message) {
    // message is Buffer
    console.log(message.toString())
  })
}

a()
