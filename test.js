var mqtt = require('mqtt')
var kafka = require('kafka-node')
var fs = require('fs')
var MongoClient = require('mongodb').MongoClient
var assert = require('assert')

var kafkaProducer = kafka.Producer
var kafkaConsumer = kafka.Consumer
var client = new kafka.Client('192.168.99.100:2181')
var producer = new kafkaProducer(client)
var offset = new kafka.Offset(client);
var payloads = [
    {
        // topic:'event',
        messages: '',
        partition: 0
    }
]

var consumer = new kafkaConsumer(
  client,
  [
    {
      topic: 'feed',
      partition: 0,
      time : -1

    },
    {
      topic: 'brokerAdd',
      partition: 0,
      time : -1
    }/*,
    {
      topic: 'brokerSub',
      partition: 0,
      time : -1
    },
    {
      topic: 'sslAdd',
      partition: 0,
      time : -1
    },
    {
      topic: 'sslSub',
      partition: 0,
      time : -1
    }*/

  ]
);





var brokerList = new Array()
var functions = new Array()

var findbrokers = function(db, callback) {
   var cursor =db.collection('brokerList').find( );
   cursor.each(function(err, doc) {
      assert.equal(err, null);
      if (doc != null) {
         makeFunction(doc.brokerId,doc.ipAddress,doc.port)
         console.log('made connection to brokerId : ' + doc.brokerId + ' ipAddress : ' + doc.ipAddress + ' port : '  + doc.port);
      } else {
         callback();
      }
   });
};

MongoClient.connect('mongodb://192.168.99.100:27017/connectionData', function(err, db) {
  assert.equal(null, err);
  findbrokers(db, function() {
      db.close();
  });
});


consumer.on('message', function (message) {
  if(message.topic == 'feed'){
    var tmp = JSON.parse(message.value)

    var topicName = tmp.topic
    var payload = tmp.payload
    var callback = payload.callback

    var arr = topicName.split("/");
    var brokerId = arr[2];

    for(i = 0 ; i<brokerList.length ; i++){
      if(brokerId == brokerList[i].brokerId){
        brokerList[i].brokerFeed.publish(topicName + "/feed",JSON.stringify(callback))
        console.log("messaging to a message " + JSON.stringify(callback)+" to topic name : " + topicName + "/feed succeed");
      }
    }
  }else if(message.topic == 'brokerAdd'){
    console.log("added broker : " + message.value);

    var tmp = JSON.parse(message.value)

    var brokerId = tmp.brokerId
    var ipAddress = tmp.ipAddress
    var port = tmp.port

    makeFunction(brokerId,ipAddress,port)
  }else if(message.topic == 'brokerSub'){
    for(i = 0 ; i<brokerList.length ; i++){
      if(message == brokerList[i].brokerId){
        console.log("delete" + message.value);
        console.log("deleting broker is not succeed")
        brokerList.splice(i,1);
        functions.splice(i,1);
      }
    }
  }else if(message.topic == 'sslAdd'){
    addSSLFunction(message.value)
    console.log("adding ssl option to topic : " + topic + " succeed");
  }else if(message.topic == 'sslSub'){
    subSSLFunction(message.value)
    console.log("subtracting ssl option to topic : " + topic + " succeed");
  }
});



function makeFunction(brokerId,ipAddress,port){
  functions.push(function(){
    var brokerSetting = "mqtt://"+ ipAddress+":"+port
    var brokerStatus = mqtt.connect(brokerSetting)
    var brokerOrder = mqtt.connect(brokerSetting)
    var brokerFeed = mqtt.connect(brokerSetting)

    brokerList.push({"brokerId":brokerId,"brokerFeed":brokerFeed,"brokerStatus":brokerStatus,"brokerOrder":brokerOrder,"brokerSetting":brokerSetting})

    brokerStatus.subscribe("enow/server0/"+brokerId+"/+/status");
    brokerStatus.on('message', function (topic, message) {
      console.log("succeed subscribing to mqtt topic " + topic);

      var payload = [
        { topic: 'status', messages: message, partition: 0 }
      ];

      producer.send(payload, function (err, data) {
        console.log("succeed publishing to kafka topic status");
      });

    });

    brokerOrder.subscribe("enow/server0/"+brokerId+"/+/order");
    brokerOrder.on('message', function (topic, message) {
      console.log("succeed subscribing to mqtt topic " + topic);

      var payload = [
        { topic: 'order', messages: message, partition: 0 }
      ];

      producer.send(payload, function (err, data) {
          console.log("succeed publishing to kafka topic order");
      });

    });


  })

  functions[functions.length-1]()
}

function addSSLFunction(brokerId){
  var index
  for(i = 0 ; i<brokerList.length ; i++){
    if(brokerId == brokerList[i].brokerId){
      var options = {
        key: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.key'),
        cert: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.crt'),
        rejectUnauthorized: true,
        // The CA list will be used to determine if server is authorized
        ca: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/ca.crt')
      }

      var brokerStatusSSL = mqtt.connect(brokerList[i].brokerSetting,options)
      var brokerOrderSSL = mqtt.connect(brokerList[i].brokerSetting,options)
      var brokerFeedSSL = mqtt.connect(brokerList[i].brokerSetting,options)

      brokerList[i] = {"brokerId":brokerId,"brokerFeedSSL":brokerFeedSSL,"brokerStatusSSL":brokerStatusSSL,"brokerOrderSSL":brokerOrderSSL,"brokerSetting":brokerList[i].brokerSetting,"options":options}

      functions[i] = function(){

        brokerStatusSSL.subscribe("enow/server0/"+brokerId+"/+/status");
        brokerStatusSSL.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          var payload = [
            { topic: 'status', messages: message, partition: 0 }
          ];

          producer.send(payload, function (err, data) {
            console.log("succeed publishing to kafka topic status");
          });

        });

        brokerOrderSSL.subscribe("enow/server0/"+brokerId+"/+/order");
        brokerOrderSSL.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          var payload = [
            { topic: 'order', messages: message, partition: 0 }
          ];

            producer.send(payload, function (err, data) {
              console.log("succeed publishing to kafka topic order");
            });
        });
      }
      index = i;
    }
  }
  functions[index]()
}

function subSSLFunction(brokerId){
  var index
  for(i = 0 ; i<brokerList.length ; i++){
    if(brokerId == brokerList[i].brokerId){
      var brokerStatus = mqtt.connect(brokerList[i].brokerSetting)
      var brokerOrder = mqtt.connect(brokerList[i].brokerSetting)

      brokerList[i] = {"brokerId":brokerId,"brokerStatus":brokerStatus,"brokerOrder":brokerOrder,"brokerSetting":brokerList[i].brokerSetting}

      functions[i] = function(){
        brokerStatus.subscribe("enow/server0/"+brokerId+"/+/status");
        brokerStatus.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          var payload = [
            { topic: 'status', messages: message, partition: 0 }
          ];
            producer.send(payloads, function (err, data) {
              console.log("succeed publishing to kafka topic status");
            });

        });

        brokerOrder.subscribe("enow/server0/"+brokerId+"/+/order");
        brokerOrder.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          var payload = [
            { topic: 'order', messages: message, partition: 0 }
          ];

            producer.send(payloads, function (err, data) {
              console.log("succeed publishing to kafka topic order");
            });

        });
      }

      index = i;
    }
  }

  functions[index]()
}
