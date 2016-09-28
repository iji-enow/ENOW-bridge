// var ponte = require('ponte');
var ascoltatori_mqtt = require('ascoltatori');
var ascoltatori_kafka = require('ascoltatori');
var mqtt = require('mqtt')
var fs = require('fs')

var KEY = fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.key')
var CERT = fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.crt')
var TRUSTED_CA_LIST = fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/ca.crt')

var options = {
  key: KEY,
  cert: CERT,
  rejectUnauthorized: true,
  // The CA list will be used to determine if server is authorized
  ca: TRUSTED_CA_LIST
}

var settings_mqtt = {
  type: 'mqtt',
  json: false,
  mqtt: require('mqtt'),
  url: 'mqtt://127.0.0.1:1883'
};

var settings_kafka = {
  type: 'kafka',
  json: false,
  kafka: require('kafka-node'),
  connectString: "127.0.0.1:2181",
  clientId: "ascoltatori",
  groupId: "ascoltatori",
  defaultEncoding: "utf8",
  encodings: {
    image: "buffer"
  }
};

var count = -1;
var functions = new Array()

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('brokerAdd', function(topic, message) {
    var topicName = arguments[0]
    var message = arguments[1]
    console.log("topic name : " + arguments[0] + " message : " + arguments[1]);


    makeFunction(message)

    functions[count]();
  });
});


// fncMap에 key, function 으로 동적으로 함수가 저장된다.
function makeFunction(topicName){
    count++
    return functions[count]=function(){
        ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
          ascoltatori_mqtt.subscribe(topicName, function(topic,message) {
            console.log("topic name : " + arguments[0] + " message : " + arguments[1]);
            console.log("succed subscribing to mqtt topic " + arguments[0]);
            ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
              ascoltatori_kafka.publish('event', message, function() {
                console.log("topic name : " + arguments[0] + " message : " + arguments[1]);
                console.log("succed publishing to kafka topic " + arguments[0]);
              });
            });
          });
        });
    }
}

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('feed', function(topic, message) {
    console.log("topic name : " + arguments[0] + " message : " + arguments[1]);
    console.log("succed subscribing to kafka topic " + arguments[0]);
    ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
      ascoltatori_mqtt.publish(topicName, message, function() {
        console.log("topic name : " + arguments[0] + " message : " + arguments[1]);
        console.log("succed publishing to mqtt topic " + arguments[0]);
      });
    });
  });
});

/*
var tmpFunction = function(topicName) {

  ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
    ascoltatori_mqtt.subscribe(topicName, function(topic,message) {
      console.log("topic name : " + arguments[0] + " message : " + arguments[1]);
      console.log("succed subscribing to mqtt topic " + arguments[0]);
      ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
        ascoltatori_kafka.publish(topicName, message, function() {
          console.log("topic name : " + arguments[0] + " message : " + arguments[1]);
          console.log("succed publishing to kafka topic " + arguments[0]);
        });
      });
    });
  });

  ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
    ascoltatori_kafka.subscribe(topicName, function(topic, message) {
      console.log("topic name : " + arguments[0] + " message : " + arguments[1]);
      console.log("succed subscribing to kafka topic " + arguments[0]);
      ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
        ascoltatori_mqtt.publish(topicName, message, function() {
          console.log("topic name : " + arguments[0] + " message : " + arguments[1]);
          console.log("succed publishing to mqtt topic " + arguments[0]);
        });
      });
    });
  });
}


*/


















/*

var client = mqtt.connect('mqtt://localhost:8883',options)

client.subscribe('enow/+/+/+/order')

client.on('message', function (topic, message) {
  //console.log(message.toString())
  console.log(arguments[0]);
  var topicName = JSON.stringify(arguments[0]);
  ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
    var arr = topicName.substring(1,topicName.length -1).split("/");
    var corporationName = arr[0];
    var serverId = arr[1];
    var brokerId = arr[2];
    var deviceId = arr[3];
    var kafkaTopic = arr[4];

    var str = "{\"corporationName\":\"" + corporationName + "\",\"serverId\":\"" + serverId + "\",\"brokerId\":\"" +  brokerId  + "\",\"deviceId\":\"" + deviceId + "\",\"kafkaTopic\":\"" + kafkaTopic + "\",\"payload\":\"" + message + "\"}"
    var jsonObj = JSON.parse(str);

      ascoltatori_kafka.publish('order', JSON.stringify(jsonObj), function() {
        console.log('message published');
      });
    });
})



ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
  ascoltatori_mqtt.subscribe('enow/+/+/+/tmp', function(topic,message) {
    console.log(arguments[0]);
    var topicName = JSON.stringify(arguments[0]);
    ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
      var arr = topicName.substring(1,topicName.length -1).split("/");
      var corporationName = arr[0];
      var serverId = arr[1];
      var brokerId = arr[2];
      var deviceId = arr[3];
      var kafkaTopic = arr[4];

      var str = "{\"corporationName\":\"" + corporationName + "\",\"serverId\":\"" + serverId + "\",\"brokerId\":\"" +  brokerId  + "\",\"deviceId\":\"" + deviceId + "\",\"kafkaTopic\":\"" + kafkaTopic + "\",\"payload\":\"" + message + "\"}"
      var jsonObj = JSON.parse(str);
      ascoltatori_kafka.publish('order', JSON.stringify(jsonObj), function() {
        console.log('message published');
      });
    });
  });
});

ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
  ascoltatori_mqtt.subscribe('/enow/server0/broker0/device0/alive/response', function(topic,message) {
    console.log("asdasdasdsa" + message)
    var jsonMessage = JSON.parse(message);
    ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
      ascoltatori_kafka.publish('aliveResponse', JSON.stringify(jsonMessage), function() {
        console.log('message published');
      });
    });
  });
});

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('aliveRequest', function(topic, message) {
    console.log(arguments);
    ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
      ascoltatori_mqtt.publish('/enow/server0/broker0/device0/alive/request', message, function() {
        console.log('message published');
      });
    });
  });
});
*/
