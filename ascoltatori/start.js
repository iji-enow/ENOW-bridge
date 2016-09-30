
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

<<<<<<< HEAD
var json = []
=======

var client = mqtt.connect('mqtt://localhost:8883',options)
>>>>>>> 04089f50ea12f4073aebbe8a570ff324340658c4

var functions = new Array()

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('brokerAdd', function(topic, message) {
    console.log("topic name : " + topic + " message : " + message);

    var jsonMessage = JSON.parse(message);

<<<<<<< HEAD
    var brokerId = jsonMessage.brokerId
    var ipAddress = jsonMessage.ipAddress
    var port = jsonMessage.port

    makeFunction(brokerId,ipAddress,port)

    functions[brokerId]();

    for(i = 0 ; i<json.length ; i++){
      if("joon" == json[i].brokerId){
        console.log(i + "번째 json : " + JSON.stringify(json[i]));
      }
    }
  });
});

function makeFunction(brokerId,ipAddress,port){
  return functions[brokerId]=function(){

    var settings_mqtt = {
      type: 'mqtt',
      // set 'true' if input type is json type
      json: false,
      mqtt: require('mqtt'),
      // must use 'http://'
      url: "mqtt://"+ ipAddress+":"+port
    };

    json.push({"brokerId":brokerId,"settings_mqtt":settings_mqtt})

    ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
      ascoltatori_mqtt.subscribe("enow/server0/"+brokerId+"/+/alive/request", function(topic,message) {
        console.log("succeed subscribing to mqtt topic " + topic);
        ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
          ascoltatori_kafka.publish("status", message, function() {
            console.log("succeed publishing to kafka topic status");
          });
        });
=======
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




/*
ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
  ascoltatori_mqtt.subscribe('test', function(topic,message) {
    console.log(arguments);
    ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
      ascoltatori_kafka.publish('test', message, function() {
        console.log('message published');
>>>>>>> 04089f50ea12f4073aebbe8a570ff324340658c4
      });
    });
  }
}

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe("feed", function(topic, message) {
    console.log("succed subscribing to kafka topic " + topic);
    var jsonMessage = JSON.parse(message);
    var topicName = jsonMessage.topic
    var payload = jsonMessage.payload
    var callback = payload.callback
    var jsonCallback = JSON.parse(callback);
    var result = jsonCallback.result

    var arr = topicName.split("/");
    var brokerId = arr[2];

    console.log("brokerId : "+ brokerId );

    for(i = 0 ; i<json.length ; i++){
      if(brokerId == json[i].brokerId){

        ascoltatori_mqtt.build(json[i].settings_mqtt, function (err, ascoltatori_mqtt){
          ascoltatori_mqtt.publish(topicName + "/alive/response", result, function() {
          //ascoltatori_mqtt.publish(corporationName + "/" + serverId + "/" + brokerId + "/" + deviceId + "/alive/response", payload, function() {
            console.log("topic name : " + topicName + "/alive/response message : " + result);
          });
        });
        console.log(i + "번째 json : " + JSON.stringify(json[i]));

      }
    }

  });
});
