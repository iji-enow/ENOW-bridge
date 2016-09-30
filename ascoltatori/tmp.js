
// var ponte = require('ponte');
var ascoltatori_mqtt = require('ascoltatori');
var ascoltatori_kafka = require('ascoltatori');
var mqtt = require('mqtt')
var fs = require('fs')

var KEY = fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.key')
var CERT = fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.crt')
var TRUSTED_CA_LIST = fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/ca.crt')

var options = {
  key: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.key'),
  cert: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.crt'),
  rejectUnauthorized: true,
  // The CA list will be used to determine if server is authorized
  ca: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/ca.crt')
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

var client = mqtt.connect('mqtt://localhost:8883',options)

client.subscribe("enow")
client.publish("enow","ssl test")
client.on('message', function (topic, message) {
  //console.log(message.toString())
  console.log(arguments[0]);
  ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.publish('feed', message, function() {
      console.log('message published');
    });
  });
})

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

//tmp.js
/////////////////////////////////////////////////////////////////////////
//start.js

// var ponte = require('ponte');
/*
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
  // set 'true' if input type is json type
  json: false,
  mqtt: require('mqtt'),
  // must use 'http://'
  url: 'mqtt://127.0.0.1:1883'
};

var settings_kafka = {
  type: 'kafka',
  json: false,
  kafka: require('kafka-node'),
  //connectString: "192.168.0.3:2181",
  connectString: "127.0.0.1:2181",
  clientId: "ascoltatori",
  groupId: "ascoltatori",
  defaultEncoding: "utf8",
  encodings: {
    image: "buffer"
  }
};

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
  ascoltatori_mqtt.subscribe('enow/+/+/+/+', function(topic,message) {
    console.log(arguments);



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

      var roadMapId = arr[4];


      var str = "{\"corporationName\":\"" + corporationName + "\",\"serverId\":\"" + serverId + "\",\"brokerId\":\"" +  brokerId  + "\",\"deviceId\":\"" + deviceId + "\",\"roadMapId\":" + roadMapId + ",\"payload\":\"" + message + "\"}"

      var kafkaTopic = arr[4];

      var str = "{\"corporationName\":\"" + corporationName + "\",\"serverId\":\"" + serverId + "\",\"brokerId\":\"" +  brokerId  + "\",\"deviceId\":\"" + deviceId + "\",\"kafkaTopic\":\"" + kafkaTopic + "\",\"payload\":\"" + message + "\"}"

      var jsonObj = JSON.parse(str);
      ascoltatori_kafka.publish('order', JSON.stringify(jsonObj), function() {
        console.log('message published');
      });
    });
  });
});
*/

/*
ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
  ascoltatori_mqtt.subscribe('test', function(topic,message) {
    console.log(arguments);
    ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
      ascoltatori_kafka.publish('test', message, function() {
=======
ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
  ascoltatori_mqtt.subscribe('/enow/server0/broker0/device0/alive/response', function(topic,message) {
    console.log("asdasdasdsa" + message)
    var jsonMessage = JSON.parse(message);
    ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
      ascoltatori_kafka.publish('aliveResponse', JSON.stringify(jsonMessage), function() {
>>>>>>> 04089f50ea12f4073aebbe8a570ff324340658c4
        console.log('message published');
      });
    });
  });
});
<<<<<<< HEAD
*/

/*
ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('onlytest', function(topic, message) {
    console.log(arguments);
    ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
      ascoltatori_mqtt.publish('onlytest', message, function() {
=======

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('aliveRequest', function(topic, message) {
    console.log(arguments);
    ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
      ascoltatori_mqtt.publish('/enow/server0/broker0/device0/alive/request', message, function() {
>>>>>>> 04089f50ea12f4073aebbe8a570ff324340658c4
        console.log('message published');
      });
    });
  });
});
*/

/*
ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('webhook', function(topic, message) {
    console.log(arguments);
    var options = {
      uri: 'https://hooks.slack.com/services/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',
      method: 'POST',
      json: {
        "text": message
      }
    };

    request(options, function (error, response, body) {
      if (!error && response.statusCode == 200) {
        console.log(body.id) // Print the shortened url.
      }
    });
  });
});
*/
