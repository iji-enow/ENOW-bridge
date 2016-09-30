var ascoltatori_kafka = require('ascoltatori')
var mqtt = require('mqtt')
var fs = require('fs')
var MongoClient = require('mongodb').MongoClient
var assert = require('assert')

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

var brokerList = new Array()
var functions = new Array()

var findbrokers = function(db, callback) {
   var cursor =db.collection('brokerList').find( );
   cursor.each(function(err, doc) {
      assert.equal(err, null);
      if (doc != null) {
         makeFunction(doc.brokerId,doc.ipAddress,doc.port)
         functions[doc.brokerId]();
      } else {
         callback();
      }
   });
};

MongoClient.connect('mongodb://localhost:27017/connectionData', function(err, db) {
  assert.equal(null, err);
  findbrokers(db, function() {
      db.close();
  });
});


ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('brokerAdd', function(topic, message) {
    console.log("topic name : " + topic + " message : " + message);

    var jsonMessage = JSON.parse(message);

    var brokerId = jsonMessage.brokerId
    var ipAddress = jsonMessage.ipAddress
    var port = jsonMessage.port

    makeFunction(brokerId,ipAddress,port)

    functions[brokerId]();

  });
});

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('brokerSub', function(topic, message) {
    for(i = 0 ; i<brokerList.length ; i++){
      if(message == brokerList[i].brokerId){
        console.log("delete" + message);
        brokerList.splice(i,1);
        functions.splice(message, 1);
      }
    }
  });
});


ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe("feed", function(topic, message) {
    console.log("succeed subscribing to kafka topic " + topic);

    var jsonMessage = JSON.parse(message);

    var topicName = jsonMessage.topic
    var payload = jsonMessage.payload
    var callback = payload.callback
    var jsonCallback = JSON.parse(callback);
    var result = jsonCallback.result

    var arr = topicName.split("/");
    var brokerId = arr[2];

    for(i = 0 ; i<brokerList.length ; i++){
      if(brokerId == brokerList[i].brokerId){
        brokerList[i].broker.publish(topicName + "/alive/response",result)
        console.log("topic name : " + topicName + "/alive/response message : " + result);
      }
    }
  });
});

scoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('sslAdd', function(topic, message) {
    console.log("topic name : " + topic + " message : " + message);

    addSSLFunction(message)

    functions[message]();

  });
});

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('sslSub', function(topic, message) {
    console.log("topic name : " + topic + " message : " + message);

    subSSLFunction(message)

    functions[message]();

  });
});

function makeFunction(brokerId,ipAddress,port){

  return functions[brokerId]=function(){

    var brokerSetting = "mqtt://"+ ipAddress+":"+port
    var broker = mqtt.connect(brokerSetting)

    brokerList.push({"brokerId":brokerId,"broker":broker,"brokerSetting":brokerSetting})

    broker.subscribe("enow/server0/"+brokerId+"/+/alive/request");
    broker.on('message', function (topic, message) {
      console.log("succeed subscribing to mqtt topic " + topic);

      ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
        ascoltatori_kafka.publish("status", message, function() {
          console.log("succeed publishing to kafka topic status");
        });
      });
    });
  }
}

function addSSLFunction(brokerId){
  return functions[brokerId]=function(){
    for(i = 0 ; i<brokerList.length ; i++){
      if(brokerId == brokerList[i].brokerId){
        var options = {
          key: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.key'),
          cert: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.crt'),
          rejectUnauthorized: true,
          // The CA list will be used to determine if server is authorized
          ca: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/ca.crt')
        }

        var brokerSSL = mqtt.connect(brokerList[i].brokerSetting,options)

        brokerList[i] = {"brokerId":brokerId,"broker":brokerSSL,"brokerSetting":brokerList[i].brokerSetting,"options":options}

        brokerSSL.subscribe("enow/server0/"+brokerId+"/+/alive/request");
        brokerSSL.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
            ascoltatori_kafka.publish("status", message, function() {
              console.log("succeed publishing to kafka topic status");
            });
          });
        });
      }
    }
  }
}

function subSSLFunction(brokerId){
  return functions[brokerId]=function(){

    for(i = 0 ; i<brokerList.length ; i++){
      if(brokerId == brokerList[i].brokerId){
        var brokerSetting = mqtt.connect(brokerList[i].brokerSetting)

        var broker = mqtt.connect(brokerSetting)

        brokerList[i] = {"brokerId":brokerId,"broker":broker,"brokerSetting":brokerSetting}

        broker.subscribe("enow/server0/"+brokerId+"/+/alive/request");
        broker.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
            ascoltatori_kafka.publish("status", message, function() {
              console.log("succeed publishing to kafka topic status");
            });
          });
        });
      }
    }
  }
}







/*
var findbrokers = function(db, callback) {
   var cursor =db.collection('brokerList').find( );
   cursor.each(function(err, doc) {
      assert.equal(err, null);
      if (doc != null) {
         makeFunction(doc.brokerId,doc.ipAddress,doc.port)
         functions[doc.brokerId]();
      } else {
         callback();
      }
   });
};

MongoClient.connect('mongodb://localhost:27017/connectionData', function(err, db) {
  assert.equal(null, err);
  findbrokers(db, function() {
      db.close();
  });
});


ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('brokerAdd', function(topic, message) {
    console.log("topic name : " + topic + " message : " + message);

    var jsonMessage = JSON.parse(message);

    var brokerId = jsonMessage.brokerId
    var ipAddress = jsonMessage.ipAddress
    var port = jsonMessage.port

    makeFunction(brokerId,ipAddress,port)

    functions[brokerId]();

  });
});

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('brokerSub', function(topic, message) {
    for(i = 0 ; i<brokerList.length ; i++){
      if(message == brokerList[i].brokerId){
        console.log("delete" + message);
        brokerList.splice(i,1);
        functions.splice(message, 1);
      }
    }
  });
});


ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe("feed", function(topic, message) {
    console.log("succeed subscribing to kafka topic " + topic);

    var jsonMessage = JSON.parse(message);

    var topicName = jsonMessage.topic
    var payload = jsonMessage.payload
    var callback = payload.callback
    var jsonCallback = JSON.parse(callback);
    var result = jsonCallback.result

    var arr = topicName.split("/");
    var brokerId = arr[2];

    for(i = 0 ; i<brokerList.length ; i++){
      if(brokerId == brokerList[i].brokerId){
        brokerList[i].broker.publish(topicName + "/alive/response",result)
        console.log("topic name : " + topicName + "/alive/response message : " + result);
      }
    }
  });
});

scoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('sslAdd', function(topic, message) {
    console.log("topic name : " + topic + " message : " + message);

    addSSLFunction(message)

    functions[message]();

  });
});

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('sslSub', function(topic, message) {
    console.log("topic name : " + topic + " message : " + message);

    subSSLFunction(message)

    functions[message]();

  });
});

function makeFunction(brokerId,ipAddress,port){

  return functions[brokerId]=function(){

    var brokerSetting = "mqtt://"+ ipAddress+":"+port
    var broker = mqtt.connect(brokerSetting)

    brokerList.push({"brokerId":brokerId,"broker":broker,"brokerSetting":brokerSetting})

    broker.subscribe("enow/server0/"+brokerId+"/+/alive/request");
    broker.on('message', function (topic, message) {
      console.log("succeed subscribing to mqtt topic " + topic);

      ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
        ascoltatori_kafka.publish("status", message, function() {
          console.log("succeed publishing to kafka topic status");
        });
      });
    });
  }
}

function addSSLFunction(brokerId){
  return functions[brokerId]=function(){
    for(i = 0 ; i<brokerList.length ; i++){
      if(brokerId == brokerList[i].brokerId){
        var options = {
          key: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.key'),
          cert: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/iui-MacBook-Air.local.crt'),
          rejectUnauthorized: true,
          // The CA list will be used to determine if server is authorized
          ca: fs.readFileSync('/Users/leegunjoon/Documents/downloadSpace/tools/TLS/ca.crt')
        }

        var brokerSSL = mqtt.connect(brokerList[i].brokerSetting,options)

        brokerList[i] = {"brokerId":brokerId,"broker":brokerSSL,"brokerSetting":brokerList[i].brokerSetting,"options":options}

        brokerSSL.subscribe("enow/server0/"+brokerId+"/+/alive/request");
        brokerSSL.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
            ascoltatori_kafka.publish("status", message, function() {
              console.log("succeed publishing to kafka topic status");
            });
          });
        });
      }
    }
  }
}

function subSSLFunction(brokerId){
  return functions[brokerId]=function(){

    for(i = 0 ; i<brokerList.length ; i++){
      if(brokerId == brokerList[i].brokerId){
        var brokerSetting = mqtt.connect(brokerList[i].brokerSetting)

        var broker = mqtt.connect(brokerSetting)

        brokerList[i] = {"brokerId":brokerId,"broker":broker,"brokerSetting":brokerSetting}

        broker.subscribe("enow/server0/"+brokerId+"/+/alive/request");
        broker.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
            ascoltatori_kafka.publish("status", message, function() {
              console.log("succeed publishing to kafka topic status");
            });
          });
        });
      }
    }
  }
}
*/
















/*
ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('brokerAdd', function(topic, message) {
    console.log("topic name : " + topic + " message : " + message);

    var jsonMessage = JSON.parse(message);

    var brokerId = jsonMessage.brokerId
    var ipAddress = jsonMessage.ipAddress
    var port = jsonMessage.port

    makeFunction(brokerId,ipAddress,port)

    functions[brokerId]();
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
      });
    });
  }
}

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe("feed", function(topic, message) {
    console.log("succded subscribing to kafka topic " + topic);

    var jsonMessage = JSON.parse(message);

    var topicName = jsonMessage.topic
    var payload = jsonMessage.payload
    var callback = payload.callback
    var jsonCallback = JSON.parse(callback);
    var result = jsonCallback.result

    var arr = topicName.split("/");
    var brokerId = arr[2];

    for(i = 0 ; i<json.length ; i++){
      if(brokerId == json[i].brokerId){
        ascoltatori_mqtt.build(json[i].settings_mqtt, function (err, ascoltatori_mqtt){
          ascoltatori_mqtt.publish(topicName + "/alive/response", result, function() {
          //ascoltatori_mqtt.publish(corporationName + "/" + serverId + "/" + brokerId + "/" + deviceId + "/alive/response", payload, function() {
            console.log("topic name : " + topicName + "/alive/response message : " + result);
          });
        });
      }
    }
  });
});
*/
