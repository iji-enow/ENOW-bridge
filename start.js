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

var count = 0
var brokerList = new Array()
var functions = new Array()

var findbrokers = function(db, callback) {
   var cursor =db.collection('brokerList').find( );
   cursor.each(function(err, doc) {
      assert.equal(err, null);
      if (doc != null) {
         makeFunction(doc.brokerId,doc.ipAddress,doc.port)
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
  });
});

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('brokerSub', function(topic, message) {
    for(i = 0 ; i<brokerList.length ; i++){
      if(message == brokerList[i].brokerId){
        console.log("delete" + message);
        brokerList.splice(i,1);
        functions.splice(i,1);
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

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('sslAdd', function(topic, message) {
    console.log("topic name : " + topic + " message : " + message);

    addSSLFunction(message)

  });
});

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('sslSub', function(topic, message) {
    console.log("topic name : " + topic + " message : " + message);

    subSSLFunction(message)

  });
});

function makeFunction(brokerId,ipAddress,port){
  functions.push(function(){
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

      var brokerSSL = mqtt.connect(brokerList[i].brokerSetting,options)

      brokerList[i] = {"brokerId":brokerId,"broker":brokerSSL,"brokerSetting":brokerList[i].brokerSetting,"options":options}

      functions[i] = function(){

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

      index = i;
    }
  }


  functions[index]()
}

function subSSLFunction(brokerId){
  var index
  for(i = 0 ; i<brokerList.length ; i++){
    if(brokerId == brokerList[i].brokerId){
      var broker = mqtt.connect(brokerList[i].brokerSetting)

      brokerList[i] = {"brokerId":brokerId,"broker":broker,"brokerSetting":brokerList[i].brokerSetting}

      functions[i] = function(){

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

      index = i;
    }
  }

  functions[index]()
}
