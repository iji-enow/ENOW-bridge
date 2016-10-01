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
         console.log("made connection to brokerId : " + doc.brokerId + " ipAddress : " + doc.ipAddress + " port : "  + doc.port);
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
    console.log("added broker : " + message);

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
        console.log("deleting broker is not succeed")
        brokerList.splice(i,1);
        functions.splice(i,1);
      }
    }
  });
});


ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe("feed", function(topic, message) {
    var jsonMessage = JSON.parse(message);

    var topicName = jsonMessage.topic
    var payload = jsonMessage.payload
    var callback = payload.callback


    var arr = topicName.split("/");
    var brokerId = arr[2];

    for(i = 0 ; i<brokerList.length ; i++){
      if(brokerId == brokerList[i].brokerId){
        brokerList[i].broker.publish(topicName + "/feed",JSON.stringify(callback))
        console.log("messaging to a message " + JSON.stringify(callback)+" to topic name : " + topicName + "/feed succeed");
      }
    }
  });
});

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('sslAdd', function(topic, message) {
    addSSLFunction(message)

    console.log("adding ssl option to topic : " + topic + " succeed");
  });
});

ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('sslSub', function(topic, message) {
    subSSLFunction(message)

    console.log("subtracting ssl option to topic : " + topic + " succeed");
  });
});

function makeFunction(brokerId,ipAddress,port){
  functions.push(function(){
    var brokerSetting = "mqtt://"+ ipAddress+":"+port
    var brokerStatus = mqtt.connect(brokerSetting)
    var brokerOrder = mqtt.connect(brokerSetting)

    brokerList.push({"brokerId":brokerId,"brokerStatus":brokerStatus,"brokerOrder":brokerOrder,"brokerSetting":brokerSetting})

    brokerStatus.subscribe("enow/server0/"+brokerId+"/+/status");
    brokerStatus.on('message', function (topic, message) {
      console.log("succeed subscribing to mqtt topic " + topic);

      ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
        ascoltatori_kafka.publish("status", message, function() {
          console.log("succeed publishing to kafka topic status");
        });
      });
    });

    brokerOrder.subscribe("enow/server0/"+brokerId+"/+/order");
    brokerOrder.on('message', function (topic, message) {
      console.log("succeed subscribing to mqtt topic " + topic);

      ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
        ascoltatori_kafka.publish("order", message, function() {
          console.log("succeed publishing to kafka topic order");
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

      var brokerStatusSSL = mqtt.connect(brokerList[i].brokerSetting,options)
      var brokerOrderSSL = mqtt.connect(brokerList[i].brokerSetting,options)

      brokerList[i] = {"brokerId":brokerId,"brokerStatusSSL":brokerStatusSSL,"brokerOrderSSL":brokerOrderSSL,"brokerSetting":brokerList[i].brokerSetting,"options":options}

      functions[i] = function(){

        brokerStatusSSL.subscribe("enow/server0/"+brokerId+"/+/status");
        brokerStatusSSL.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
            ascoltatori_kafka.publish("status", message, function() {
              console.log("succeed publishing to kafka topic status");
            });
          });
        });

        brokerOrderSSL.subscribe("enow/server0/"+brokerId+"/+/order");
        brokerOrderSSL.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
            ascoltatori_kafka.publish("order", message, function() {
              console.log("succeed publishing to kafka topic order");
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
      var brokerStatus = mqtt.connect(brokerList[i].brokerSetting)
      var brokerOrder = mqtt.connect(brokerList[i].brokerSetting)

      brokerList[i] = {"brokerId":brokerId,"brokerStatus":brokerStatus,"brokerOrder":brokerOrder,"brokerSetting":brokerList[i].brokerSetting}

      functions[i] = function(){

        brokerStatus.subscribe("enow/server0/"+brokerId+"/+/status");
        brokerStatus.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
            ascoltatori_kafka.publish("status", message, function() {
              console.log("succeed publishing to kafka topic status");
            });
          });
        });

        brokerOrder.subscribe("enow/server0/"+brokerId+"/+/order");
        brokerOrder.on('message', function (topic, message) {
          console.log("succeed subscribing to mqtt topic " + topic);

          ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
            ascoltatori_kafka.publish("order", message, function() {
              console.log("succeed publishing to kafka topic order");
            });
          });
        });
      }

      index = i;
    }
  }

  functions[index]()
}
