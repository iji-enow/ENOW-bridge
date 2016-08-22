// var ponte = require('ponte');
var ascoltatori_mqtt = require('ascoltatori');
var ascoltatori_kafka = require('ascoltatori');
var mqtt = require('mqtt');
var arrrg = "no connection";
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
  connectString: "localhost:2181",
  clientId: "ascoltatori",
  groupId: "ascoltatori",
  defaultEncoding: "utf8",
  encodings: {
    image: "buffer"
  }
};


ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
  ascoltatori_mqtt.subscribe('test', function(topic,message) {
    console.log(arguments);
    ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
      ascoltatori_kafka.publish('test', message, function() {
        console.log('message published');
      });
    });
  });
});


ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
  ascoltatori_kafka.subscribe('onlytest', function(topic, message) {
    console.log(arguments);
    ascoltatori_mqtt.build(settings_mqtt, function (err, ascoltatori_mqtt){
      ascoltatori_mqtt.publish('onlytest', message, function() {
        console.log('message published');
      });
    });
  });
});
// build for mqtt
/*
ascoltatori.build(settings_mqtt, function (err, ascoltatore_mqtt) {
    ascoltatore.subscribe("topic_from_mqtt", function() {
        console.log(arguments);
        arrrg = arguments;
        // for testing..
        ascoltatore.publish("topic_for_mqtt",arrrg, function(){
            console.log(arguments);
        });
        // build for kafka
        ascoltatori_kafka.build(settings_kafka, function (err_kafka, ascoltatore_kafka){
            ascoltatore_kafka.publish("topic_for_kafka",arrrg, function(){
                console.log(arguments);
            });
            if(err_kafka){
                console.log(err)
            }
        });
    });
    if(err){
        console.log(err)
    }



});
*/
