var functions = new Array()
var ascoltatori_kafka = require('ascoltatori')
var mqtt = require('mqtt')
var sleep = require('sleep')

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

functions.push(function(){
  var brokerSetting = "mqtt://127.0.0.1:1883"
  var broker = mqtt.connect(brokerSetting)

  broker.subscribe("enow/server0/broker1/+/alive/request");
  broker.on('message', function (topic, message) {
    console.log("succeed subscribing to mqtt topic " + topic);

    ascoltatori_kafka.build(settings_kafka, function (err, ascoltatori_kafka){
      ascoltatori_kafka.publish("status", message, function() {
        console.log("succeed publishing to kafka topic status");
      });
    });
  });
})

console.log(functions.length);

functions[functions.length-1]()

for(i= 0 ; i<functions.length ; i++){
  console.log(functions[i]);
}

functions.splice(0,1);

console.log(functions.length);

for(i= 0 ; i<functions.length ; i++){
  console.log(functions[i]);
}
