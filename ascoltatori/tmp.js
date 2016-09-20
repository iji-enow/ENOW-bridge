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

var client = mqtt.connect('mqtt://localhost:8883',options)

client.subscribe('test')
client.publish('test', 'Hello mqtt')

client.on('message', function (topic, message) {
  console.log(message.toString())
})
