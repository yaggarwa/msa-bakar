const redis = require("redis");
const kafka = require('kafka-node');
const client = redis.createClient({password:"test123"});
const express = require("express");
const app = express();
const url = require('url');
const qs = require('querystring');
var Producer = kafka.Producer;
var Consumer = kafka.Consumer;
var KeyedMessage = kafka.KeyedMessage;
//const bodyParser = require('body-parser');
app.use(express.json());
 
client.on("error", function(error) {
  console.error(error);
});
//var producer = new Producer(kclient);
//var km = new KeyedMessage('key', 'message');

app.get("/customer", (req, res) => {
    var kclient = new kafka.KafkaClient();
    let parsedUrl = url.parse(req.url);
    let parsedQs = qs.parse(parsedUrl.query);
    client.get (parsedQs.id, function(err, reply){
        console.log(reply);
        var producer = new Producer(kclient);
        var payload = [{topic:'node-logs', messages:reply}];
        producer.on('ready', function(){
            console.log("inside send logic");
            producer.send(payload, function(err, data){
                console.log(err);
                //console.log(data);
            });
            //producer.close();
        });
        producer.on('error', function(err){
            console.log("inside error");
            console.log(err);
            //producer.close();
        });
        res.json(reply);
    });
   });

app.post("/customer", (req, res)=> {
    //const obj = JSON.parse(req);
    console.log(req.body.fname);
    res.send("ok");
});

app.get("/discount", (req, res)=>{
    var kclient = new kafka.KafkaClient();
    var resMessage=[];
    var consumer = new Consumer(
        kclient,
        [
            { topic: 'node-logs', partition: 0 }, { topic: 'node-logs', partition: 1 }
        ],
        {
            autoCommit: false
        }
    );
    consumer.on('message', function(message){
        console.log("inside message");
        //console.log(message);
        consumer.close();
        resMessage.push(message);
        //console.log(resMessage);
    });
    console.log("outside message");
    console.log(resMessage);
    res.send(resMessage);
})

app.listen(3000, () => {
    console.log("Server running on port 3000");
   });
