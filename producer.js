const Kafka = require("kafkajs").Kafka;
const {ip} = require('./var.js');
const dynamic = require('./dynamic.json');

run();
async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "myapp",
            "brokers": [ip]
        })

        const producer = kafka.producer();
        console.log('Connecting...')
        await producer.connect()
        console.log('Connected!')
        const result = await producer.send({
            "topic": "Messages",
            "messages": [
                {"value": JSON.stringify(dynamic[Math.floor(Math.random() * 998)]), "partition": 0}
            ]
        })
        console.log(`Sent successfully! ${JSON.stringify(result)}`)
        await producer.disconnect();
    }
    catch (ex) {
        console.error(`Something bad happened ${ex}`)
    }
    finally{
        process.exit(0);
    }
}