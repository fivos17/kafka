const Kafka = require("kafkajs").Kafka;
const {ip} = require('./var.js');

run();
async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "myapp",
            "brokers": [ip]
        })

        const admin = kafka.admin();
        console.log('Connecting...')
        await admin.connect()
        console.log('Connected!')
        await admin.createTopics({
            'topics': [
                { 'topic': 'Messages', 'numPartitions': 1 }
            ]
        })
        console.log('Created successfully!')
        await admin.disconnect();
    }
    catch (ex) {
        console.error(`Something bad happened ${ex}`)
    }
    finally{
        process.exit(0);
    }
}