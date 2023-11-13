const Kafka = require("kafkajs").Kafka;
const { ip } = require('./var.js');
const { client } = require('./database.js');

run();
async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "myapp",
            "brokers": [ip]
        })

        const consumer = kafka.consumer({ "groupId": "test" });
        console.log('Connecting...')
        await consumer.connect()
        await client.connect();
        console.log('Connected!')

        await consumer.subscribe({
            "topic": "Messages",
            "fromBeginning": true
        })

        await consumer.run({
            "eachMessage": async result => {
                console.log(`Message: ${result.message.value}`)
                let parsedResult = JSON.parse(result.message.value);
                client.query('INSERT INTO dynamic_data (sourcemmsi, navigational_status, rate_of_turn, speed_over_ground, course_over_ground, true_heading, lon, lat, ts) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)',
                    [parsedResult.sourcemmsi, parsedResult.navigationalstatus, parsedResult.rateofturn, parsedResult.speedoverground, parsedResult.courseoverground, parsedResult.trueheading, parsedResult.lon, parsedResult.lat, parsedResult.t,], (err, res) => {
                        if (!err) {
                            console.log(res.rows);
                        } else {
                            console.log(err.message);
                        }
                    })
            }
        })
    }
    catch (ex) {
        console.error(`Something bad happened ${ex}`)
    }
    finally {
        client.end;
    }
}