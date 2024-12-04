const { default: axios } = require("axios");
const mqtt = require("mqtt");
const Queue = require("bull");
require("dotenv").config();

const options = {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  protocol: process.env.MQTT_PROTOCOL,
  port: Number(process.env.MQTT_PORT),
};

const mqttUrl = process.env.MQTT_URL;
const topicPago = process.env.MQTT_TOPIC;
const topicSensor = process.env.MQTT_TOPIC_SENSOR;
const topicAlert = process.env.MQTT_TOPIC_ALERT;

if (
  !options.username ||
  !options.password ||
  !mqttUrl ||
  !topicPago ||
  !topicSensor ||
  !topicAlert
) {
  throw new Error("Las variables de entorno no están definidas correctamente.");
}

const sensorQueue = new Queue("sensor-messages");
const alertQueue = new Queue("alert-messages");

sensorQueue.process(5, async (job) => {
  const { topic, message } = job.data;
  try {
    const parsedMessage = JSON.parse(message);
    await axios.post(`${process.env.URL_API}/notification/data`, parsedMessage);
    console.log(`Mensaje de sensor procesado: ${parsedMessage}`);
  } catch (error) {
    console.error("Error al procesar mensaje de sensor en la cola:", error);
  }
});

alertQueue.process(5, async (job) => {
  const { topic, message } = job.data;
  try {
    const parsedMessage = JSON.parse(message);
    await axios.post(
      `${process.env.URL_API}/notification/alert`,
      parsedMessage
    );
    console.log(`Mensaje de alerta procesado: ${parsedMessage}`);
  } catch (error) {
    console.error("Error al procesar mensaje de alerta en la cola:", error);
  }
});

const connectToMqtt = async (topics, onMessage) => {
  try {
    const client = mqtt.connect(mqttUrl, options);

    client.on("connect", () => {
      console.log(`Conectado al broker MQTT.`);
      client.subscribe(topics, { qos: 1 }, (err) => {
        if (err) {
          console.error(`Error al suscribirse a los temas:`, err);
        } else {
          console.log(`Suscrito a los temas: ${topics.join(", ")}`);
        }
      });
    });

    client.on("message", onMessage);

    client.on("error", (error) => {
      console.error("Error en el consumidor:", error);
    });
  } catch (error) {
    console.error("Error en el consumidor MQTT:", error);
  }
};

const handleSensorMessage = (topic, message) => {
  sensorQueue.add({ topic, message: message.toString() });
};

const handleSensorAlert = (topic, message) => {
  alertQueue.add({ topic, message: message.toString() });
};

const main = async () => {
  await connectToMqtt([topicSensor, topicAlert], (topic, message) => {
    if (topic === topicSensor) {
      handleSensorMessage(topic, message);
    } else if (topic === topicAlert) {
      handleSensorAlert(topic, message);
    }
  });
};

main();
