const { default: axios } = require("axios");
const mqtt = require("mqtt");
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
  !topicSensor
) {
  throw new Error("Las variables de entorno no están definidas correctamente.");
}

const connectToMqtt = async (topic, onMessage) => {
  try {
    const client = mqtt.connect(mqttUrl, options);

    client.on("connect", () => {
      console.log(`Conectado al broker MQTT. Suscrito al tema: ${topic}`);
      client.subscribe(topic, { qos: 1 }, (err) => {
        if (err) {
          console.error(`Error al suscribirse al tema ${topic}:`, err);
        }
      });
    });

    client.on("message", onMessage);

    client.on("error", (error) => {
      console.error("Error en el consumidor:", error);
    });
  } catch (error) {
    console.error("Error en el consumidor:", error);
  }
};

/*const handlePagoMessage = async (topic, message) => {
  try {
    console.log(`Mensaje recibido en ${topic}: ${message.toString()}`);
    await axios.post(
      `${process.env.URL_API}/notification/alert`,
      JSON.parse(message.toString())
    );
  } catch (error) {
    console.error("Error al procesar el mensaje de pago:", error);
  }
};*/

const handleSensorMessage = async (topic, message) => {
  try {
    const parsedMessage = JSON.parse(message.toString());
    await axios.post(`${process.env.URL_API}/notification/data`, parsedMessage);
    console.log(
      `Datos de sensor recibidos y enviados a la API: ${parsedMessage}`
    );
  } catch (error) {
    console.error("Error al procesar el mensaje del sensor:", error);
  }
};
const handleSensorAlert = async (topic, message) => {
  try {
    const parsedMessage = JSON.parse(message.toString());
    await axios.post(`${process.env.URL_API}/notification/alert`, parsedMessage);
    console.log(
      `Datos de sensor recibidos y enviados a la API: ${parsedMessage}`
    );
  } catch (error) {
    console.error("Error al procesar el mensaje del sensor:", error);
  }
};

const main = async () => {
  //await connectToMqtt(topicPago, handlePagoMessage);
  await connectToMqtt(topicSensor, handleSensorMessage);
  await connectToMqtt(topicAlert, handleSensorAlert);
};

main();
