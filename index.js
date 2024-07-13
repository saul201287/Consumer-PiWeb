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

if (!options.username || !options.password || !mqttUrl || !topicPago || !topicSensor) {
  throw new Error("Las variables de entorno no están definidas correctamente.");
}

const pagos = async () => {
  try {
    const client = mqtt.connect(mqttUrl, options);

    client.on("connect", () => {
      console.log("Conectado al broker MQTT");
      client.subscribe(topicPago, { qos: 1 }, (err) => {
        if (err) {
          console.error("Error al suscribirse al tema:", err);
        } else {
          console.log("Suscrito al tema:", topicPago);
        }
      });
    });

    client.on("message", (topicPago, message) => {
      console.log(" [x] Recibido '%s'", message.toString());
    });

    client.on("error", (error) => {
      console.error("Error en el consumidor:", error);
    });
  } catch (error) {
    console.error("Error en el consumidor:", error);
  }
};

const datosSensor = async () => {
  try {
    const client = mqtt.connect(mqttUrl, options);

    client.on("connect", () => {
      console.log("Conectado al broker MQTT");
      client.subscribe(topicSensor, { qos: 1 }, (err) => {
        if (err) {
          console.error("Error al suscribirse al tema:", err);
        } else {
          console.log("Suscrito al tema:", topicSensor);
        }
      });
    });

    client.on("message", (topicSensor, message) => {
      console.log(" [x] Recibido '%s'", JSON.parse(message));
    });

    client.on("error", (error) => {
      console.error("Error en el consumidor:", error);
    });
  } catch (error) {
    console.error("Error en el consumidor:", error);
  }
};


pagos();
datosSensor();
