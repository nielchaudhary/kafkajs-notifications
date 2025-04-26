import express from "express";
import { Kafka } from "kafkajs";

const app = express();

const kafka = new Kafka({
  clientId: "notification-system",
  brokers: ["localhost:9092"],
});

const admin = kafka.admin();
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "notification-group" });

const TOPIC_NAME = "notifications";
const NOTIFICATION_INTERVAL = 3000;

export const runNotificationSystem = async () => {
  try {
    await admin.connect();

    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes(TOPIC_NAME)) {
      await admin.createTopics({
        topics: [
          {
            topic: TOPIC_NAME,
            numPartitions: 1,
            replicationFactor: 1,
          },
        ],
      });
      console.log(`Topic created: ${TOPIC_NAME}`);
    }

    await producer.connect();
    console.log("Producer connected");

    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
    console.log("Consumer subscribed");

    const producerInterval = setInterval(async () => {
      try {
        const timestamp = new Date().toISOString();
        const message = `Notification at ${timestamp}`;

        await producer.send({
          topic: TOPIC_NAME,
          messages: [
            {
              value: message,
              timestamp: Date.now().toString(),
            },
          ],
        });

        console.log(`Sent: ${message}`);
      } catch (error) {
        console.error("Error sending message:", error);
      }
    }, NOTIFICATION_INTERVAL);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          receivedAt: new Date().toISOString(),
          topic,
          partition,
          offset: message.offset,
          timestamp: message.timestamp,
          value: message.value?.toString(),
        });
      },
    });

    const shutdown = async () => {
      clearInterval(producerInterval);
      await consumer.disconnect();
      await producer.disconnect();
      await admin.disconnect();
      console.log("Disconnected all Kafka clients");
      process.exit(0);
    };

    process.on("SIGTERM", shutdown);
    process.on("SIGINT", shutdown);
  } catch (error) {
    console.error("Error in notification system:", error);
    process.exit(1);
  }
};

runNotificationSystem();

app.listen(8090, () => console.log("Server is running on port 8090"));
