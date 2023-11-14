const { Kafka } = require("kafkajs");
//import AsyncRetryHelper, { RetryTopicNaming } from "kafkajs-async-retry";

const AsyncRetryHelperLib = require("kafkajs-async-retry");
const AsyncRetryHelper = AsyncRetryHelperLib.default;
const { RetryTopicNaming } = AsyncRetryHelperLib;


// const { RetryTopicNaming } = require("kafkajs-async-retry");

async function main() {
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["42.192.41.175:9092","42.192.41.175:9093","42.192.41.175:9094"],
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: "notification" });

  const asyncRetryHelper = new AsyncRetryHelper({
    producer,
    groupId: "notification",
    retryTopicNaming: RetryTopicNaming.ATTEMPT_BASED,
    retryDelays: [5, 5, 5],
    maxRetries: 3,
  });

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topics: ["notification"], fromBeginning: true });

  // Ensure that retry topic pattern is subscribed properly
  // This should be a string or regex pattern, not an array
  await consumer.subscribe({
    topic: asyncRetryHelper.retryTopicPattern,
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: asyncRetryHelper.eachMessage(
      async ({ topic, originalTopic, message, previousAttempts }) => {
        if (previousAttempts > 0) {
          console.log(`重试 Retrying message from topic ${originalTopic}`);
        }
        // do something with the message (exceptions will be caught and the
        // message will be sent to the appropriate retry or dead-letter topic)
        processMessage(message);
      }
    ),
  });

  asyncRetryHelper.on("retry", ({ message, error }) => {
    console.log(`重试的回调逻辑log`);
    // log info about message/error here
  });

  asyncRetryHelper.on("dead-letter", ({ message, error }) => {
    console.log(`死信的回调逻辑`);
    // log info about message/error here
  });
  
  // Define your processMessage function or make sure it exists
  function processMessage(message) {
    console.log("消息处理逻辑:");
    throw new Error("抛出错误 Error processing message");
    // Your message processing logic here
  }
}

main().catch(console.error);
