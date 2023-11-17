const { Kafka } = require("kafkajs");

const AsyncRetryHelperLib = require("kafkajs-async-retry");
const AsyncRetryHelper = AsyncRetryHelperLib.default;
const { RetryTopicNaming } = AsyncRetryHelperLib;

async function main() {
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["42.192.41.175:9092"],
  });

  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: "notification" });

  const asyncRetryHelper = new AsyncRetryHelper({
    producer,
    groupId: "notification",
    retryTopicNaming: RetryTopicNaming.ATTEMPT_BASED,
    maxWaitTime: 100000,
    retryDelays: [2, 2, 2],
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
          console.log(`Retrying message from topic ${originalTopic} ${previousAttempts}`);
        }

        // do something with the message (exceptions will be caught and the
        // message will be sent to the appropriate retry or dead-letter topic)
        processMessage(message);
      }
    ),
  });

  // await consumer.run({
  //   eachBatch: asyncRetryHelper.eachBatch(
  //     async ({
  //       batch,
  //       asyncRetryMessageDetails,
  //       messageFailureHandler
  //     }) => {
  //       for (let message of batch.messages) {
  //         try {
  //           const details = asyncRetryMessageDetails(message);
  //           if (details.previousAttempts > 0) {
  //             console.log(`Retrying message from topic ${details.originalTopic} ${details.previousAttempts}`);
  //           }
  
  //           // 处理消息的逻辑
  //           processMessage(message);
  
  //         } catch (error) {
  //           // 如果在处理消息时发生错误，使用 messageFailureHandler 来处理失败的消息
  //           await messageFailureHandler(error, message);
  //         }
  //       }
  //     }
  //   ),
  // });

  asyncRetryHelper.on("retry", ({ message, error }) => {
    console.log(`retry call back`);
  });

  asyncRetryHelper.on("dead-letter", ({ message, error }) => {
    console.log(`dead-letter call back`);
  });
  
  // Define your processMessage function or make sure it exists
  function processMessage(message) {
    console.log("error in processing");
    //simulate the error
    throw new Error("Error processing message");
  }
}

main().catch(console.error);
