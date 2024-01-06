const { Kafka } = require("kafkajs");

const AsyncRetryHelperLib = require("kafkajs-async-retry");
const AsyncRetryHelper = AsyncRetryHelperLib.default;
const { RetryTopicNaming } = AsyncRetryHelperLib;

async function main() {
  const sourceKafka = new Kafka({
    clientId: "source",
    brokers: ["42.192.41.175:9092"],
  });


  const destinationKafka = new Kafka({
    clientId: "destination",
    brokers: ["42.192.41.175:9092"],
  });

  const sourceConsumer = sourceKafka.consumer({ groupId: "source" });

  await sourceConsumer.connect();

  await sourceConsumer.subscribe({ topics: ["source"], fromBeginning: true });


  const destinationProducer = destinationKafka.producer();
  const destinationConsumer = destinationKafka.consumer({ groupId: "destination" });

  const asyncRetryHelper = new AsyncRetryHelper({
    producer: destinationProducer,
    groupId: "destination",
    retryTopicNaming: RetryTopicNaming.ATTEMPT_BASED,
    maxWaitTime: 100000,
    retryDelays: [2, 2, 2],
    maxRetries: 3,
  });

  await destinationConsumer.connect();
  await destinationProducer.connect();

  await destinationConsumer.subscribe({ topics: ["destination"], fromBeginning: true });

  // Ensure that retry topic pattern is subscribed properly
  // This should be a string or regex pattern, not an array
  await destinationConsumer.subscribe({
    topic: asyncRetryHelper.retryTopicPattern,
    fromBeginning: true,
  });

  // await destinationConsumer.run({
  //   eachMessage: asyncRetryHelper.eachMessage(
  //     async ({ topic, originalTopic, message, previousAttempts }) => {
  //       if (previousAttempts > 0) {
  //         console.log(`Retrying message from topic ${originalTopic} ${previousAttempts}`);
  //       }

  //       // do something with the message (exceptions will be caught and the
  //       // message will be sent to the appropriate retry or dead-letter topic)
  //       processMessage(message);
  //     }
  //   ),
  // });

  await sourceConsumer.run({
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      for (let message of batch.messages) {
        if (!isRunning() || isStale()) break;
  
        const payload = message.value.toString();
        console.log(`Received message from A: ${payload}`);
        console.log("转发吧");
        // TODO: 在这里你可以根据需要对消息进行加工或转换
  
        // 将消息转发到集群B的topic
        await destinationProducer.send({
          topic: 'destination',
          messages: [{ value: payload }],
        });
  
        // 手动解析消息，以确保每条消息处理后都能正确提交偏移量
        resolveOffset(message.offset);
        await heartbeat();
      }
    },
  });

  await destinationConsumer.run({
    eachBatch: asyncRetryHelper.eachBatch(
      async ({
        batch,
        asyncRetryMessageDetails,
        messageFailureHandler
      }) => {
        for (let message of batch.messages) {
          try {
            const details = asyncRetryMessageDetails(message);
            if (details.previousAttempts > 0) {
              console.log(`Retrying message from topic ${details.originalTopic} ${details.previousAttempts}`);
            }
  
            // 处理消息的逻辑
            processMessage(message);
  
          } catch (error) {
            console.log('错误发生');
            // 如果在处理消息时发生错误，使用 messageFailureHandler 来处理失败的消息
            await messageFailureHandler(error, message);
          }
        }
      }
    ),
  });

  asyncRetryHelper.on("retry", ({ message, error }) => {
    console.log(`retry call back`);
  });

  asyncRetryHelper.on("dead-letter", ({ message, error }) => {
    console.log(`dead-letter call back`);
  });
  
  // Define your processMessage function or make sure it exists
  function processMessage(message) {
    console.log("错误 error in processing");
    //simulate the error
    throw new Error("Error processing message");
  }
}

main().catch(console.error);
