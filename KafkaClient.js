const { Kafka } = require('kafkajs');
const { service: CascadeService } = require('kafka-cascade');
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['42.192.41.175:9092'] // 替换为您的 Kafka 集群地址
});

function serviceCallback(message, resolve, reject) {
    try {
      // 在这里处理消息
      console.log(`Received message: ${message.value.toString()}`);
      // 调用 resolve 表示成功处理
      resolve();
    } catch (error) {
      // 调用 reject 表示处理失败
      reject();
    }
  }
  
  function successCallback(message) {
    console.log(`Message successfully processed: ${message.value.toString()}`);
  }
  
  function dlqCallback(message) {
    console.error(`Message sent to DLQ: ${message.value.toString()}`);
  }

  
  async function main() {
    const topic = 'notification';    // 替换为您想监听的 Kafka 主题
    const groupId = 'my-group';  // 替换为您的消费者组 ID
  
    try {
      const cascadeService = await CascadeService(kafka, topic, groupId, serviceCallback, successCallback, dlqCallback);
      
      await cascadeService.connect();
      console.log('Connected to Kafka and running service.');
      await cascadeService.run();
    } catch (error) {
      console.error('Error setting up Kafka Cascade service:', error);
    }
  }
  
  main().catch(console.error);
  