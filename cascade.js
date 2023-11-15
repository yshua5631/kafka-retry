const { Kafka } = require('kafkajs');
const { service: CascadeService, producer: CascadeProducer } = require('kafka-cascade');
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['42.192.41.175:9092','42.192.41.175:9093','42.192.41.175:9094'] // 替换为您的 Kafka 集群地址
});

function serviceCallback(message, resolve, reject) {
    try {
      // 在这里处理消息
      console.log(`Received message: ${message}`);
      // 调用 resolve 表示成功处理
      reject(message);
      //resolve();
    } catch (error) {
      // 调用 reject 表示处理失败
      console.log(`error happens`, error);
      //reject(error);
    }
  }
  
  function successCallback(message) {
    console.log(`Message successfully processed: ${message}`);
  }
  
  function dlqCallback(message) {
    console.error(`Message sent to DLQ: ${message}`);
  }

  
  async function main() {
    const topic = 'notification';  
    const groupId = 'my-group';  
  
    try {
      const cascadeService = await CascadeService(kafka, topic, groupId, serviceCallback, successCallback, dlqCallback);
      
      await cascadeService.setDefaultRoute(3, {
        timeoutLimit: [10000, 20000, 30000], 
      }).then(() => {
        console.log('Default retry route has been set.');
      }).catch(error => {
        console.error('Failed to set default retry route:', error);
      });

      cascadeService.on('retry', (message) => {
        console.log('retry callback:', message);
        // 在这里可以添加额外的重试逻辑
      });

      cascadeService.on('dlq', (message) => {
        console.error(`DLQ callback: ${message}`);
      });

      await cascadeService.connect();
      console.log('Connected to Kafka and running service.');
      await cascadeService.run();
    } catch (error) {
      console.error('Error setting up Kafka Cascade service:', error);
    }
  }
  
  main().catch(console.error);
  