const { Kafka } = require('kafkajs');
const { service: CascadeService } = require('kafka-cascade');
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['42.192.41.175:9092','42.192.41.175:9093','42.192.41.175:9094'] // 替换为您的 Kafka 集群地址
});

function serviceCallback(message, resolve, reject) {
    try {
      // 在这里处理消息
      console.log(`接受到消息Received message: ${JSON.stringify(message)}`);
      throw new Error('Error processing message');
      // 调用 resolve 表示成功处理
      resolve();
    } catch (error) {
      // 调用 reject 表示处理失败
      console.log(`错误发生啦`, error);
      reject(error);
    }
  }
  
  function successCallback(message) {
    console.log(`成功处理 Message successfully processed: ${message}`);
  }
  
  function dlqCallback(message) {
    console.error(`Message sent to DLQ: ${message}`);
  }

  
  async function main() {
    const topic = 'notification';    // 替换为您想监听的 Kafka 主题
    const groupId = 'my-group';  // 替换为您的消费者组 ID
  
    try {
      const cascadeService = await CascadeService(kafka, topic, groupId, serviceCallback, successCallback, dlqCallback);
      
      await cascadeService.setDefaultRoute(3, {
        timeoutLimit: [1000, 3000, 5000], // 依次延迟1秒，3秒，5秒进行重试
      }).then(() => {
        console.log('Default retry route has been set.');
      }).catch(error => {
        console.error('Failed to set default retry route:', error);
      });

      cascadeService.on('retry', (message) => {
        console.log('重试的逻辑:', message);
        // 在这里可以添加额外的重试逻辑
      });

      await cascadeService.connect();
      console.log('Connected to Kafka and running service.');
      await cascadeService.run();
    } catch (error) {
      console.error('Error setting up Kafka Cascade service:', error);
    }
  }
  
  main().catch(console.error);
  