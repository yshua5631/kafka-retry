# kafka-retry
## Prerequisite
 * any kafka client

## Test for kafka-async-retry
  * node ./kafkajs-async-retry.js
  * test result with config retryDelays: [1, 2, 3]
  ![Alt](./retry1.jpg)
  * test result with config retryDelays: [2, 4, 6], it does not work well
  ![Alt](./retry2.jpg)
## Test for kafka-cascade
  *  node ./cascade.js
  *  test result
  ![Alt](./cascade.jpg)
