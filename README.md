# kafka-retry
## Prerequisite
 * any kafka client

## Test for kafka-async-retry
  * node ./kafkajs-async-retry.js
  * test result with config retryDelays: [1, 2, 3]
  ![Alt](./retry1.jpg)
## Test for kafka-cascade
  *  node ./cascade.js
  *  test result
  ![Alt](./cascade.jpg)

## Diagram
```mermaid
sequenceDiagram
    actor C as Client
    participant B as BFF
    participant St as student-self-study
    C->>+B: Load Self Study
    Note over B,St: Get current course, level, unit, lesson path
    B->>+St: GET /v2/students/{studentId}/focus
    St-->>-B: 200 Success
    Note over B,St: Get level content and progress based on focus
    B->>+St: /v2/students/{studentId}/progress/courses/{courseNodeId}/node/{levelNodeId}?depth=2
    St-->>-B: 200 Success
    Note over B,St: Get progress test, if applicable
    B->>+St: /v2/students/{studentId}/progress/courses/{courseNodeId}/levelId/{levelNodeId}/progress-test
    alt level has a progress test
        St-->>B: 200 Success
    else level does not have a progress test
        St-->>-B: 404 not found
    end
B-->>-C: success
```
