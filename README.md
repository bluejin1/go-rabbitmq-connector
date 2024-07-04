# RabbitMq Connector

```azure
- RabbitMQ Connect을 위한 git submodule
- 일대일, 일대다, 구독 처리를 위한 기능 지원
```

### Connect Adaptor
- AMQP
- TLS 지원
- Direct Queue
- Workgroup Topic
- Exchange Topic

***
### 사용 예제
- rabbitmq_message.go
***

### 변경사항
```azure
- 커넥션이 끈어진 경우 재연결 기능 추가
- 사용모듈 간소화
    
```

### 사용 예제

    
````    
    - test 폴더 test code 참조
    - Work Queue 방식
    - consumer 경우 - 1단계 호출
        RunConsumer()
    - Publisher : 2단계 호출
        1.SetPublisher()
        2. RunPublishMessage()
    
	
````
