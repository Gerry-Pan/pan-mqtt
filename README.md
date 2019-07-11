# pan-mqtt
 
```
@Bean
public MqttListenerAnnotationBeanPostProcessor mqttListenerAnnotationBeanPostProcessor(
  MqttPahoMessageDrivenChannelAdapter mqttAdapter, ReactiveMongoTemplate reactiveMongoTemplate) {
 return new MqttListenerAnnotationBeanPostProcessor(mqttAdapter);
}
```

```
@MqttListener(topics = "${mqtt.topic.prefix}/register", qos = 1)
public void handle(Message<String> message) {
 log.info(message);
}
```

```
@MqttListener(beanRef = "#{mqttConfiguration.topicList}", qos = 1)
 public void handle(Message<?> message) {
 log.info(message);
}
```
