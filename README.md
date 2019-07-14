# mqtt
 
```
@Bean
public MqttListenerAnnotationBeanPostProcessor mqttListenerAnnotationBeanPostProcessor(
  MqttPahoMessageDrivenChannelAdapter mqttAdapter, ReactiveMongoTemplate reactiveMongoTemplate) {
 return new MqttListenerAnnotationBeanPostProcessor(mqttAdapter);
}
```

```
@Bean
public MqttPahoMessageDrivenChannelAdapter mqttAdapter() {
 MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
 mqttConnectOptions.setUserName(userName);
 mqttConnectOptions.setPassword(password.toCharArray());
 mqttConnectOptions.setCleanSession(cleanSession);

 DefaultMqttPahoClientFactory clientFactory = new DefaultMqttPahoClientFactory();
 clientFactory.setConnectionOptions(mqttConnectOptions);

 MqttPahoMessageDrivenChannelAdapter adapter = new MqttPahoMessageDrivenChannelAdapter(url, clientId,
   clientFactory);
 adapter.setCompletionTimeout(5000);
 adapter.setConverter(new DefaultPahoMessageConverter());
 adapter.setQos(1);

 return adapter;
}
```

```
@MqttListener(topics = "${mqtt.topic.prefix}/+/register/#", qos = 1)
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
