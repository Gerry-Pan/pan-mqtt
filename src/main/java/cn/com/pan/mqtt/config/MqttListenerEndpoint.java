package cn.com.pan.mqtt.config;

import java.util.Collection;

public interface MqttListenerEndpoint {

	Collection<String> getTopics();

}
