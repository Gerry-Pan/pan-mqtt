package cn.com.pan.mqtt.config;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.springframework.util.Assert;

import lombok.Getter;
import lombok.Setter;

public class MethodMqttListenerEndpoint implements MqttListenerEndpoint {

	@Setter
	@Getter
	private Object bean;

	@Setter
	@Getter
	private Method method;

	private final Collection<String> topics = new ArrayList<>();

	@Override
	public Collection<String> getTopics() {
		return Collections.unmodifiableCollection(this.topics);
	}

	public void setTopics(String... topics) {
		Assert.notNull(topics, "'topics' must not be null");
		this.topics.clear();
		this.topics.addAll(Arrays.asList(topics));
	}

}
