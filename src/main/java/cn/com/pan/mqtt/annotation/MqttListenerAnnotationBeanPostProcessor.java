package cn.com.pan.mqtt.annotation;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.Scope;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import cn.com.pan.mqtt.config.MethodMqttListenerEndpoint;
import lombok.Getter;

public class MqttListenerAnnotationBeanPostProcessor
		implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton {

	protected final Logger log = LogManager.getLogger(getClass());

	private BeanFactory beanFactory;

	private BeanExpressionContext expressionContext;

	private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();

	private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap<>(64));

	private final List<Topic> allTopicList = new ArrayList<Topic>();

	private final List<MethodMqttListenerEndpoint> endpointList = new ArrayList<MethodMqttListenerEndpoint>();

	private final MqttPahoMessageDrivenChannelAdapter adapter;

	private final static AntPathMatcher matcher = new AntPathMatcher();

	public MqttListenerAnnotationBeanPostProcessor(MqttPahoMessageDrivenChannelAdapter adapter) {
		Assert.notNull(adapter, "adapter must not be null.");
		this.adapter = adapter;
	}

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
			Class<?> targetClass = AopUtils.getTargetClass(bean);

			Map<Method, Set<MqttListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
					new MethodIntrospector.MetadataLookup<Set<MqttListener>>() {

						@Override
						public Set<MqttListener> inspect(Method method) {
							Set<MqttListener> listenerMethods = findListenerAnnotations(method);
							return (!listenerMethods.isEmpty() ? listenerMethods : null);
						}

					});

			if (annotatedMethods.isEmpty()) {
				this.nonAnnotatedClasses.add(bean.getClass());
			} else {
				for (Map.Entry<Method, Set<MqttListener>> entry : annotatedMethods.entrySet()) {
					Method method = entry.getKey();
					for (MqttListener listener : entry.getValue()) {
						Method methodToUse = checkProxy(method, bean);
						MethodMqttListenerEndpoint endpoint = null;

						List<Topic> topicList = resolveTopicList(listener);
						List<String> tempTopics = new ArrayList<String>();

						for (Topic t : topicList) {
							if (allTopicList.contains(t)) {
								log.warn("The topic '" + t.getTopic() + "' is duplicate.");
								continue;
							}
							tempTopics.add(t.getTopic());
							allTopicList.add(t);
						}

						String[] topics = tempTopics.toArray(new String[tempTopics.size()]);

						if (topics != null && topics.length > 0) {
							if (endpoint == null) {
								endpoint = new MethodMqttListenerEndpoint();
							}

							endpoint.setTopics(topics);
						}

						if (endpoint != null) {
							endpoint.setBean(bean);
							endpoint.setMethod(methodToUse);

							endpointList.add(endpoint);
						}
					}
				}
			}
		}

		return bean;
	}

	@Override
	public void afterSingletonsInstantiated() {
		DirectChannel mqttInputChannel = new DirectChannel();

		String[] topics = new String[allTopicList.size()];
		int[] qos = new int[allTopicList.size()];

		int i = 0;
		for (Topic t : allTopicList) {
			topics[i] = t.getTopic();
			qos[i] = t.getQos();
			i++;
		}

		adapter.addTopics(topics, qos);
		adapter.setOutputChannel(mqttInputChannel);

		mqttInputChannel.subscribe(message -> {
			MethodMqttListenerEndpoint endpoint = handleMapping(message);

			if (endpoint == null || endpoint.getMethod() == null || endpoint.getBean() == null) {
				throw new MessagingException("No MethodMqttListenerEndpoint");
			}

			Method method = endpoint.getMethod();

			List<Object> args = new LinkedList<Object>();
			Class<?>[] clazzList = method.getParameterTypes();

			for (Class<?> clazz : clazzList) {
				if (clazz.equals(Message.class)) {
					args.add(message);
				} else {
					args.add(null);
				}
			}

			ReflectionUtils.invokeMethod(endpoint.getMethod(), endpoint.getBean(),
					args.toArray(new Object[args.size()]));
		});
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory,
					new ListenerScope());
		}
	}

	protected MethodMqttListenerEndpoint handleMapping(Message<?> message) {
		try {
			MessageHeaders headers = message.getHeaders();
			String topic = headers.get("mqtt_receivedTopic", String.class);

			for (MethodMqttListenerEndpoint endpoint : endpointList) {
				boolean isMatch = false;

				Collection<String> topics = endpoint.getTopics();
				if (topics.contains(topic)) {
					isMatch = true;
				}

				if (!isMatch) {
					for (String t : topics) {
						String regex = t.replace("/+/", "/*/");

						if (regex.endsWith("/+")) {
							regex = regex.replace("/+", "/*");
						}

						if (regex.endsWith("/#")) {
							regex = regex.replace("/#", "/**");
						}

						isMatch = matcher.match(regex, topic);

						if (isMatch) {
							break;
						}
					}
				}

				if (isMatch) {
					return endpoint;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	private Set<MqttListener> findListenerAnnotations(Method method) {
		Set<MqttListener> listeners = new HashSet<>();
		MqttListener ann = AnnotatedElementUtils.findMergedAnnotation(method, MqttListener.class);
		if (ann != null) {
			listeners.add(ann);
		}
		return listeners;
	}

	private Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
				Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
				for (Class<?> iface : proxiedInterfaces) {
					try {
						method = iface.getMethod(method.getName(), method.getParameterTypes());
						break;
					} catch (NoSuchMethodException noMethod) {
					}
				}
			} catch (SecurityException ex) {
				ReflectionUtils.handleReflectionException(ex);
			} catch (NoSuchMethodException ex) {
				throw new IllegalStateException(String.format(
						"@MqttListener method '%s' found on bean target class '%s', "
								+ "but not found in any interface(s) for bean JDK proxy. Either "
								+ "pull the method up to an interface or switch to subclass (CGLIB) "
								+ "proxies by setting proxy-target-class/proxyTargetClass " + "attribute to 'true'",
						method.getName(), method.getDeclaringClass().getSimpleName()), ex);
			}
		}
		return method;
	}

	@SuppressWarnings("unchecked")
	protected String[] resolveTopics(MqttListener kafkaListener) {
		List<String> topics = null;
		String beanRef = kafkaListener.beanRef();

		if (StringUtils.hasText(beanRef)) {
			Object r = resolveExpression(beanRef);

			if (r instanceof String[]) {
				topics = Arrays.asList((String[]) resolveExpression(beanRef));
			} else if (r instanceof List) {
				topics = (List<String>) resolveExpression(beanRef);
			}

		}

		if (topics == null) {
			topics = Arrays.asList(kafkaListener.topics());
		}

		List<String> result = new ArrayList<>();
		if (topics.size() > 0) {
			for (int i = 0; i < topics.size(); i++) {
				Object topic = resolveExpression(topics.get(i));
				resolveAsString(topic, result);
			}
		}
		return result.toArray(new String[result.size()]);
	}

	@SuppressWarnings("unchecked")
	protected List<Topic> resolveTopicList(MqttListener mqttListener) {
		int qos = mqttListener.qos();
		List<String> topics = null;
		List<Topic> topicList = new ArrayList<Topic>();
		String beanRef = mqttListener.beanRef();

		if (StringUtils.hasText(beanRef)) {
			Object r = resolveExpression(beanRef);

			if (r instanceof String[]) {
				topics = Arrays.asList((String[]) resolveExpression(beanRef));
			} else if (r instanceof List) {
				topics = (List<String>) resolveExpression(beanRef);
			}

		}

		if (topics == null) {
			topics = Arrays.asList(mqttListener.topics());
		}

		List<String> result = new ArrayList<>();
		if (topics.size() > 0) {
			for (int i = 0; i < topics.size(); i++) {
				Object topic = resolveExpression(topics.get(i));
				resolveAsString(topic, result);
			}
		}

		for (String topic : result) {
			Topic t = new Topic(topic, qos);
			topicList.add(t);
		}

		return topicList;
	}

	@SuppressWarnings("unchecked")
	protected void resolveAsString(Object resolvedValue, List<String> result) {
		if (resolvedValue instanceof String[]) {
			for (Object object : (String[]) resolvedValue) {
				resolveAsString(object, result);
			}
		} else if (resolvedValue instanceof String) {
			result.add((String) resolvedValue);
		} else if (resolvedValue instanceof Iterable) {
			for (Object object : (Iterable<Object>) resolvedValue) {
				resolveAsString(object, result);
			}
		} else {
			throw new IllegalArgumentException(
					String.format("@KafKaListener can't resolve '%s' as a String", resolvedValue));
		}
	}

	private Object resolveExpression(String value) {
		return this.resolver.evaluate(resolve(value), this.expressionContext);
	}

	private String resolve(String value) {
		if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

	protected static final class Topic {

		@Getter
		private final String topic;

		@Getter
		private volatile int qos;

		Topic(String topic, int qos) {
			this.topic = topic;
			this.qos = qos;
		}

		@Override
		public int hashCode() {
			return this.topic.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Topic other = (Topic) obj;
			if (this.topic == null) {
				if (other.topic != null) {
					return false;
				}
			} else if (!this.topic.equals(other.topic)) {
				return false;
			}
			return true;
		}

	}

	protected static class ListenerScope implements Scope {

		private final Map<String, Object> listeners = new HashMap<>();

		ListenerScope() {
			super();
		}

		public void addListener(String key, Object bean) {
			this.listeners.put(key, bean);
		}

		public void removeListener(String key) {
			this.listeners.remove(key);
		}

		@Override
		public Object get(String name, ObjectFactory<?> objectFactory) {
			return this.listeners.get(name);
		}

		@Override
		public Object remove(String name) {
			return null;
		}

		@Override
		public void registerDestructionCallback(String name, Runnable callback) {
		}

		@Override
		public Object resolveContextualObject(String key) {
			return this.listeners.get(key);
		}

		@Override
		public String getConversationId() {
			return null;
		}

	}

}
