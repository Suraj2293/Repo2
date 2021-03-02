package net.paladion.model;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import lombok.Getter;
import lombok.Setter;

public class KafkaProducerSingleton {

	private static KafkaProducerSingleton INSTANCE = null;
	@Setter
	@Getter
	private static Producer<Integer, String> producer;

	@Setter
	@Getter
	private static String topic;

	@Setter
	@Getter
	private static Properties producerProps;

	private KafkaProducerSingleton(Properties prop) {
		setKafkaProperties(prop);
		ProducerConfig producerConfig = new ProducerConfig(producerProps);
		producer = new Producer<Integer, String>(producerConfig);

	}

	public static KafkaProducerSingleton getInstance(Properties props) {
		if (INSTANCE == null) {
			// Thread Safe. Might be costly operation in some case
			synchronized (KafkaProducerSingleton.class) {
				if (INSTANCE == null) {
					INSTANCE = new KafkaProducerSingleton(props);
				}
			}
		}
		return INSTANCE;
	}

	private static void setKafkaProperties(Properties props) {
		producerProps = new Properties();

		if (!StringUtils.isEmpty(props.getProperty("kafka.broker.send.list"))) {
			producerProps.put("metadata.broker.list",
					props.getProperty("kafka.broker.send.list"));
		}

		if (!StringUtils.isEmpty(props.getProperty("kafka.serializer.class"))) {
			producerProps.put("serializer.class",
					props.getProperty("kafka.serializer.class"));
		}

		if (!StringUtils.isEmpty(props
				.getProperty("kafka.request.required.acks"))) {
			producerProps.put("request.required.acks",
					props.getProperty("kafka.request.required.acks"));
		}

		if (!StringUtils.isEmpty(props.getProperty("kafka.retries"))) {
			int retriesValue = Integer.parseInt(props
					.getProperty("kafka.retries"));
			producerProps.put("retries", retriesValue);
		}

		if (!StringUtils.isEmpty(props.getProperty("kafka.batch.size"))) {
			int sizeValue = Integer.parseInt(props
					.getProperty("kafka.batch.size"));
			producerProps.put("batch.size", sizeValue);
		}

		if (!StringUtils.isEmpty(props.getProperty("kafka.linger.ms"))) {
			long lingerValue = Long.parseLong(props
					.getProperty("kafka.linger.ms"));
			producerProps.put("linger.ms", lingerValue);
		}

		if (!StringUtils.isEmpty(props.getProperty("kafka.buffer.memory"))) {
			long memoryValue = Long.parseLong(props
					.getProperty("kafka.buffer.memory"));
			producerProps.put("buffer.memory", memoryValue);
		}

		if (!StringUtils.isEmpty(props.getProperty("kafka.key.serializer"))) {
			producerProps.put("key.serializer",
					props.getProperty("kafka.key.serializer"));
		}

		if (!StringUtils.isEmpty(props.getProperty("kafka.value.serializer"))) {
			producerProps.put("value.serializer",
					props.getProperty("kafka.value.serializer"));
		}

		if (!StringUtils.isEmpty(props.getProperty("kafka.compression.type"))) {
			producerProps.put("compression.type",
					props.getProperty("kafka.compression.type"));
		}
	}

	@SuppressWarnings("static-access")
	public void send1(String topic, String data) {
		KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(
				topic, data);
		INSTANCE.getProducer().send(keyedMsg);
	}
}
