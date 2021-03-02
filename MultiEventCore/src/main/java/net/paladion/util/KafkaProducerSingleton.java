package net.paladion.util;

import java.util.Properties;

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

		producerProps.put("metadata.broker.list",
				props.getProperty("kafka.broker.send.list"));
		producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
		producerProps.put("request.required.acks", "1");
		// topic = props.getProperty("kafka.send.topic");

	}

	@SuppressWarnings("static-access")
	public void send1(String topic, String data) {
		KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(
				topic, data);
		INSTANCE.getProducer().send(keyedMsg);
	}

}
