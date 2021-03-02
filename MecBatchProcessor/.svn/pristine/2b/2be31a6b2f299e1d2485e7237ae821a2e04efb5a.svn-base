package net.paladion.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import lombok.Getter;
import lombok.Setter;
import net.paladion.model.MultiEventCoreDTO;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class KafkaProducer implements Serializable {

	public KafkaProducer(Properties props) {
		super();
		setKafkaProperties(props);
		init();

	}

	/**
   * 
   */
	private static final long serialVersionUID = 1L;

	@Setter
	@Getter
	private Producer<Integer, String> producer;

	@Setter
	@Getter
	private String topic;

	@Setter
	@Getter
	private Properties producerProps;

	/*
	 * @Getter private final ObjectMapper jsonConv = new ObjectMapper();
	 */

	public void init() {

		ProducerConfig producerConfig = new ProducerConfig(producerProps);
		producer = new Producer<Integer, String>(producerConfig);
	}

	public void send(MultiEventCoreDTO msg) {
		String jsonMsg = null;
		try {
			ObjectMapper jsonConv = new ObjectMapper();

			jsonMsg = jsonConv.writeValueAsString(msg);
			KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(
					topic, jsonMsg);
			producer.send(keyedMsg);

		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void setKafkaProperties(Properties props) {
		this.producerProps = new Properties();

		producerProps.put("metadata.broker.list",
				props.getProperty("kafka.broker.send.list"));
		producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
		producerProps.put("request.required.acks", "1");
		this.topic = props.getProperty("kafka.send.topic");

	}

}
