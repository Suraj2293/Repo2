package net.paladion.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.Getter;
import lombok.Setter;
import net.paladion.dao.KafkaOffsetDaoImpl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class KafkaThreatSimcoreReceiver implements SimcoreReceiver<String> {

	/**
   * 
   */
	private static final long serialVersionUID = 1L;
	@Getter
	@Setter
	private String brokers;
	@Getter
	@Setter
	private String topics;
	@Getter
	@Setter
	private Properties clientProp;

	public KafkaThreatSimcoreReceiver(Properties clientProp) {
		super();
		this.clientProp = clientProp;
	}
	
	/**
	 * 
	 */
	@Override
	public JavaInputDStream<ConsumerRecord<String, String>> getStreamData(
			JavaStreamingContext javaStreamingContext,
			List<KafkaDTO> selectOffsetsFromYourDatabase) {

		List<String> topicsSet = new ArrayList<String>(Arrays.asList(topics
				.split(",")));
		Map<TopicPartition, Long> fromOffsets = new HashMap<>();
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("enable.auto.commit", false);
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("group.id", "sparkTest");
		if (!selectOffsetsFromYourDatabase.isEmpty()) {
			for (KafkaDTO resultSet : selectOffsetsFromYourDatabase) {
				fromOffsets.put(new TopicPartition(resultSet.getTopic(),
						resultSet.getPartition()), resultSet.getOffsets());
			}
		} else {
			for (String topic : topicsSet) {
				KafkaOffsetDaoImpl kafkaOffsetInsert = new KafkaOffsetDaoImpl(
						clientProp);
				Integer numberofPartitions = kafkaOffsetInsert
						.getPartitions(topic);
				// log.info("The number of partiions are :" +
				// numberofPartitions);

				for (int i = 0; i < numberofPartitions; i++) {
					fromOffsets.put(new TopicPartition(topic, i), (long) 0);
				}
			}
		}
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils
				.createDirectStream(
						javaStreamingContext,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String> Assign(
								fromOffsets.keySet(), kafkaParams, fromOffsets));

		return messages;
	}

}
