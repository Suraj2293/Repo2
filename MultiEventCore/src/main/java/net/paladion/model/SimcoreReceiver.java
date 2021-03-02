package net.paladion.model;

import java.io.Serializable;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public interface SimcoreReceiver<T> extends Serializable {

  public JavaInputDStream<ConsumerRecord<String, String>> getStreamData(JavaStreamingContext javaStreamingContext, List<KafkaDTO> resultSet);

}
