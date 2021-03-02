package net.paladion.listener;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.KafkaOffsetDaoImpl;
import net.paladion.model.KafkaDTO;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.SimcoreReceiver;
import net.paladion.steps.AlertGenerationStep;
import net.paladion.steps.DelayLogger;
import net.paladion.steps.DroolsRuleInsertStep;
import net.paladion.steps.FinalInsertStep;
import net.paladion.steps.MultiEventCoreFirstStep;
import net.paladion.steps.RawDataInsertStep;
import net.paladion.steps.RuleExecuterStep;
import net.paladion.steps.ThresholdExecuterStep;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.springframework.stereotype.Component;

import com.paladion.racommon.data.BaseDataCarrier;
import com.paladion.racommon.data.DataCarrier;

@Component
@Slf4j
public class MultiEventCoreSparkListener {
	@Getter
	@Setter
	private JavaStreamingContext javaStreamingContext;
	@Getter
	@Setter
	private SparkConf sparkConf;
	@Setter
	@Getter
	private Properties clientProp;
	@Setter
	@Getter
	private String streamingBackpressure;
	@Setter
	@Getter
	private String kafkaMaxPartition;
	@Setter
	@Getter
	SimcoreReceiver<String> streamReceiver;
	static Broadcast<Date> broadStartDate = null;
	Long lastOffset = null;

	public MultiEventCoreSparkListener(Properties clientProp) {
		super();
		this.clientProp = clientProp;
	}

	public void init() {
		sparkConf.set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer").set(
				"spark.kryoserializer.buffer.mb", "24");
		sparkConf.set("spark.streaming.backpressure.enabled",
				streamingBackpressure);
		sparkConf.set("spark.streaming.kafka.maxRatePerPartition",
				kafkaMaxPartition);
		// sparkConf
		// .set("spark.executor.extraJavaOptions",
		// "-XX:+UseG1GC -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark  -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThread=20");

		try {
			sparkConf.registerKryoClasses(new Class<?>[] {
					Class.forName("net.paladion.model.MultiEventCoreDTO"),
					Class.forName("net.paladion.model.RuleMatchDTO"),
					Class.forName("net.paladion.model.ThreatDroolsRuleMatch"),
					Class.forName("kafka.javaapi.producer.Producer"),
                    Class.forName("net.paladion.model.DelayLoggerKeys") });
		} catch (ClassNotFoundException e) {
			log.error(e.getMessage());
		}
		
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(
				sparkConf, Durations.seconds(Integer.parseInt(clientProp
						.getProperty("streamingDurationInSeconds"))));

		broadStartDate = javaStreamingContext.sparkContext().broadcast(
				new Date());

		KafkaOffsetDaoImpl kafkaOffsetInsert = new KafkaOffsetDaoImpl(
				clientProp);
		List<KafkaDTO> resultSet = new ArrayList<KafkaDTO>();
		resultSet = kafkaOffsetInsert.readOffsets();
		JavaDStream<ConsumerRecord<String, String>> txnStreamRulesExe = streamReceiver
				.getStreamData(javaStreamingContext, resultSet);
		txnStreamRulesExe
				.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(JavaRDD<ConsumerRecord<String, String>> r)
							throws Exception {

						OffsetRange[] offsetRanges = ((HasOffsetRanges) r.rdd())
								.offsetRanges();
						JavaRDD<String> txnStreamRulesExe1 = r.map(f1);
						process(txnStreamRulesExe1);

						Long fromOffset = null;
						Long toOffset = null;
						String topic = "";
						Integer partition = null;
						for (OffsetRange offset : offsetRanges) {
							fromOffset = offset.fromOffset();
							toOffset = offset.untilOffset();
							topic = offset.topic();
							partition = offset.partition();
							kafkaOffsetInsert.savingOffset(toOffset, topic,
									partition);
						}

					}

				});
		javaStreamingContext.start();
		try {
			javaStreamingContext.awaitTermination();
		} catch (InterruptedException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * 
	 */
	static Function<ConsumerRecord<String, String>, String> f1 = new Function<ConsumerRecord<String, String>, String>() {
		private static final long serialVersionUID = 1L;

		@Override
		public String call(ConsumerRecord<String, String> record)
				throws Exception {
			return record.value();

		}

	};

	/**
	 * 
	 * @param t
	 */
	public void process(JavaRDD<String> t) {

		final ConcurrentHashMap<Integer, String> map = fileToTopicMap(clientProp
				.getProperty("custTopicMap"));

		if (t != null && !t.isEmpty()) {
			DataCarrier<JavaRDD<String>> stringFormatStream = new BaseDataCarrier<JavaRDD<String>>(
					t, null);

			stringFormatStream.addResource("topicMap", map);

			DataCarrier<JavaRDD<MultiEventCoreDTO>> threatRdd = null;

			if (Boolean.parseBoolean(clientProp
					.getProperty("multiEventCoreFirstStepEnabled"))) {
				MultiEventCoreFirstStep multiEventCoreFirstStep = new MultiEventCoreFirstStep(
						clientProp);
				threatRdd = multiEventCoreFirstStep
						.transform(stringFormatStream);
				DelayLogger delayLogger = new DelayLogger(clientProp);
				delayLogger.delayFilter(threatRdd);
			}

			if (Boolean.parseBoolean(clientProp
					.getProperty("rawDataInsertStepEnabled"))) {
				RawDataInsertStep rawDataInsertStep = new RawDataInsertStep(
						clientProp);
				threatRdd = rawDataInsertStep.transform(threatRdd);
			}

			if (Boolean.parseBoolean(clientProp
					.getProperty("ruleExecuterStepEnabled"))) {
				RuleExecuterStep ruleExecuterStep = new RuleExecuterStep(
						clientProp, broadStartDate);
				threatRdd = ruleExecuterStep.transform(threatRdd);
			}

			if (Boolean.parseBoolean(clientProp
					.getProperty("droolsRuleInsertStepEnabled"))) {
				DroolsRuleInsertStep droolsRuleInsertStep = new DroolsRuleInsertStep(
						clientProp);
				threatRdd = droolsRuleInsertStep.transform(threatRdd);
			}

			if (Boolean.parseBoolean(clientProp
					.getProperty("thresholdExecuterStepEnabled"))) {
				ThresholdExecuterStep thresholdExecuterStep = new ThresholdExecuterStep(
						clientProp);
				threatRdd = thresholdExecuterStep.transform(threatRdd);
			}

			if (Boolean.parseBoolean(clientProp
					.getProperty("finalInsertStepEnabled"))) {
				FinalInsertStep finalInsertStep = new FinalInsertStep(
						clientProp);
				threatRdd = finalInsertStep.transform(threatRdd);
			}

			if (Boolean.parseBoolean(clientProp
					.getProperty("alertGenerationStepEnabled"))) {
				AlertGenerationStep alertGenerationStep = new AlertGenerationStep(
						clientProp);
				threatRdd = alertGenerationStep.transform(threatRdd);
			}

		}
	}

	/**
	 * 
	 * @param filepath
	 * @return
	 */
	private static ConcurrentHashMap<Integer, String> fileToTopicMap(
			String filepath) {
		File file = new File(filepath);
		ConcurrentHashMap<Integer, String> topicMapRange = new ConcurrentHashMap<Integer, String>();
		FileInputStream fileInputStream = null;

		try {
			fileInputStream = new FileInputStream(file);
			Properties properties = new Properties();
			properties.load(fileInputStream);
			fileInputStream.close();
			for (final Entry<Object, Object> entry : properties.entrySet()) {

				String range = (String) entry.getKey();
				String[] Ranges = range.split("-");
				int start = Integer.valueOf(Ranges[0]);
				int end = Integer.valueOf(Ranges[1]);
				for (int i = start; i <= end; i++) {
					topicMapRange.put(i, (String) entry.getValue());
				}
			}
			return topicMapRange;
			fileInputStream.close();
		} catch (IOException e) {
			
		}
		finally{
			System.out.println("");
		}
		
	}

}
