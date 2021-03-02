package net.paladion.listener;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.FinalInsertDaoImpl;
import net.paladion.dao.KafkaOffsetDaoImpl;
import net.paladion.model.KafkaDTO;
import net.paladion.model.KafkaProducerImpl;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.MultiEventThresholdDTO;
import net.paladion.model.OffsetUpdatorKeys;
import net.paladion.model.RuleMatchDTO;
import net.paladion.model.SimcoreReceiver;
import net.paladion.rule.LoadRuleList;
import net.paladion.util.UtilityTools;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.stereotype.Component;

import scala.collection.JavaConverters;
import scala.collection.Seq;

@Component
@Slf4j
//fdfddfd
public class MecBatchListener implements Serializable {
	private static final long serialVersionUID = 1L;
	@Getter
	@Setter
	private JavaStreamingContext javaStreamingContext;
	@Getter
	@Setter
	private SparkConf sparkConf;
	@Setter
	@Getter
	private String streamingBackpressure;
	@Setter
	@Getter
	private String kafkaMaxPartition;
	@Setter
	@Getter
	private static Properties clientProp;
	@Setter
	@Getter
	SimcoreReceiver<String> streamReceiver;
	static Broadcast<Date> broadStartDate = null;

	@SuppressWarnings("static-access")
	public MecBatchListener(Properties clientProp) {
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

		try {
			sparkConf.registerKryoClasses(new Class<?>[] {
					Class.forName("net.paladion.model.RuleMatchDTO"),
					Class.forName("net.paladion.model.MultiEventThresholdDTO"),
					Class.forName("net.paladion.model.EveryConditionData"),
					Class.forName("kafka.javaapi.producer.Producer") });
		} catch (ClassNotFoundException e) {
			log.error(e.getMessage());
		}

		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(
				sparkConf, Durations.minutes(Integer.parseInt(clientProp
						.getProperty("streamingDurationInMinutes"))));

		broadStartDate = javaStreamingContext.sparkContext().broadcast(
				new Date());

		KafkaOffsetDaoImpl kafkaOffsetInsert = new KafkaOffsetDaoImpl(
				clientProp);
		List<KafkaDTO> resultSet = new ArrayList<KafkaDTO>();
		resultSet = kafkaOffsetInsert.readOffsets();
		JavaDStream<ConsumerRecord<String, String>> txnStreamRulesExe = streamReceiver
				.getStreamData(javaStreamingContext, resultSet);

		if (Boolean.parseBoolean(clientProp.getProperty("partitionReq)"))) {
			int i = Integer
					.parseInt(clientProp.getProperty("partition.number"));
			txnStreamRulesExe = txnStreamRulesExe.repartition(i);
		}
		JavaDStream<String> txnStreamRulesExe1 = txnStreamRulesExe.map(f1);
		JavaDStream<MultiEventThresholdDTO> dtoStreamDto = txnStreamRulesExe1
				.mapPartitions(changeDTO);

		JavaDStream<MultiEventThresholdDTO> window = dtoStreamDto.window(
				new Duration(Integer.parseInt(clientProp
						.getProperty("windowTimeInMinutes")) * 60000),
				new Duration(Integer.parseInt(clientProp
						.getProperty("windowSlideTimeInMinutes")) * 60000));

		Encoder<MultiEventThresholdDTO> eventEncoder = Encoders
				.bean(MultiEventThresholdDTO.class);

		window.foreachRDD(new VoidFunction<JavaRDD<MultiEventThresholdDTO>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<MultiEventThresholdDTO> threatRDD)
					throws Exception {
				String topic = "";
				Integer partition = null;
				Long lastOffset = null;
				Map<OffsetUpdatorKeys, KafkaDTO> map = new HashMap<>();

				process(threatRDD, eventEncoder);

				List<MultiEventThresholdDTO> list = threatRDD.collect();
				for (MultiEventThresholdDTO lis : list) {
					KafkaDTO kafkaDTO = new KafkaDTO();
					topic = lis.getTopic();
					partition = lis.getPartition();
					lastOffset = lis.getOffset();
					lastOffset++;
					kafkaDTO.setTopic(lis.getTopic());
					kafkaDTO.setPartition(lis.getPartition());
					kafkaDTO.setOffsets(lastOffset);
					map.put(new OffsetUpdatorKeys(topic, partition), kafkaDTO);
				}
				List<KafkaDTO> kafkaDTOList = new ArrayList<KafkaDTO>(map
						.values());
				KafkaOffsetDaoImpl kafkaOffsetDaoImpl = new KafkaOffsetDaoImpl(
						clientProp);
				kafkaOffsetDaoImpl.savingOffset(kafkaDTOList);
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
			String value = record.value() + ",topic=" + record.topic()
					+ ",partition=" + record.partition() + ",offset="
					+ record.offset();
			return value;

		}

	};

	/**
   * 
   */
	static ForeachPartitionFunction<MultiEventThresholdDTO> generateAlert = new ForeachPartitionFunction<MultiEventThresholdDTO>() {
		private static final long serialVersionUID = 1L;

		public void call(Iterator<MultiEventThresholdDTO> t) throws Exception {
			Connection connection = null;
			PreparedStatement psRuleMatch = null;
			PreparedStatement psParentChild = null;
			try {
				Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
				if (clientProp == null) {
					clientProp = UtilityTools
							.loadProperty("/etc/risqvu_di/MecBatchProcessorConfig/MecBatchProcessor.properties");
				}

				connection = DriverManager.getConnection(clientProp
						.getProperty("phoenix.database.url"));
				connection.setAutoCommit(false);

				StringBuilder sbr1 = new StringBuilder();

				sbr1.append("upsert into ");
				sbr1.append(clientProp.getProperty("phoenix.schemaname"));
				sbr1.append(".threat_rule_match (id, ti, mr, et, cu, bh, bd) values ");
				sbr1.append("(next value for ");
				sbr1.append(clientProp.getProperty("phoenix.schemaname"));
				sbr1.append(".threat_rule_match_id,?,?,?,?,?,?)");

				psRuleMatch = connection.prepareStatement(sbr1.toString());
				sbr1.delete(0, sbr1.length());
				sbr1 = null;

				StringBuilder sbr2 = new StringBuilder();

				sbr2.append("upsert into ");
				sbr2.append(clientProp.getProperty("phoenix.schemaname"));
				sbr2.append(".threat_rule_parent_child_mapping (ti, mr, dt, et, tn, tt, rr) values ");
				sbr2.append("(?,?,?,?,?,?,?)");

				psParentChild = connection.prepareStatement(sbr2.toString());
				sbr2.delete(0, sbr2.length());
				sbr2 = null;

				while (t.hasNext()) {
					MultiEventThresholdDTO multiEventThresholdDTO = t.next();

					List<Long> chl = multiEventThresholdDTO.getChl();

					List<Long> chl1 = new ArrayList<Long>();
					if (chl != null) {
						for (Long l : chl) {
							if (l != multiEventThresholdDTO.getTi()) {
								chl1.add(l);
							}
						}
					}
					multiEventThresholdDTO.setChl(chl1);

					long count = multiEventThresholdDTO.getCount();

					if (count < multiEventThresholdDTO.getRt()) {
						multiEventThresholdDTO.setCount(multiEventThresholdDTO
								.getRt());
					}

					try {
						MultiEventCoreDTO multiEventCoreDTO = UtilityTools
								.convertToMultiEventCoreDTO(multiEventThresholdDTO);

						KafkaProducerImpl.sendToKafka(clientProp,
								multiEventCoreDTO);

						FinalInsertDaoImpl.insertRuleMatchData(
								multiEventCoreDTO, connection, psRuleMatch,
								psParentChild, clientProp);
					} catch (Exception e) {
						log.error("Error while saving and generating alert for: "
								+ multiEventThresholdDTO
								+ " Error is: "
								+ e.getMessage());
					}
				}
				connection.commit();
			} catch (SQLException | ClassNotFoundException e) {
				log.error("Error in Final method: " + e.getMessage());
			} finally {
				if (connection != null) {
					try {
						connection.close();
						connection = null;
					} catch (Exception e) {
						log.error(e.getMessage());
					}
				}
				if (psRuleMatch != null) {
					try {
						psRuleMatch.close();
						psRuleMatch = null;
					} catch (Exception e) {
						log.error(e.getMessage());
					}
				}
				if (psParentChild != null) {
					try {
						psParentChild.close();
						psParentChild = null;
					} catch (Exception e) {
						log.error(e.getMessage());
					}
				}
			}
		}
	};

	/**
       * 
       */
	final static FlatMapFunction<Iterator<String>, MultiEventThresholdDTO> changeDTO = new FlatMapFunction<Iterator<String>, MultiEventThresholdDTO>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<MultiEventThresholdDTO> call(
				Iterator<String> kafkaStream) throws Exception {
			List<MultiEventThresholdDTO> multiEventThresholdDTOList = new ArrayList<MultiEventThresholdDTO>();

			while (kafkaStream.hasNext()) {
				String t1 = kafkaStream.next();

				MultiEventThresholdDTO multiEventThresholdDTO = UtilityTools
						.convertJsonToMultiEventThresholdDTO(t1);

				multiEventThresholdDTOList.add(multiEventThresholdDTO);
				t1 = null;
			}
			return multiEventThresholdDTOList.iterator();
		}
	};

	/**
	 * 
	 * @param args
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		// LoggerContext ctx = (LoggerContext)
		// LoggerFactory.getILoggerFactory();
		// JoranConfigurator jc = new JoranConfigurator();
		// jc.setContext(ctx);
		// try {
		// ctx.reset();
		// jc.doConfigure(args[1]);
		// } catch (Exception e) {
		// try {
		// throw new
		// ConnectorCommonConfigurationException("Error while configuring logger",
		// e);
		// } catch (ConnectorCommonConfigurationException e1) {
		// log.error(e1.getMessage());
		// }
		// }
		@SuppressWarnings("resource")
		ApplicationContext context = new FileSystemXmlApplicationContext(
				args[0]);
	}

	public void process(JavaRDD<MultiEventThresholdDTO> threatRDD,
			Encoder<MultiEventThresholdDTO> eventEncoder) {

		final SparkSession spark = JavaSparkSessionSingleton
				.getInstance(threatRDD.context().getConf());
		Calendar cal1 = Calendar.getInstance();
		cal1.setTime(new Date());

		cal1.add(Calendar.SECOND, -10);

		Timestamp dNow = new Timestamp(cal1.getTime().getTime());

		Dataset<Row> windowDataSet = spark.createDataFrame(threatRDD,
				MultiEventThresholdDTO.class);

		windowDataSet.persist();
		// windowDataSet.show(500);
		// Long count = windowDataSet.count();
		// log.info("Count=" + count);
		// TODO Remove following comment to see rule wise matched record count
		// Dataset<Row> windowDataSetNow =
		// windowDataSet.filter("sd > cast('" + dNow +
		// "' as timestamp)").groupBy("mr").count();
		//
		// windowDataSetNow.persist();
		// windowDataSetNow.show(200);
		// List<Row> currentBatchMatchedRules =
		// windowDataSetNow.select(new Column("mr")).collectAsList();
		// List<Integer> currentBatchMatchruleId = new ArrayList<Integer>();
		// for (Row row : currentBatchMatchedRules) {
		// if (row.get(0) != null) {
		// currentBatchMatchruleId.add((Integer) row.get(0));
		// }
		// }

		List<Row> currentBatchMatchedRules = windowDataSet
				.filter("sd > cast('" + dNow + "' as timestamp)")
				.select(new Column("mr")).distinct().collectAsList();

		List<Integer> currentBatchMatchruleId = new ArrayList<Integer>();
		for (Row row : currentBatchMatchedRules) {
			if (row.get(0) != null) {
				currentBatchMatchruleId.add((Integer) row.get(0));
			}
		}

		// System.out.println("currentBatchMatchruleId: " +
		// currentBatchMatchruleId);

		Date startDate = (Date) broadStartDate.value();
		Date currentDate = new Date();

		Long duration = currentDate.getTime() - startDate.getTime();
		Long diffInMinutes = TimeUnit.MILLISECONDS.toMinutes(duration);

		boolean reloadRules = false;
		if (diffInMinutes
				% Integer.parseInt(clientProp
						.getProperty("reloadDroolsInMinutes")) == 0) {
			reloadRules = true;
		}
		List<RuleMatchDTO> ruleList = LoadRuleList.getRuleList(clientProp,
				reloadRules);

		for (RuleMatchDTO ruleMatchDTO : ruleList) {
			int ruleId = ruleMatchDTO.getRuleId();
			if (currentBatchMatchruleId.contains(ruleId)) {
				// System.out.println("Calculation start for rule Id: " + ruleId
				// + " start time: "
				// + new Date());

				int thresholdVal = ruleMatchDTO.getEventAThresholdValue();
				int timeWindowValue = ruleMatchDTO.getTimeWindowValue();
				String timeWindowUnit = ruleMatchDTO.getTimeWindowUnit();
				String thresholdCondition = ruleMatchDTO
						.getThresholdCondition();
				String identicalCol = ruleMatchDTO.getIdenticalColStr();
				String distinctCol = ruleMatchDTO.getDistinctColStr();

				if (timeWindowValue != 0
						&& !StringUtils.isEmpty(timeWindowUnit)
						&& !timeWindowUnit.equalsIgnoreCase("na")
						&& !timeWindowUnit.equalsIgnoreCase("null")) {
					List<Column> colList = new ArrayList<Column>();
					List<Column> icolList = new ArrayList<Column>();
					if (!StringUtils.isEmpty(distinctCol)
							&& !distinctCol.equalsIgnoreCase("na")
							&& !distinctCol.equalsIgnoreCase("null")) {
						String[] dCStrArr = distinctCol.split(",");
						if (dCStrArr != null && dCStrArr.length > 0) {
							for (String dCol : dCStrArr) {
								String cName = clientProp.getProperty(dCol
										.trim());
								colList.add(new Column(cName));
								cName = null;
							}
							dCStrArr = null;
						}
					}

					if (!StringUtils.isEmpty(identicalCol)
							&& !identicalCol.equalsIgnoreCase("na")
							&& !identicalCol.equalsIgnoreCase("null")) {
						String[] identicalStrArr = identicalCol.split(",");

						if (identicalStrArr != null
								&& identicalStrArr.length > 0) {
							for (String dCol : identicalStrArr) {
								String cName = clientProp.getProperty(dCol
										.trim());
								colList.add(new Column(cName));
								icolList.add(new Column(cName));
								cName = null;
							}
						}
						identicalStrArr = null;
					}

					StringBuilder tACSbr = new StringBuilder();
					Calendar cal = Calendar.getInstance();

					cal.setTime(new Date());

					long endRange = 1;

					if (timeWindowUnit.equalsIgnoreCase("s")) {
						cal.add(Calendar.SECOND, -timeWindowValue);
						endRange = timeWindowValue * 1000;
					} else if (timeWindowUnit.equalsIgnoreCase("m")) {
						cal.add(Calendar.MINUTE, -timeWindowValue);
						endRange = timeWindowValue * 60000;
					} else if (timeWindowUnit.equalsIgnoreCase("h")) {
						cal.add(Calendar.HOUR, -timeWindowValue);
						endRange = timeWindowValue * 3600000;
					}

					Timestamp dBefore = new Timestamp(cal.getTime().getTime());

					// tACSbr.append(" et >= cast('" + dBefore +
					// "' as timestamp) ");

					dBefore = null;
					cal = null;

					Dataset<Row> windowDFWithCond = null;

					windowDFWithCond = windowDataSet// .where(tACSbr.toString())
							.filter(windowDataSet.col("mr").equalTo(ruleId));

					// windowDFWithCond =
					// windowDataSet.filter(windowDataSet.col("mr").equalTo(ruleId));

					// windowDFWithCond.cache();
					// windowDFWithCond.show();
					// log.info("Count = " + windowDFWithCond.count());
					tACSbr.delete(0, tACSbr.length());
					tACSbr = null;

					try {
						windowDFWithCond.persist();
						icolList.add(new Column("mr"));
						icolList.add(new Column("ct"));

						colList.add(new Column("mr"));
						colList.add(new Column("ct"));
						colList.add(new Column("matchedRecords"));
						colList.add(new Column("matchedFamilyId"));

						Seq<Column> iColSq = JavaConverters
								.asScalaBufferConverter(icolList).asScala()
								.seq();

						WindowSpec winspec = Window.partitionBy(iColSq)
								.orderBy(new Column("el").desc())
								.rangeBetween(0, endRange);

						Dataset<Row> windowDFWithCondCopy = windowDFWithCond;
						Seq<Column> sqCol = null;
						if (!StringUtils.isEmpty(distinctCol)
								&& !distinctCol.equalsIgnoreCase("na")
								&& !distinctCol.equalsIgnoreCase("null")) {
							sqCol = JavaConverters
									.asScalaBufferConverter(colList).asScala()
									.seq();

							windowDFWithCondCopy = windowDFWithCondCopy
									.orderBy(new Column("el").desc())
									.groupBy(sqCol)
									.agg(functions.max("ab").as("ab"),
											functions.max("el").as("el"),
											functions.first("sd").as("sd"),
											functions.first("rt").as("rt"),
											functions.first("rd").as("rd"),
											functions.first("ti").as("ti"));
						}

						// windowDFWithCondCopy.show();
						// log.info("Count = " + windowDFWithCondCopy.count());
						List<String> list = new ArrayList<String>();
						list.add("mr");
						list.add("ti");
						list.add("ct");

						Seq<String> sqTMR = JavaConverters
								.asScalaBufferConverter(list).asScala().seq();

						WindowSpec winspec2 = Window.partitionBy(iColSq)
								.orderBy(new Column("el").desc())
								.rangeBetween(1, endRange);

						if (thresholdCondition.equalsIgnoreCase("every")) {
							Dataset<Row> countDataSet = windowDFWithCondCopy
									.withColumn(
											"count",
											functions.sum(
													windowDFWithCondCopy
															.col("ab")).over(
													winspec))
									.withColumn(
											"countold",
											functions.sum(
													windowDFWithCondCopy
															.col("ab")).over(
													winspec2))
									.withColumn(
											"countRecords",
											functions.count(
													windowDFWithCondCopy
															.col("ab")).over(
													winspec))
									.orderBy(new Column("el").desc());
							// countDataSet.cache();
							// countDataSet.show();
							// log.info("Count = " + countDataSet.count());

							EveryConditionUDF everyConditionUDF = new EveryConditionUDF();
							final String udfName = "everyConditionUDF";
							spark.udf().register(udfName, everyConditionUDF,
									DataTypes.StringType);

							Dataset<Row> countDataSet1 = countDataSet
									.withColumn(
											"matchedRecords",
											functions
													.callUDF(
															"everyConditionUDF",
															countDataSet
																	.col("rt"),
															countDataSet
																	.col("countold"),
															countDataSet
																	.col("count"),
															countDataSet
																	.col("countRecords"),
															countDataSet
																	.col("rd")))
									.orderBy(new Column("el").desc());

							// countDataSet1.cache();
							// countDataSet1.show();
							// log.info("Count = " + countDataSet1.count());

							Dataset<Row> countDataSet2 = countDataSet1
									.withColumn(
											"matchedFamilyId",
											functions
													.split(countDataSet1
															.col("matchedRecords"),
															"_").getItem(0))
									.orderBy(new Column("el").desc());

							// countDataSet2.cache();
							// countDataSet2.show();
							// log.info("Count = " + countDataSet2.count());

							icolList.add(new Column("matchedFamilyId"));
							Seq<Column> iColSq1 = JavaConverters
									.asScalaBufferConverter(icolList).asScala()
									.seq();

							WindowSpec winspec1 = Window.partitionBy(iColSq1)
									.orderBy(new Column("el").desc())
									.rangeBetween(0, endRange);

							Dataset<Row> countDataSet3 = countDataSet2
									.withColumn(
											"chl",
											functions.collect_list(
													countDataSet2.col("ti"))
													.over(winspec1))
									.withColumn(
											"count",
											functions.sum(
													countDataSet2.col("ab"))
													.over(winspec1))
									.orderBy(new Column("el").desc());

							// countDataSet3.cache();
							// countDataSet3.show();
							// log.info("Count = " + countDataSet3.count());

							List<String> elList = new ArrayList<String>();
							elList.add("el");

							Seq<String> sqEl = JavaConverters
									.asScalaBufferConverter(elList).asScala()
									.seq();

							Dataset<Row> countDataSet4 = countDataSet3
									.select(countDataSet3.col("mr"),
											countDataSet3.col("ti"),
											countDataSet3.col("el"),
											countDataSet3.col("ct"),
											countDataSet3.col("count"),
											countDataSet3.col("chl"))
									.filter(" sd > cast('"
											+ dNow
											+ "' as timestamp) and matchedRecords like \"%_matched%\"")
									.dropDuplicates(sqEl);

							// countDataSet4.cache();
							// countDataSet4.show();
							// log.info("Count = " + countDataSet4.count());

							elList = null;
							sqEl = null;

							// Dataset<Row> countDataSet5 =
							// countDataSet4.dropDuplicates(sqEl);
							//
							// countDataSet5.cache();
							// countDataSet5.show();

							// List<Row> ds = countDataSet4.collectAsList();
							//
							// if (ds != null) {
							// for (Row r : ds) {
							// System.out.println("ti: " + r.getAs("ti"));
							// System.out.println("chl: " + r.getAs("chl"));
							// System.out.println("count: " + r.getAs("count"));
							// }
							// }

							Dataset<Row> matchDataSet1 = windowDFWithCond
									.join(countDataSet4, sqTMR)
									.drop(windowDFWithCond.col("count"))
									.drop(windowDFWithCond.col("chl"))
									.drop(windowDFWithCond.col("el"));
							// matchDataSet1.show();
							matchDataSet1.persist();
							Dataset<MultiEventThresholdDTO> matchDataSet2 = matchDataSet1
									.as(eventEncoder);

							// matchDataSet2.persist();
							// matchDataSet2.show(500);
							// log.info("Count = " + matchDataSet2.count());

							matchDataSet2.foreachPartition(generateAlert);
							matchDataSet1.unpersist();
						} else {
							Dataset<Row> countDataSet = windowDFWithCondCopy
									.withColumn(
											"count",
											functions.sum(
													windowDFWithCondCopy
															.col("ab")).over(
													winspec))
									.withColumn(
											"countRecords",
											functions.count(
													windowDFWithCondCopy
															.col("ti")).over(
													winspec))
									.withColumn(
											"chl",
											functions.collect_list(
													windowDFWithCondCopy
															.col("ti")).over(
													winspec));

							// countDataSet.cache();
							// countDataSet.show();
							// log.info("Count = " + countDataSet.count());
							Dataset<Row> matchDataSetMore;
							if (!StringUtils.isEmpty(distinctCol)
									&& !distinctCol.equalsIgnoreCase("na")
									&& !distinctCol.equalsIgnoreCase("null")) {
								matchDataSetMore = countDataSet
										.select(new Column("mr"),
												new Column("ti"),
												new Column("ct"),
												new Column("count"),
												new Column("chl"))
										.where("sd > cast('" + dNow
												+ "' as timestamp)")
										.filter(countDataSet
												.col("countRecords")
												.$greater$eq(thresholdVal));
							} else {
								matchDataSetMore = countDataSet
										.select(new Column("mr"),
												new Column("ti"),
												new Column("ct"),
												new Column("count"),
												new Column("chl"))
										.where("sd > cast('" + dNow
												+ "' as timestamp)")
										.filter(countDataSet.col("count")
												.$greater$eq(thresholdVal));
							}

							Dataset<Row> matchDataSetMore1 = windowDFWithCond
									.join(matchDataSetMore, sqTMR)
									.drop(windowDFWithCond.col("count"))
									.drop(windowDFWithCond.col("chl"));
							// matchDataSetMore1.cache();
							// matchDataSetMore1.show();
							// log.info("Count = " + matchDataSetMore1.count());

							Dataset<MultiEventThresholdDTO> matchDataSetMore2 = matchDataSetMore1
									.as(eventEncoder);

							matchDataSetMore2.foreachPartition(generateAlert);
						}

						sqTMR = null;
						list = null;

						sqCol = null;
						colList = null;
						iColSq = null;
						icolList = null;
					} catch (NoSuchElementException e) {
						// log.error("Dataframe for rule id: " + ruleId +
						// " is empty");
					} catch (Exception e) {
						log.error("Error: " + e.getMessage());
					}

					windowDFWithCond.unpersist();
				}

				// System.out.println("Calculation end for rule Id: " + ruleId +
				// " end time: "
				// + new Date());
			}
		}

		dNow = null;
		windowDataSet.unpersist();

	}
}

/** Lazily instantiated singleton instance of SparkSession */
class JavaSparkSessionSingleton {
	private static transient SparkSession instance = null;

	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession.builder().config(sparkConf).getOrCreate();
		}
		return instance;
	}
}

/**
 * 
 * @author ankush
 *
 */
class EveryConditionUDF implements
		UDF5<Integer, Long, Long, Long, String, String> {
	private static final long serialVersionUID = 1L;

	@Override
	public String call(Integer rt, Long countold, Long count, Long countRec,
			String distinctFields) throws Exception {
		String r = "";
		if (count != null) {
			if (!StringUtils.isEmpty(distinctFields)
					&& !distinctFields.equalsIgnoreCase("na")
					&& !distinctFields.equalsIgnoreCase("null")) {
				long reminder = countRec / rt;
				if (countRec % rt == 0) {
					r = reminder + "_matched";
				} else {
					r = reminder + 1 + "";
				}
			} else {
				long reminder = count / rt;

				// long oldcount = count - ab;
				long oldreminder = 0;
				if (countold != null) {
					oldreminder = countold / rt;
				}

				if (oldreminder < reminder) {
					r = reminder + "_matched";
				} else {
					r = reminder + 1 + "";
				}
			}
		}

		return r;
	}
}
