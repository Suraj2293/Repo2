package net.paladion.listener;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.RuleSimulationDaoImpl;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.RuleSimulationDetailsDTO;
import net.paladion.model.TestBedDTO;
import net.paladion.model.ThreatRuleMatchDTO;
import net.paladion.steps.DroolsRuleInsertStep;
import net.paladion.steps.FinalInsertStep;
import net.paladion.steps.RuleExecuterStep;
import net.paladion.steps.TestBedFirstStep;
import net.paladion.steps.ThresholdExecuterStep;
import net.paladion.steps.TriggeredAlertStep;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;

/**
 * 
 * @author Karthik Madhu
 *
 */
@Component
@Slf4j
public class RuleSimulatorListener {
	@Getter
	@Setter
	private SparkConf sparkConf;
	@Setter
	@Getter
	private Properties clientProp;

	/**
	 * 
	 * @param clientProp
	 */
	public RuleSimulatorListener(Properties clientProp) {
		super();
		this.clientProp = clientProp;
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "deprecation", "unused" })
	private void init() {
		log.info("Rule Simulator has started");

		RuleSimulationDaoImpl ruleSimulationDaoImpl = new RuleSimulationDaoImpl(
				clientProp);
		Timestamp startTime = null;
		Timestamp endTime = null;
		Integer queryTimeInterval = Integer.parseInt(clientProp
				.getProperty("queryTimeInterval"));
		RuleSimulationDetailsDTO ruleSimulationDetailsDTO = ruleSimulationDaoImpl
				.getEventTime();
		startTime = ruleSimulationDetailsDTO.getIngestionStartTime();
		endTime = ruleSimulationDetailsDTO.getIngestionEndTime();
		if (Boolean.parseBoolean(StringUtils.lowerCase(clientProp
				.getProperty("convertDateToUtc")))) {
			SimpleDateFormat sdf = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss.SSS");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			String startTimeStr = sdf.format(startTime);
			startTime = Timestamp.valueOf(startTimeStr);
			String endTimeStr = sdf.format(endTime);
			endTime = Timestamp.valueOf(endTimeStr);
		}

		try {
			if (new File(clientProp.getProperty("tempRawLogCSVPath")).exists()) {

				FileUtils.forceDelete(new File(clientProp
						.getProperty("tempRawLogCSVPath")));

			}
			if (new File(clientProp.getProperty("tempThreatRuleMatchCSVPath"))
					.exists()) {
				FileUtils.forceDelete(new File(clientProp
						.getProperty("tempThreatRuleMatchCSVPath")));
			}
		} catch (Exception e) {
			log.error("Exception occured while deleting the temporary files "
					+ e.getMessage());
		}

		sparkConf.set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer").set(
				"spark.kryoserializer.buffer.mb", "24");

		try {

			JavaSparkContext jsc = new JavaSparkContext(sparkConf);

			SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
			String query = clientProp.getProperty("phoenix.schemaname")
					+ ".simulator_threat_raw";
			createThreatRawCSV(sqlContext, query);
			ruleCorrelationProcess(startTime, endTime, queryTimeInterval,
					sqlContext, query);
			createThreatRuleCSV(sqlContext);
		
		} catch (Exception e) {
			log.error("Error in init() : " + e.getMessage());
		}

	}

	public void createThreatRawCSV(SQLContext sqlContext, String query) {

		Dataset<Row> rawDF = null;
		rawDF = sqlContext
				.read()
				.format("jdbc")
				.options(
						ImmutableMap.of("driver",
								"org.apache.phoenix.jdbc.PhoenixDriver", "url",
								clientProp.getProperty("phoenix.database.url"),
								"dbtable", query)).load();
		/*
		 * long count = rawDF.count(); log.info("Count = " + count);
		 */

		rawDF.coalesce(1).write().mode(SaveMode.Overwrite)
				.option("header", "true")
				.csv(clientProp.getProperty("tempRawLogCSVPath"));
		rawDF.unpersist();
		rawDF = null;
	}

	/**
	 * This method does the rule correlation process
	 * 
	 * @param startTime
	 * @param endTime
	 * @param queryTimeInterval
	 * @param sqlContext
	 * @param query
	 */
	public void ruleCorrelationProcess(Timestamp startTime, Timestamp endTime,
			Integer queryTimeInterval, SQLContext sqlContext, String query) {
		try {
			Long tempTime = startTime.getTime();
			while (tempTime <= endTime.getTime()) {

				String filterDF = "";
				SimpleDateFormat sdf = new SimpleDateFormat(
						"yyyy-MM-dd HH:mm:ss.SSS");
				Timestamp variableTime = new Timestamp(tempTime);
				log.info("Time1 = " + variableTime.toString());
				Calendar cal = Calendar.getInstance();
				cal.clear();
				cal.setTime(variableTime);
				cal.add(Calendar.MINUTE, queryTimeInterval);
				Date date2 = cal.getTime();
				String time2 = sdf.format(date2);
				Timestamp t2 = Timestamp.valueOf(time2);
				tempTime = t2.getTime();
				if (t2.getTime() <= endTime.getTime()) {
					log.info("Time2 = " + t2.toString());
					filterDF = "ET between '" + variableTime.toString()
							+ "' and '" + t2.toString() + "'";
				} else {
					log.info("Time2 = " + endTime.toString());
					filterDF = "ET between '" + variableTime.toString()
							+ "' and '" + endTime.toString() + "'";
				}

				Dataset<Row> df = null;
				Encoder<TestBedDTO> dfEncoder = null;
				Dataset<TestBedDTO> set = null;
				JavaRDD<TestBedDTO> rdd = null;
				log.info("Dataframe reader started");
				df = sqlContext
						.read()
						.format("jdbc")
						.options(
								ImmutableMap
										.of("driver",
												"org.apache.phoenix.jdbc.PhoenixDriver",
												"url",
												clientProp
														.getProperty("phoenix.database.url"),
												"dbtable", query)).load()
						.filter(filterDF);
				log.info("Dataframe reader ended");
				dfEncoder = Encoders.bean(TestBedDTO.class);
				set = df.as(dfEncoder);
				rdd = set.javaRDD();
				if (!rdd.isEmpty()) {
					/*
					 * log.info("RDD repartition started");
					 * rdd.repartition(100); log.info("RDD repartition ended");
					 */
					JavaRDD<MultiEventCoreDTO> threatRdd = null;

					// Enable or disable FirstStep
					if (Boolean.parseBoolean(clientProp
							.getProperty("testBedFirstStepEnabled"))) {
						TestBedFirstStep testBedFirstStep = new TestBedFirstStep(
								clientProp);
						threatRdd = testBedFirstStep.tranform(rdd);
					}

					// Enable or disable ruleExecutorStep
					if (Boolean.parseBoolean(clientProp
							.getProperty("ruleExecuterStepEnabled"))) {
						RuleExecuterStep ruleExecuterStep = new RuleExecuterStep(
								clientProp);
						threatRdd = ruleExecuterStep.transform(threatRdd);
					}

					// Enable or disable droolRuleInsertStep
					if (Boolean.parseBoolean(clientProp
							.getProperty("droolsRuleInsertStepEnabled"))) {
						DroolsRuleInsertStep droolsRuleInsertStep = new DroolsRuleInsertStep(
								clientProp);
						threatRdd = droolsRuleInsertStep.transform(threatRdd);
					}

					// Enable or disable thresholdExecuterStep
					if (Boolean.parseBoolean(clientProp
							.getProperty("thresholdExecuterStepEnabled"))) {
						ThresholdExecuterStep thresholdExecuterStep = new ThresholdExecuterStep(
								clientProp);
						threatRdd = thresholdExecuterStep.transform(threatRdd);
					}

					// Enable or disable finalInsettStep
					if (Boolean.parseBoolean(clientProp
							.getProperty("finalInsertStepEnabled"))) {
						FinalInsertStep finalInsertStep = new FinalInsertStep(
								clientProp);
						threatRdd = finalInsertStep.transform(threatRdd);
					}
					threatRdd.count();
					threatRdd.unpersist();
				}
			}
		} catch (Exception e) {
			log.error("Exception occurred in Rule Correlation Process {} {} ",
					e.getMessage(), e.getCause());
		}
	}

	/**
	 * This method creates the Threat rule csv file
	 * 
	 * @param sqlContext
	 */
	public void createThreatRuleCSV(SQLContext sqlContext) {
		try {
			String threatRule = "(select TI,ET,DT from  "
					+ clientProp.getProperty("phoenix.schemaname")
					+ ".simulator_threat_rule_match)";
			Dataset<Row> df2 = null;
			df2 = sqlContext
					.read()
					.format("jdbc")
					.options(
							ImmutableMap.of(
									"driver",
									"org.apache.phoenix.jdbc.PhoenixDriver",
									"url",
									clientProp
											.getProperty("phoenix.database.url"),
									"dbtable", threatRule)).load();
			Encoder<ThreatRuleMatchDTO> threatRuleMatchEncoder = null;
			Dataset<ThreatRuleMatchDTO> setThreatRuleMatch = null;
			JavaRDD<ThreatRuleMatchDTO> rddThreatRuleMatch = null;
			threatRuleMatchEncoder = Encoders.bean(ThreatRuleMatchDTO.class);
			setThreatRuleMatch = df2.as(threatRuleMatchEncoder);
			rddThreatRuleMatch = setThreatRuleMatch.javaRDD();

			JavaRDD<MultiEventCoreDTO> ruleMatchRdd = null;
			if (Boolean.parseBoolean(clientProp
					.getProperty("triggeredAlertStepEnabled"))) {
				TriggeredAlertStep triggeredAlertStep = new TriggeredAlertStep(
						clientProp);
				ruleMatchRdd = triggeredAlertStep.tranform(rddThreatRuleMatch);
				Dataset<Row> df3 = sqlContext.createDataFrame(ruleMatchRdd,
						MultiEventCoreDTO.class);
				log.info("Creating rule match table");
				df3.createOrReplaceTempView("rule_match");
				Dataset<Row> ruleMatchDf = sqlContext
						.sql("select threatId,eventTime,"
								+ clientProp.getProperty("variable_sequence")
								+ " from rule_match");
				ruleMatchDf
						.coalesce(1)
						.write()
						.mode(SaveMode.Overwrite)
						.option("header", "true")
						.csv(clientProp
								.getProperty("tempThreatRuleMatchCSVPath"));
			}
		} catch (Exception e) {
			log.error("Exception occured while creating the threat rule csv");
		}
	}
}
