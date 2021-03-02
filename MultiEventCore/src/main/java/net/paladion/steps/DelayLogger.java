package net.paladion.steps;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.DelayLoggerDaoImpl;
import net.paladion.model.DelayLoggerKeys;
import net.paladion.model.MultiEventCoreDTO;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.paladion.racommon.data.DataCarrier;

@Slf4j
public class DelayLogger implements Serializable {
	private static final long serialVersionUID = 1L;

	private final Properties clientProp;

	public DelayLogger(Properties clientProp1) {
		super();
		clientProp = clientProp1;
	}

	/**
	 * 
	 * @param threatRdd
	 */
	public void delayFilter(DataCarrier<JavaRDD<MultiEventCoreDTO>> threatRdd) {

		JavaRDD<MultiEventCoreDTO> rdd1 = threatRdd.getPayload();

		JavaRDD<MultiEventCoreDTO> rdd2 = rdd1
				.filter(new Function<MultiEventCoreDTO, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(MultiEventCoreDTO v1) throws Exception {
						if (v1.getLogDelay().contains("Yes")) {
							return true;
						}
						return false;

					}

				});
		// rdd2.collect().forEach(f -> System.out.println(f));
		delayGroupBy(rdd2);

	}

	/**
	 * 
	 * @param rdd2
	 */
	public void delayGroupBy(JavaRDD<MultiEventCoreDTO> rdd2) {
		JavaRDD<MultiEventCoreDTO> rdd3 = rdd2
				.mapPartitions(new FlatMapFunction<Iterator<MultiEventCoreDTO>, MultiEventCoreDTO>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<MultiEventCoreDTO> call(
							Iterator<MultiEventCoreDTO> t) throws Exception {

						MultiEventCoreDTO row = null;
						Map<DelayLoggerKeys, String[]> rtMap = new HashMap<>();
						Map<DelayLoggerKeys, String[]> artMap = new HashMap<>();
						while (t.hasNext()) {
							try {
								row = t.next();
								String rtdelayFlag = "";
								String artdelayFlag = "";
								rtdelayFlag = row.getRtdelayFlag();
								artdelayFlag = row.getArtdelayFlag();

								String agentAddr = row.getAgentAddress();
								if (!StringUtils.isEmpty(agentAddr)) {
									agentAddr = agentAddr.trim();
								}

								String agentName = row.getAgentHostName();
								if (!StringUtils.isEmpty(agentName)) {
									agentName = agentName.trim();
								}

								String cusName = row.getCustomerName();
								if (!StringUtils.isEmpty(cusName)) {
									cusName = cusName.trim();
								}

								String dvcHost = row.getDeviceHostName();
								if (!StringUtils.isEmpty(dvcHost)) {
									dvcHost = dvcHost.trim();
								}

								String dvcAddr = row.getDeviceAddress();
								if (!StringUtils.isEmpty(dvcAddr)) {
									dvcAddr = dvcAddr.trim();
								}

								String dvcVendor = row.getDeviceVendor();
								if (!StringUtils.isEmpty(dvcVendor)) {
									dvcVendor = dvcVendor.trim();
								}
								Long eventId = row.getEventId();
								String eventIdStr = null;
								if (eventId != null && eventId != 0L) {
									eventIdStr = eventId.toString();
								}
								if (!StringUtils.isEmpty(rtdelayFlag)
										&& rtdelayFlag != ""
										&& rtdelayFlag.contentEquals("rt")) {
									Timestamp lRT = row.getDeviceReceiptTime();
									String lRTStr = "";
									if (lRT != null) {
										lRTStr = lRT.toString();
									}

									Timestamp eT = row.getEventTime();
									String etStr = "";
									if (eT != null) {
										etStr = eT.toString();
									}

									Timestamp lART = row.getAgentReceiptTime();
									String lARTStr = "";
									if (lART != null) {
										lARTStr = lART.toString();
									}
									Integer timeMins = null;
									String dm = null;
									if (lRT != null && eT != null) {
										long timeMilis = eT.getTime()
												- lRT.getTime();
										timeMins = (int) TimeUnit.MILLISECONDS
												.toMinutes(timeMilis);
										dm = timeMins.toString();
									}
									String str2[] = { agentAddr, agentName,
											cusName, dvcHost, dvcAddr,
											dvcVendor, lRTStr, lARTStr, etStr,
											eventIdStr, dm };

									if (rtMap.containsKey(new DelayLoggerKeys(
											agentAddr, agentName, cusName,
											dvcHost, dvcAddr, dvcVendor,
											rtdelayFlag))) {
										log.info("New Key in rt");
									} else {
										rtMap.put(new DelayLoggerKeys(
												agentAddr, agentName, cusName,
												dvcHost, dvcAddr, dvcVendor,
												rtdelayFlag), str2);
									}
								}

								if (!StringUtils.isEmpty(artdelayFlag)
										&& artdelayFlag != ""
										&& artdelayFlag.contentEquals("art")) {
									Timestamp lART = row.getAgentReceiptTime();

									String lARTStr = "";
									if (lART != null) {
										lARTStr = lART.toString();
									}

									Timestamp lRT = row.getDeviceReceiptTime();
									String lRTStr = "";
									if (lRT != null) {
										lRTStr = lRT.toString();
									}
									Timestamp eT = row.getEventTime();
									String etStr = "";
									if (eT != null) {
										etStr = eT.toString();
									}
									Integer timeMins = null;
									String dm = null;
									if (lART != null && lRT != null) {
										long timeMilis = lRT.getTime()
												- lART.getTime();
										timeMins = (int) TimeUnit.MILLISECONDS
												.toMinutes(timeMilis);
										dm = timeMins.toString();
									}
									String str2[] = { agentAddr, agentName,
											cusName, dvcHost, dvcAddr,
											dvcVendor, lRTStr, lARTStr, etStr,
											eventIdStr, dm };

									if (artMap.containsKey(new DelayLoggerKeys(
											agentAddr, agentName, cusName,
											dvcHost, dvcAddr, dvcVendor,
											artdelayFlag))) {
										log.info("New Key in art");
									} else {
										artMap.put(new DelayLoggerKeys(
												agentAddr, agentName, cusName,
												dvcHost, dvcAddr, dvcVendor,
												artdelayFlag), str2);
									}
								}

							} catch (Exception e) {
								log.error(e.getMessage() + " Error for row: "
										+ row);
							}
						}
						if (rtMap != null && !rtMap.isEmpty()) {
							List<String[]> rtDelayLoggerDTOList = new ArrayList<String[]>(
									rtMap.values());

							rtDelayPhnxInsert(rtDelayLoggerDTOList);
						}
						if (artMap != null && !artMap.isEmpty()) {
							List<String[]> artDelayLoggerDTOList = new ArrayList<String[]>(
									artMap.values());
							artDelayPhnxInsert(artDelayLoggerDTOList);
						}
						return t;
					}

				});
		rdd3.count();
	}

	/**
	 * 
	 * @param rtDelayLoggerDTOList
	 */
	public void rtDelayPhnxInsert(List<String[]> rtDelayLoggerDTOList) {
		DelayLoggerDaoImpl delayLoggerDaoImpl = new DelayLoggerDaoImpl();
		Connection connection = null;

		String ins = " upsert into "
				+ clientProp.getProperty("phoenix.schemaname")
				+ "."
				+ clientProp.getProperty("delayLogger.tableName")
				+ " (aa,an,cn,dn,da,dv,rt,art,et,dm,lu,fl,bh) values(?,?,?,?,?,?,?,?,?,?,now(),'rt',?)";
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			connection = DriverManager.getConnection(clientProp
					.getProperty("phoenix.database.url"));
			delayLoggerDaoImpl.insertDelayLogger(connection, ins,
					rtDelayLoggerDTOList);
		} catch (Exception e) {
			log.error("Error in rtDelayPhnxInsert(): " + e);
		} finally {
			try {
				if (connection != null) {
					connection.close();
					connection = null;
				}
			} catch (Exception e) {
				log.error("Unable to close connection in rtDelayPhnxInsert(): "
						+ e);
			}
		}
	}

	/**
	 * 
	 * @param artDelayLoggerDTOList
	 */
	public void artDelayPhnxInsert(List<String[]> artDelayLoggerDTOList) {
		DelayLoggerDaoImpl delayLoggerDaoImpl = new DelayLoggerDaoImpl();
		Connection connection = null;

		String ins = " upsert into "
				+ clientProp.getProperty("phoenix.schemaname")
				+ "."
				+ clientProp.getProperty("delayLogger.tableName")
				+ " (aa,an,cn,dn,da,dv,rt,art,et,dm,lu,fl,bh) values(?,?,?,?,?,?,?,?,?,?,now(),'art',?)";
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			connection = DriverManager.getConnection(clientProp
					.getProperty("phoenix.database.url"));
			delayLoggerDaoImpl.insertDelayLogger(connection, ins,
					artDelayLoggerDTOList);
		} catch (Exception e) {
			log.error("Error in artDelayPhnxInsert(): " + e);
		} finally {
			try {
				if (connection != null) {
					connection.close();
					connection = null;
				}
			} catch (Exception e) {
				log.error("Unable to close connection in artDelayPhnxInsert(): "
						+ e);
			}
		}

	}
}
