/**
 * 
 */
package net.paladion.steps;

import java.io.File;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.util.DTOConverter;
import net.paladion.util.UtilityTools;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import com.paladion.racommon.data.BaseDataCarrier;
import com.paladion.racommon.data.DataCarrier;

/**
 * @author ankush
 *
 */
@Slf4j
public class MultiEventCoreFirstStep
		extends
		SparkStep<DataCarrier<JavaRDD<String>>, DataCarrier<JavaRDD<MultiEventCoreDTO>>> {
	private static final long serialVersionUID = 1L;
	private final Properties clientProp;

	public MultiEventCoreFirstStep(Properties clientProp1) {
		super();
		clientProp = clientProp1;
	}

	public DataCarrier<JavaRDD<MultiEventCoreDTO>> customTransform(
			DataCarrier<JavaRDD<String>> dataCarrier) {
		JavaRDD<String> stringFormatStream = dataCarrier.getPayload();
		@SuppressWarnings("unchecked")
		final ConcurrentHashMap<Integer, String> map = (ConcurrentHashMap<Integer, String>) dataCarrier
				.getResource("topicMap");

		int i = Integer.parseInt(clientProp.getProperty("partition.number"));
		if ("true".equalsIgnoreCase((String) clientProp
				.getProperty("partitionReq"))) {
			stringFormatStream = stringFormatStream.repartition(i);
		}

		JavaPairRDD<String, Long> paired_idx = stringFormatStream
				.zipWithUniqueId();

		JavaRDD<MultiEventCoreDTO> dtoStream = paired_idx
				.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Long>>, MultiEventCoreDTO>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<MultiEventCoreDTO> call(
							Iterator<Tuple2<String, Long>> iterator)
							throws Exception {

						List<MultiEventCoreDTO> multiEventCoreDTOList = new ArrayList<MultiEventCoreDTO>();
						Map<String, String> fieldsMap = UtilityTools
								.convertPropertyToMap(clientProp
										.getProperty("mappingPath"));

						DatabaseReader databaseReader = null;
						if (!StringUtils.isEmpty(clientProp
								.getProperty("geoLocationEnabled"))
								&& clientProp.getProperty("geoLocationEnabled")
										.equalsIgnoreCase("true")) {

							try {
								File file = new File(clientProp
										.getProperty("geoliteDbFilePath"));
								databaseReader = new DatabaseReader.Builder(
										file).build();

								file = null;
							} catch (Exception e) {
								log.error("Error while fetching geo lite db, exception is: "
										+ e.getMessage());
							}
						}

						try {
							while (iterator.hasNext()) {
								Tuple2<String, Long> line = iterator.next();
								String l = line._1;
								Long index = line._2;

								MultiEventCoreDTO multiEventCoreDTO = DTOConverter
										.cefConverter(l, clientProp, fieldsMap);

								if (!StringUtils.isEmpty(multiEventCoreDTO
										.getCustomerName())) {
									String custName = multiEventCoreDTO
											.getCustomerName().trim();
									try {
										String custSplit[] = custName
												.split("_");
										if (custSplit != null
												&& custSplit.length > 0
												&& StringUtils
														.isNumeric(custSplit[0])) {
											multiEventCoreDTO
													.setCustomerId(custSplit[0]);
											String cust = map.get(Integer
													.valueOf(custSplit[0]));
											multiEventCoreDTO
													.setClientTopic(cust);
											multiEventCoreDTO.setCustomer(cust);
											cust = null;
										}
										custSplit = null;
									} catch (Exception e) {
										log.error(e.getMessage());
									}
									custName = null;
								}

								if (!StringUtils.isEmpty(multiEventCoreDTO
										.getClientTopic())) {
									Timestamp eventTime = null;
									if (multiEventCoreDTO.getEventTime() != null) {
										eventTime = multiEventCoreDTO
												.getEventTime();
									} else {
										eventTime = new Timestamp(
												new java.util.Date().getTime());
										multiEventCoreDTO
												.setEventTime(eventTime);
									}

									StringBuilder tIdSbr = new StringBuilder(
											eventTime.getTime() + "");
									multiEventCoreDTO.setThreatId(Long
											.parseLong(index
													+ ""
													+ tIdSbr.delete(0, 2)
															.reverse()
															.toString()));
									tIdSbr.delete(0, tIdSbr.length());
									tIdSbr = null;

									if (!StringUtils.isEmpty(clientProp
											.getProperty("geoLocationEnabled"))
											&& clientProp.getProperty(
													"geoLocationEnabled")
													.equalsIgnoreCase("true")
											&& databaseReader != null) {

										StringBuilder geoBlr = new StringBuilder();
										if (!StringUtils
												.isEmpty(multiEventCoreDTO
														.getSourceAddress())) {
											try {
												CityResponse srcGeoResponse = databaseReader.city(InetAddress
														.getByName(multiEventCoreDTO
																.getSourceAddress()));

												if (srcGeoResponse != null) {
													multiEventCoreDTO
															.setSourceGeoCityName(srcGeoResponse
																	.getCity()
																	.toString());
													multiEventCoreDTO
															.setSourceGeoCountryName(srcGeoResponse
																	.getCountry()
																	.toString());

													geoBlr.append("sourceGeoCityName="
															+ srcGeoResponse
																	.getCity()
																	.toString());
													geoBlr.append(", sourceGeoCountryName="
															+ srcGeoResponse
																	.getCountry()
																	.toString());
												}
											} catch (AddressNotFoundException e) {
												// log.error(e.getMessage());
											} catch (Exception e) {
												log.error("Error :"
														+ e.getMessage());
											}
										}

										if (!StringUtils.isEmpty(multiEventCoreDTO
												.getDestinationAddress())) {
											try {
												CityResponse destGeoResponse = databaseReader.city(InetAddress
														.getByName(multiEventCoreDTO
																.getDestinationAddress()));

												if (destGeoResponse != null) {
													multiEventCoreDTO
															.setDestinationGeoCityName(destGeoResponse
																	.getCity()
																	.toString());
													multiEventCoreDTO
															.setDestinationGeoCountryName(destGeoResponse
																	.getCountry()
																	.toString());

													geoBlr.append(", destinationGeoCityName="
															+ destGeoResponse
																	.getCity()
																	.toString());
													geoBlr.append(", destinationGeoCountryName="
															+ destGeoResponse
																	.getCountry()
																	.toString());
												}
											} catch (AddressNotFoundException e) {
												// log.error(e.getMessage());
											} catch (Exception e) {
												log.error("Error :"
														+ e.getMessage());
											}
										}

										String[] dt = multiEventCoreDTO
												.getDataArray();
										dt[dt.length - 1] = ("geoDetails==" + geoBlr
												.toString());

										geoBlr.delete(0, geoBlr.length());
										geoBlr = null;

										multiEventCoreDTO.setDataArray(dt);
									}

									Timestamp loggerRT = multiEventCoreDTO
											.getDeviceReceiptTime();
									Timestamp evntTime = multiEventCoreDTO
											.getEventTime();
									Timestamp agentRT = multiEventCoreDTO
											.getAgentReceiptTime();

									int loggerTimeinMins = 0;
									if (loggerRT != null && evntTime != null) {
										long timeMilis = evntTime.getTime()
												- loggerRT.getTime();
										loggerTimeinMins = (int) TimeUnit.MILLISECONDS
												.toMinutes(timeMilis);
									}

									int agentTimeinMins = 0;
									if (agentRT != null && loggerRT != null) {
										long timeMilis = loggerRT.getTime()
												- agentRT.getTime();
										agentTimeinMins = (int) TimeUnit.MILLISECONDS
												.toMinutes(timeMilis);
									}

									// timeMins = (int) (Math.random()*(7 - 1))
									// + 1;
									if (loggerTimeinMins >= Integer.parseInt(clientProp
											.getProperty("loggerDelayThreshold"))
											|| agentTimeinMins >= Integer.parseInt(clientProp
													.getProperty("loggerDelayThreshold"))) {
										log.debug("EventTime - LoggerReceiptTime = "
												+ loggerTimeinMins);
										if (loggerTimeinMins >= Integer.parseInt(clientProp
												.getProperty("loggerDelayThreshold"))) {
											multiEventCoreDTO
													.setLogDelay("Yes");
											multiEventCoreDTO
													.setRtdelayFlag("rt");
										}
										log.debug("LoggerReceiptTime - AgentReceiptTime = "
												+ agentTimeinMins);
										if (agentTimeinMins >= Integer.parseInt(clientProp
												.getProperty("loggerDelayThreshold"))) {
											multiEventCoreDTO
													.setLogDelay("Yes");
											multiEventCoreDTO
													.setArtdelayFlag("art");

										}
									} else {
										multiEventCoreDTO.setLogDelay("No");
									}

									log.debug("MultiEventCoreDTO: "
											+ multiEventCoreDTO);

									multiEventCoreDTOList
											.add(multiEventCoreDTO);
								}
							}
						} finally {
							if (databaseReader != null) {
								databaseReader.close();
								databaseReader = null;
							}
						}

						return multiEventCoreDTOList.iterator();
					}
				});

		// dtoStream.persist(StorageLevel.MEMORY_ONLY_SER());
		// dtoStream.print(1);
		DataCarrier<JavaRDD<MultiEventCoreDTO>> newDataCarrier = new BaseDataCarrier<JavaRDD<MultiEventCoreDTO>>(
				dtoStream, null);
		return newDataCarrier;
	}
}
