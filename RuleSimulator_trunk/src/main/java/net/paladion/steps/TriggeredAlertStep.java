package net.paladion.steps;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.ThreatRuleMatchDTO;
import net.paladion.util.DTOConverter;
import net.paladion.util.UtilityTools;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;

@Slf4j
public class TriggeredAlertStep implements Serializable {

	private static final long serialVersionUID = 1L;
	private final Properties clientProp;

	public TriggeredAlertStep(Properties clientProp1) {
		super();
		clientProp = clientProp1;
	}

	public JavaRDD<MultiEventCoreDTO> tranform(JavaRDD<ThreatRuleMatchDTO> rdd) {
		JavaRDD<MultiEventCoreDTO> rdd1 = rdd
				.mapPartitions(new FlatMapFunction<Iterator<ThreatRuleMatchDTO>, MultiEventCoreDTO>() {
					private static final long serialVersionUID = 1L;

					// long startTime = System.currentTimeMillis();

					@Override
					public Iterator<MultiEventCoreDTO> call(
							Iterator<ThreatRuleMatchDTO> t) throws Exception {
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

						ThreatRuleMatchDTO row = null;
						try {
							while (t.hasNext()) {
								String rawData = "";
								row = t.next();
								rawData = row.getDt();
								if (!StringUtils.isEmpty(rawData)
										&& rawData != "") {
									MultiEventCoreDTO multiEventCoreDTO = DTOConverter
											.convertToMultiCore(clientProp,
													row.getDt(), fieldsMap);
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
												ConcurrentHashMap<Integer, String> map = fileToTopicMap(clientProp
														.getProperty("custTopicMap"));
												String cust = map.get(Integer
														.valueOf(custSplit[0]));
												multiEventCoreDTO
														.setClientTopic(cust);
												multiEventCoreDTO
														.setCustomer(cust);
												cust = null;
												map = null;
											}
											custSplit = null;
										} catch (Exception e) {
											log.error(e.getMessage());
										}
										custName = null;
									}
									multiEventCoreDTO.setThreatId(row.getTi());
									multiEventCoreDTO.setEventTime(row.getEt());

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

									// log.debug("FirstStep MultiEventCoreDTO: "
									// + multiEventCoreDTO);

									multiEventCoreDTOList
											.add(multiEventCoreDTO);
								} else {
									log.error("Null dt for threatid: "
											+ row.getTi());
								}

							}
						} finally {
							if (databaseReader != null) {
								databaseReader.close();
								databaseReader = null;
							}

							if (row != null) {
								row = null;
							}
						}
						// long endTime = System.currentTimeMillis();
						// getTotalTime(startTime, endTime,
						// Thread.currentThread()
						// .getStackTrace()[1].getMethodName());
						return multiEventCoreDTOList.iterator();
					}
				});
		return rdd1;
	}

	/**
	 * 
	 * @param filepath
	 * @return
	 * @throws IOException
	 */
	private ConcurrentHashMap<Integer, String> fileToTopicMap(String filepath)
			throws IOException {
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

		} catch (IOException e) {
			log.error("Error in Topic mapping: " + e.getMessage());
		}
		fileInputStream.close();
		return topicMapRange;
	}

	/**
	 * This method calculate the 2 dates and takes time difference and print the
	 * time in hours, minutes and seconds format
	 * 
	 * @param startTime
	 *            : when the process has started
	 * @param endTime
	 *            : when the process has ended
	 */
	public static void getTotalTime(final long startTime, final long endTime,
			final String methodName) {

		long duration = (endTime - startTime);
		final long diffSeconds = duration / 1000 % 60;
		final long diffMinutes = duration / (60 * 1000) % 60;
		final long diffHours = duration / (60 * 60 * 1000) % 24;

		log.info("Method Name : " + methodName);
		log.info("Total time taken for process :- " + diffHours + " hours, "
				+ diffMinutes + " minutes," + diffSeconds + " seconds");
	}

}
