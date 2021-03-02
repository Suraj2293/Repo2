/**
 * 
 */
package net.paladion.steps;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.RuleMatchDTO;
import net.paladion.rule.KieSessionService;
import net.paladion.util.ConvertIpToLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.kie.api.runtime.StatelessKieSession;

/**
 * @author ankush
 *
 */
@SuppressWarnings("serial")
@Slf4j
public class RuleExecuterStep implements Serializable {

	private final Properties clientProp;

	public RuleExecuterStep(Properties clientProp1) {
		super();
		clientProp = clientProp1;
	}

	public JavaRDD<MultiEventCoreDTO> transform(JavaRDD<MultiEventCoreDTO> rdd) {
		JavaRDD<MultiEventCoreDTO> rdd1 = rdd
				.mapPartitions(new FlatMapFunction<Iterator<MultiEventCoreDTO>, MultiEventCoreDTO>() {
					private static final long serialVersionUID = 1L;

					// long startTime = System.currentTimeMillis();

					@Override
					public Iterator<MultiEventCoreDTO> call(
							Iterator<MultiEventCoreDTO> multiEventCoreDTOs)
							throws Exception {
						List<MultiEventCoreDTO> multiEventCoreDTOList = new ArrayList<MultiEventCoreDTO>();

						StatelessKieSession ksession = KieSessionService
								.getKieSession(clientProp);

						Connection connection = null;
						// PreparedStatement psmtZone = null;
						// PreparedStatement psmtGeo = null;

						try {
							Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
							connection = DriverManager.getConnection(clientProp
									.getProperty("phoenix.database.url"));

							ksession.setGlobal("Connection", connection);

							Map<String, String> networkZoneMap = KieSessionService.networkZoneMap;
							// psmtZone =
							// connection
							// .prepareStatement("select * from (select 'src' as at, zo from "
							// + clientProp.getProperty("phoenix.schemaname")
							// +
							// ".network_zone_master WHERE tin >= ? and cu like ? ORDER BY tin ASC LIMIT 1)as a union all select * from (select 'dest' as at,zo from "
							// + clientProp.getProperty("phoenix.schemaname")
							// +
							// ".network_zone_master WHERE tin >= ? and cu like ? ORDER BY tin ASC LIMIT 1)as b");
							//
							// psmtGeo =
							// connection
							// .prepareStatement("select * from (select 'src' as at, con, cin from "
							// + clientProp.getProperty("phoenix.schemaname")
							// +
							// ".geo_location_master WHERE tin >= ? ORDER BY tin ASC LIMIT 1)as a union all select * from (select 'dest' as at, con, cin from "
							// + clientProp.getProperty("phoenix.schemaname")
							// +
							// ".geo_location_master WHERE tin >= ? ORDER BY tin ASC LIMIT 1)as b");

							while (multiEventCoreDTOs.hasNext()) {
								MultiEventCoreDTO multiEventCoreDTO = null;
								try {
									multiEventCoreDTO = multiEventCoreDTOs
											.next();

									if (!StringUtils.isEmpty(multiEventCoreDTO
											.getCustomerName())) {
										multiEventCoreDTO
												.setSourceAddressLong(ConvertIpToLong
														.stringIpToLong(multiEventCoreDTO
																.getSourceAddress()));

										multiEventCoreDTO
												.setDestinationAddressLong(ConvertIpToLong
														.stringIpToLong(multiEventCoreDTO
																.getDestinationAddress()));

										if (networkZoneMap != null
												&& !networkZoneMap.isEmpty()) {
											try {
												multiEventCoreDTO
														.setSourceZone(networkZoneMap.get(ConvertIpToLong
																.stringIpToLong(multiEventCoreDTO
																		.getSourceAddress())
																+ "_"
																+ multiEventCoreDTO
																		.getCustomerId()
																		.substring(
																				0,
																				3)));

												multiEventCoreDTO
														.setDestinationZone(networkZoneMap.get(ConvertIpToLong
																.stringIpToLong(multiEventCoreDTO
																		.getDestinationAddress())
																+ "_"
																+ multiEventCoreDTO
																		.getCustomerId()
																		.substring(
																				0,
																				3)));
											} catch (Exception e) {
												log.error("Error while fetching network zone details: "
														+ e.getMessage());
											}
										}

										ksession.execute(multiEventCoreDTO);

										if (multiEventCoreDTO
												.getRuleMatchDTOList() != null
												&& !multiEventCoreDTO
														.getRuleMatchDTOList()
														.isEmpty()) {
											int listCount = multiEventCoreDTO
													.getRuleMatchDTOList()
													.size();
											for (RuleMatchDTO r : multiEventCoreDTO
													.getRuleMatchDTOList()) {
												if (listCount > 1) {
													MultiEventCoreDTO m = (MultiEventCoreDTO) multiEventCoreDTO
															.clone();
													m.setRuleMatchDTO(r);
													multiEventCoreDTOList
															.add(m);
												} else {
													multiEventCoreDTO
															.setRuleMatchDTO(r);
													multiEventCoreDTO
															.setRuleMatchDTOList(null);
													multiEventCoreDTOList
															.add(multiEventCoreDTO);
												}
											}
											listCount = 0;
										}
									}
								} catch (NumberFormatException e) {
									log.error("NumberFormatException in Rule Execution: "
											+ e.getMessage());
								} catch (Exception e) {
									log.error("Error in Rule Execution: "
											+ e.getMessage());
								}
							}

							networkZoneMap = null;
						} catch (Exception e) {
							log.error("Error: " + e.getMessage());
						} finally {
							// if (psmtGeo != null) {
							// try {
							// psmtGeo.close();
							// psmtGeo = null;
							// } catch (Exception e) {
							// log.error(e.getMessage());
							// }
							// }
							// if (psmtZone != null) {
							// try {
							// psmtZone.close();
							// psmtZone = null;
							// } catch (Exception e) {
							// log.error(e.getMessage());
							// }
							// }
							if (connection != null) {
								try {
									connection.close();
									connection = null;
								} catch (Exception e) {
									log.error(e.getMessage());
								}
							}
						}
						// long endTime = System.currentTimeMillis();
						// getTotalTime(startTime, endTime,
						// Thread.currentThread()
						// .getStackTrace()[1].getMethodName());
						return multiEventCoreDTOList.iterator();
					}
				});
		// dtoStreamNew.persist(StorageLevel.MEMORY_ONLY_SER());
		// dtoStreamNew.print(1);

		return rdd1;
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
