/**
 * 
 */
package net.paladion.steps;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.ThresholdDaoImpl;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.RuleMatchDTO;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * @author ankush
 *
 */
@Slf4j
public class ThresholdExecuterStep implements Serializable {

	private static final long serialVersionUID = 1L;

	private final Properties clientProp;
	private ThresholdDaoImpl thresholdDaoImpl;

	public ThresholdExecuterStep(Properties clientProp1) {
		super();
		clientProp = clientProp1;
		this.thresholdDaoImpl = new ThresholdDaoImpl(clientProp);
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
						Connection connection = null;

						try {
							Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
							connection = DriverManager.getConnection(clientProp
									.getProperty("phoenix.database.url"));
							connection.setAutoCommit(false);

							while (multiEventCoreDTOs.hasNext()) {
								MultiEventCoreDTO multiEventCoreDTO = multiEventCoreDTOs
										.next();
								RuleMatchDTO ruleMatchDTO = multiEventCoreDTO
										.getRuleMatchDTO();

								if (!ruleMatchDTO
										.getRuleType()
										.equalsIgnoreCase(
												clientProp
														.getProperty("batchRuleName"))
										&& !StringUtils.isEmpty(ruleMatchDTO
												.getThresholdApplicable())
										&& ruleMatchDTO
												.getThresholdApplicable()
												.equalsIgnoreCase("yes")
										&& !StringUtils.isEmpty(ruleMatchDTO
												.getTimeWindowUnit())
										&& ruleMatchDTO.getTimeWindowValue() != 0) {

									Boolean thresholdApplied = thresholdDaoImpl
											.threshodApplicable(
													multiEventCoreDTO,
													connection);
									if (thresholdApplied) {
										multiEventCoreDTOList
												.add(multiEventCoreDTO);
									}
								} else {
									if (!ruleMatchDTO
											.getRuleSubType()
											.equalsIgnoreCase(
													clientProp
															.getProperty("followedByRuleName"))) {
										multiEventCoreDTOList
												.add(multiEventCoreDTO);
									}
								}
							}
							connection.commit();
						} catch (SQLException | ClassNotFoundException e) {
							log.error("Error in insertThresholdRuleMatch: "
									+ e.getMessage());
						} finally {
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
