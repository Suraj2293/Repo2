/**
 * 
 */
package net.paladion.steps;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.FinalInsertDaoImpl;
import net.paladion.model.MultiEventCoreDTO;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * @author ankush
 *
 */
@Slf4j
public class FinalInsertStep implements Serializable {

	private static final long serialVersionUID = 1L;

	private final Properties clientProp;
	private FinalInsertDaoImpl finalInsertDaoImpl;

	public FinalInsertStep(Properties clientProp1) {
		super();
		clientProp = clientProp1;
		this.finalInsertDaoImpl = new FinalInsertDaoImpl(clientProp);
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

						PreparedStatement psRuleMatch = null;
						PreparedStatement psParentChild = null;
						Connection connection = null;

						try {
							Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
							connection = DriverManager.getConnection(clientProp
									.getProperty("phoenix.database.url"));
							connection.setAutoCommit(false);

							StringBuilder sbr1 = new StringBuilder();

							sbr1.append("upsert into ");
							sbr1.append(clientProp
									.getProperty("phoenix.schemaname"));
							sbr1.append(".simulator_threat_rule_match (id, ti, mr, et, cu, bh, bd, dt) values ");
							sbr1.append("(next value for ");
							sbr1.append(clientProp
									.getProperty("phoenix.schemaname"));
							sbr1.append(".simulator_threat_rule_match_id,?,?,?,?,?,?,?)");

							psRuleMatch = connection.prepareStatement(sbr1
									.toString());
							sbr1.delete(0, sbr1.length());
							sbr1 = null;

							StringBuilder sbr2 = new StringBuilder();

							sbr2.append("upsert into ");
							sbr2.append(clientProp
									.getProperty("phoenix.schemaname"));
							sbr2.append(".simulator_threat_rule_parent_child_mapping (ti, mr, dt, et, tn, tt, rr) values ");
							sbr2.append("(?,?,?,?,?,?,?)");

							psParentChild = connection.prepareStatement(sbr2
									.toString());
							sbr2.delete(0, sbr2.length());
							sbr2 = null;
							Integer batch = Integer.parseInt(clientProp
									.getProperty("batch.insert.size"));
							Integer commitSize = 0;
							while (multiEventCoreDTOs.hasNext()) {
								MultiEventCoreDTO multiEventCoreDTO = multiEventCoreDTOs
										.next();
								if (!multiEventCoreDTO
										.getRuleMatchDTO()
										.getRuleType()
										.equalsIgnoreCase(
												clientProp
														.getProperty("batchRuleName"))) {
									finalInsertDaoImpl.insertData(
											multiEventCoreDTO, connection,
											psParentChild, psRuleMatch);
									commitSize++;
									if (commitSize % batch == 0) {
										connection.commit();
									}
								}

								multiEventCoreDTOList.add(multiEventCoreDTO);
							}
							connection.commit();
						} catch (SQLException | ClassNotFoundException e) {
							log.error("Error in insertThreatRawData method: "
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
							if (psParentChild != null) {
								try {
									psParentChild.close();
									psParentChild = null;
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
