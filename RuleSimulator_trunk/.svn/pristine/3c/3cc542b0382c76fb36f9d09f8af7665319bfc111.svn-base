/**
 * 
 */
package net.paladion.dao;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.RuleMatchDTO;
import net.paladion.model.RuleSimulationDetailsDTO;
import net.paladion.rule.InActiveConditions;

import org.apache.commons.lang.StringUtils;

/**
 * @author ankush
 *
 */
@Slf4j
public class SiemRuleDaoImpl implements Serializable {
	private static final long serialVersionUID = 1L;
	private final Properties clientProp;

	public SiemRuleDaoImpl(Properties clientProps) {
		super();
		clientProp = clientProps;
	}

	/**
	 * This method is used to create drl string for SIEM rules using database
	 * tables
	 * 
	 * @param conn
	 * 
	 * @return
	 */
	public List<RuleMatchDTO> getRulesDTOList(Connection conn) {
		PreparedStatement psmt = null;
		ResultSet rs = null;
		List<RuleMatchDTO> ruleMatchDTOList = new ArrayList<RuleMatchDTO>();
		RuleSimulationDetailsDTO ruleSimulationDetailsDTO = getRuleSimulationDetails(conn);
		Integer ri = ruleSimulationDetailsDTO.getRuleId();
		String rn = ruleSimulationDetailsDTO.getRuleName();
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss.SSS");
			if (Boolean
					.parseBoolean(clientProp.getProperty("convertDateToUtc"))) {
				sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			}

			String secRuleQuery = "";
			secRuleQuery = "select rule_id,processing_mode,rule_name,rule_action,is_enable_aggregation,followed_by_rule_id"
					+ ",drools_rule,drools_follwed_by_rule,threshold_count,threshold_time,event_A_count,event_B_count,threshold_time_unit,"
					+ "followed_by_threshold_time,followed_by_threshold_time_unit,unique_fields,aggregate_fields,threshold_condition,list_name"
					+ ",severity,recommendation from "
					+ clientProp.getProperty("phoenix.schemaname")
					+ ".security_rule where ";
			StringBuilder secBuilder = new StringBuilder();
			secBuilder.append(secRuleQuery);
			if (ri != null && ri != 0)
				secBuilder.append("rule_id = ").append(
						ruleSimulationDetailsDTO.getRuleId());
			if (!StringUtils.isEmpty(rn))
				secBuilder.append("rule_name = '")
						.append(ruleSimulationDetailsDTO.getRuleName())
						.append("'");
			psmt = conn.prepareStatement(secBuilder.toString());
			rs = psmt.executeQuery();

			if (rs != null) {
				while (rs.next()) {
					int ruleId = rs.getInt("rule_id");
					try {
						RuleMatchDTO ruleMatchDTO = new RuleMatchDTO();
						boolean isFollowedByRule = false;
						String processingMode = rs.getString("processing_mode");
						String ruleName = rs.getString("rule_name");
						String ruleAction = rs.getString("rule_action");
						boolean isEnableAggregation = rs
								.getBoolean("is_enable_aggregation");
						int followedByRuleId = rs.getInt("followed_by_rule_id");
						String droolsRule = rs.getString("drools_rule");
						String droolsFollwedByRule = rs
								.getString("drools_follwed_by_rule");
						int thresholdCount = rs.getInt("threshold_count");
						int thresholdTime = rs.getInt("threshold_time");
						int eventACount = rs.getInt("event_A_count");
						int eventBCount = rs.getInt("event_B_count");
						String thresholdTimeUnit = rs
								.getString("threshold_time_unit");
						int followedByThresholdTime = rs
								.getInt("followed_by_threshold_time");
						String followedByThresholdTimeUnit = rs
								.getString("followed_by_threshold_time_unit");
						String uniqueFields = rs.getString("unique_fields");
						String aggregateFields = rs
								.getString("aggregate_fields");
						String thresholdCondition = rs
								.getString("threshold_condition");
						String listName = rs.getString("list_name");
						String severity = rs.getString("severity");
						String recommendation = rs.getString("recommendation");

						if (!StringUtils.isEmpty(ruleName)) {
							ruleName = ruleName.trim();
						}

						ruleMatchDTO.setRuleId(ruleId);
						ruleMatchDTO.setRuleType(processingMode);
						if (!StringUtils.isEmpty(droolsFollwedByRule)) {
							ruleMatchDTO.setRuleSubType("FollowedBy Rule");
						} else {
							ruleMatchDTO.setRuleSubType("Standard Rule");
						}

						if (followedByRuleId == 0
								&& !StringUtils.isEmpty(droolsFollwedByRule)) {
							ruleMatchDTO.setRuleName(ruleName
									+ " FollowedBy Rule");
							isFollowedByRule = true;
						} else {
							ruleMatchDTO.setRuleName(ruleName);
						}

						if (followedByRuleId != 0) {
							String temStr = null;
							if (!StringUtils.isEmpty(droolsFollwedByRule)) {
								temStr = droolsFollwedByRule.replaceAll(
										"\\bDomainList\\b", "HostList");
								if (temStr != null) {
									temStr = temStr.replaceAll(
											"\\bIPListGreaterThanEqual70\\b",
											"iplistgte70");
									temStr = temStr
											.replaceAll(
													"\\bDomainListGreaterThanEqual70\\b",
													"hostlistgte70");
									temStr = temStr.replaceAll(
											"\\bIPListLessThan70\\b",
											"iplistlt70");
									temStr = temStr.replaceAll(
											"\\bDomainListLessThan70\\b",
											"hostlistlt70");
									temStr = temStr
											.replaceAll(" InCSVActiveList",
													" RuleCustomCondition.InCSVActiveList");
									temStr = temStr
											.replaceAll(" NotInCSVActiveList",
													" RuleCustomCondition.NotInCSVActiveList");
									temStr = temStr
											.replaceAll(" InActiveList\\(",
													" InActiveConditions.InActiveList\\(\\$m,Connection,");
									temStr = temStr
											.replaceAll(" NotInActiveList\\(",
													" InActiveConditions.NotInActiveList\\(\\$m,Connection,");
									ruleMatchDTO.setRuleCondition(temStr);
								}
							}
						} else {
							if (!StringUtils.isEmpty(droolsRule)) {
								String temStr = droolsRule.replaceAll(
										"\\bDomainList\\b", "HostList");
								if (temStr != null) {
									temStr = temStr.replaceAll(
											"\\bIPListGreaterThanEqual70\\b",
											"iplistgte70");
									temStr = temStr
											.replaceAll(
													"\\bDomainListGreaterThanEqual70\\b",
													"hostlistgte70");
									temStr = temStr.replaceAll(
											"\\bIPListLessThan70\\b",
											"iplistlt70");
									temStr = temStr.replaceAll(
											"\\bDomainListLessThan70\\b",
											"hostlistlt70");
									temStr = temStr
											.replaceAll(" InCSVActiveList",
													" RuleCustomCondition.InCSVActiveList");
									temStr = temStr
											.replaceAll(" NotInCSVActiveList",
													" RuleCustomCondition.NotInCSVActiveList");
									temStr = temStr
											.replaceAll(" InActiveList\\(",
													" InActiveConditions.InActiveList\\(\\$m,Connection,");
									temStr = temStr
											.replaceAll(" NotInActiveList\\(",
													" InActiveConditions.NotInActiveList\\(\\$m,Connection,");
								}
								ruleMatchDTO.setRuleCondition(temStr);

							}
						}

						if (isEnableAggregation) {
							ruleMatchDTO.setThresholdApplicable("yes");
						} else {
							ruleMatchDTO.setThresholdApplicable("No");
						}

						if (!StringUtils.isEmpty(droolsFollwedByRule)) {
							if (isFollowedByRule) {
								ruleMatchDTO.setEventAThresholdValue(0);
								ruleMatchDTO
										.setEventBThresholdValue(eventBCount);
								ruleMatchDTO.setTimeWindowValue(0);
								ruleMatchDTO.setTimeWindowUnit("NA");
							} else {
								ruleMatchDTO
										.setEventAThresholdValue(eventACount);
								ruleMatchDTO
										.setEventBThresholdValue(eventBCount);
								ruleMatchDTO
										.setTimeWindowValue(followedByThresholdTime);

								if (!StringUtils
										.isEmpty(followedByThresholdTimeUnit)) {
									ruleMatchDTO
											.setTimeWindowUnit(followedByThresholdTimeUnit);
								} else {
									ruleMatchDTO.setTimeWindowUnit("NA");
								}
							}

						} else {
							ruleMatchDTO
									.setEventAThresholdValue(thresholdCount);
							ruleMatchDTO.setEventBThresholdValue(eventBCount);
							ruleMatchDTO.setTimeWindowValue(thresholdTime);

							ruleMatchDTO.setTimeWindowUnit(thresholdTimeUnit);
						}

						ruleMatchDTO.setRulePriority(5000);
						if (!isFollowedByRule) {
							if (isEnableAggregation) {
								ruleMatchDTO
										.setThresholdCondition(thresholdCondition);
							}

							if (!StringUtils.isEmpty(aggregateFields)) {
								ruleMatchDTO.setIdenticalColStr(aggregateFields
										.trim());
							}

							if (!StringUtils.isEmpty(uniqueFields)) {
								ruleMatchDTO.setDistinctColStr(uniqueFields
										.trim());
							}

							if (!StringUtils.isEmpty(listName)) {
								String activeListCond = getFieldList(listName
										.trim());
								ruleMatchDTO.setLightweightStr(activeListCond);
							}

							if (followedByRuleId != 0) {
								ruleMatchDTO.setFollwedByRuleName(String
										.valueOf(followedByRuleId));
							}
							ruleMatchDTO.setSeverity(severity);
							ruleMatchDTO.setRecommendations(recommendation);
						}

						if (!StringUtils.isEmpty(ruleAction)) {
							if (ruleAction.trim().equalsIgnoreCase(
									"Send to Triage")) {
								ruleMatchDTO.setActions("trigger");
							} else if (ruleAction.trim().equalsIgnoreCase(
									"Add to Active List")) {
								ruleMatchDTO.setActions("activelist");
							} else if (ruleAction.trim().equalsIgnoreCase(
									"Send to Triage, Add to Active List")) {
								ruleMatchDTO.setActions("trigger,activelist");
							}
						}

						if (StringUtils.isEmpty(ruleMatchDTO
								.getTimeWindowUnit())) {
							ruleMatchDTO.setTimeWindowUnit("NA");
						}
						if (StringUtils.isEmpty(ruleMatchDTO
								.getThresholdCondition())) {
							ruleMatchDTO.setThresholdCondition("NA");
						}
						if (StringUtils.isEmpty(ruleMatchDTO
								.getThresholdCondition())) {
							ruleMatchDTO.setThresholdCondition("NA");
						}
						if (StringUtils.isEmpty(ruleMatchDTO
								.getIdenticalColStr())) {
							ruleMatchDTO.setIdenticalColStr("NA");
						}
						if (StringUtils.isEmpty(ruleMatchDTO
								.getDistinctColStr())) {
							ruleMatchDTO.setDistinctColStr("NA");
						}
						if (StringUtils.isEmpty(ruleMatchDTO
								.getLightweightStr())) {
							ruleMatchDTO.setLightweightStr("NA");
						}
						if (StringUtils.isEmpty(ruleMatchDTO
								.getFollwedByRuleName())) {
							ruleMatchDTO.setFollwedByRuleName("NA");
						}
						if (StringUtils.isEmpty(ruleMatchDTO.getSeverity())) {
							ruleMatchDTO.setSeverity("NA");
						}
						if (StringUtils.isEmpty(ruleMatchDTO
								.getRecommendations())) {
							ruleMatchDTO.setRecommendations("NA");
						}
						if (StringUtils.isEmpty(ruleMatchDTO.getActions())) {
							ruleMatchDTO.setActions("NA");
						}

						ruleMatchDTOList.add(ruleMatchDTO);
					} catch (Exception e) {
						sendToRCEDiNotification(
								conn,
								clientProp.getProperty("phoenix.schemaname"),
								"Exception occured while loading the rules, for rule id: "
										+ ruleId + " exception is: "
										+ e.getMessage(),
								clientProp.getProperty("exceptionEmailIds"));
						log.error("Error in CsvFileWriter with rule_id- "
								+ ruleId + " Error is: " + e.getMessage());
					}
				}
			}
			secBuilder.setLength(0);
			secBuilder = null;
			sdf = null;
			ruleSimulationDetailsDTO = null;
		} catch (Exception e) {
			sendToRCEDiNotification(conn,
					clientProp.getProperty("phoenix.schemaname"),
					"Exception occured while loading the rule list from database exception is: "
							+ e.getMessage(),
					clientProp.getProperty("exceptionEmailIds"));
			log.error(
					"Exception occured in : {} in Method {}() Exception is : {}",
					new Object[] {
							this.getClass(),
							Thread.currentThread().getStackTrace()[1]
									.getMethodName(), e.getMessage() });
		} finally {
			try {
				if (rs != null) {
					rs.close();
					rs = null;
				}
				if (psmt != null) {
					psmt.close();
					psmt = null;
				}
			} catch (SQLException e) {
				log.error(
						"Exception occured in : {} in Method {}() Exception is : {}",
						new Object[] {
								this.getClass(),
								Thread.currentThread().getStackTrace()[1]
										.getMethodName(), e.getMessage() });
			}
		}

		return ruleMatchDTOList;
	}

	/**
	 * 
	 * @param listName
	 * @return
	 * @throws ConnectorCommonException
	 */
	private String getFieldList(String listName) throws Exception {
		StringBuilder strBlr = new StringBuilder();
		String SEPARATOR = "~###~";
		strBlr.append(listName).append("~####~");
		if (listName != null && !listName.isEmpty()) {
			Map<String, String> map = InActiveConditions.getInActiveListMap();
			if (map == null) {
				map = new HashMap<String, String>();
			}

			Integer listId = null;
			PreparedStatement psmt1 = null;
			PreparedStatement psmt2 = null;
			Connection connection = null;
			ResultSet rs1 = null;
			ResultSet rs2 = null;
			try {
				Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
				connection = DriverManager.getConnection(clientProp
						.getProperty("phoenix.database.url"));
				psmt1 = connection.prepareStatement("select id from "
						+ clientProp.getProperty("phoenix.schemaname")
						+ ".security_rule_activeList where list_name='"
						+ listName.trim() + "'");

				rs1 = psmt1.executeQuery();

				if (rs1 != null) {
					while (rs1.next()) {
						listId = rs1.getInt("id");
					}
				}

				if (listId != null && listId > 0) {
					psmt2 = connection
							.prepareStatement("select field_name from "
									+ clientProp
											.getProperty("phoenix.schemaname")
									+ ".security_rule_list_field_mapping where activeList_id="
									+ listId);

					rs2 = psmt2.executeQuery();

					StringBuilder sbr = new StringBuilder();
					if (rs2 != null) {
						boolean first = true;
						while (rs2.next()) {
							String fieldName = rs2.getString("field_name");
							strBlr.append(fieldName);
							if (!first) {
								strBlr.append(SEPARATOR);
								sbr.append(",");
							} else {
								first = false;
							}
							sbr.append(fieldName.trim() + "."
									+ fieldName.trim());
						}

						map.put(listName, sbr.toString());
					}
				}
			} catch (Exception e) {
				log.error("Error is : {} in class {}", e.getMessage(),
						this.getClass());
				throw new Exception("Exception Occured", e);
			} finally {
				if (connection != null) {
					try {
						connection.close();
						connection = null;
					} catch (Exception e) {
						log.error(e.getMessage());
					}
				}
				if (psmt1 != null) {
					try {
						psmt1.close();
						psmt1 = null;
					} catch (Exception e) {
						log.error(e.getMessage());
					}
				}
				if (rs1 != null) {
					try {
						rs1.close();
						rs1 = null;
					} catch (Exception e) {
						log.error(e.getMessage());
					}
				}
				if (psmt2 != null) {
					try {
						psmt2.close();
						psmt2 = null;
					} catch (Exception e) {
						log.error(e.getMessage());
					}
				}
				if (rs2 != null) {
					try {
						rs2.close();
						rs2 = null;
					} catch (Exception e) {
						log.error(e.getMessage());
					}
				}
			}
		}

		return strBlr.toString();
	}

	/**
	 * 
	 * @param conn
	 * @return
	 */
	public boolean checkForNewRulesInAutomated(Connection conn) {
		PreparedStatement psCheck = null;
		ResultSet rsCheck = null;
		boolean reloadRule = false;

		try {
			SimpleDateFormat sdf = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss.SSS");
			if (Boolean
					.parseBoolean(clientProp.getProperty("convertDateToUtc"))) {
				sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			}

			String dateStr = sdf.format(new Date());

			StringBuilder sqlbldr = new StringBuilder();

			sqlbldr.append("select updated_date from ");
			sqlbldr.append(clientProp.getProperty("phoenix.schemaname"));
			sqlbldr.append(".security_rule where");
			sqlbldr.append(" updated_date >= to_date('");
			sqlbldr.append(dateStr);
			sqlbldr.append("')-(");
			sqlbldr.append(clientProp
					.getProperty("checkUpdatedRulesForMinutes"));
			sqlbldr.append(")/(24.0*60) limit 1");

			psCheck = conn.prepareStatement(sqlbldr.toString());
			rsCheck = psCheck.executeQuery();

			if (rsCheck != null) {
				while (rsCheck.next()) {
					reloadRule = true;
				}
			}
			sdf = null;
			dateStr = null;
			sqlbldr.delete(0, sqlbldr.length());
			sqlbldr = null;
		} catch (Exception e) {
			log.error(
					"Exception occured in : {} in Method {}() Exception is : {}",
					new Object[] {
							this.getClass(),
							Thread.currentThread().getStackTrace()[1]
									.getMethodName(), e.getMessage() });
		} finally {
			try {
				if (rsCheck != null) {
					rsCheck.close();
					rsCheck = null;
				}
				if (psCheck != null) {
					psCheck.close();
					psCheck = null;
				}
			} catch (SQLException e) {
				log.error(
						"Exception occured in : {} in Method {}() Exception is : {}",
						new Object[] {
								this.getClass(),
								Thread.currentThread().getStackTrace()[1]
										.getMethodName(), e.getMessage() });
			}
		}
		return reloadRule;
	}

	/**
	 *
	 * @param conn
	 * @param phoenixSchema
	 * @param tableConetent
	 * @param instanceName
	 * @param toEmail
	 */
	public void sendToRCEDiNotification(Connection conn, String phoenixSchema,
			String tableConetent, String toEmail) {
		String instanceName = null;
		try {
			instanceName = java.net.InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			log.error(e1.getMessage());
		}
		PreparedStatement ps = null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		String str = "upsert into "
				+ phoenixSchema
				+ ".email(rd,to,cc,bcc,eti,ci,cu,eod,st,re,rel,cd,pd) values(NEXT VALUE FOR "
				+ phoenixSchema
				+ ".email_sequence,?,NULL,NULL,5,0,?,?,1,NULL,1,now(),now())";
		Map<String, String> emailOtherDetails = new HashMap<String, String>();
		emailOtherDetails.put("tableContent", tableConetent);
		emailOtherDetails.put("instanceName", instanceName);

		try {
			ObjectOutputStream ob = new ObjectOutputStream(bos);
			ob.writeObject(emailOtherDetails);
			ob.close();
		} catch (Exception e) {
			log.error("Exception while creating the binary object: "
					+ e.getMessage() + e.getClass());
		}
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(str);
			ps.setString(1, toEmail);
			ps.setString(2, instanceName);
			ps.setBytes(3, bos.toByteArray());
			ps.executeUpdate();
			conn.commit();

		} catch (Exception e) {
			log.error("Exception occured in sendToRCEDINotification(): "
					+ e.getMessage() + e.getClass());
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				log.error("Excpetion while closing ps in sendToRCEDINotification(): "
						+ e.getMessage());
			}
		}
	}

	public RuleSimulationDetailsDTO getRuleSimulationDetails(Connection conn) {
		PreparedStatement ps1 = null;
		ResultSet rs = null;
		String query = "";
		RuleSimulationDetailsDTO ruleSimulationDetailsDTO = new RuleSimulationDetailsDTO();
		query = "select rec_id,rule_id,rule_name from "
				+ clientProp.getProperty("phoenix.schemaname")
				+ ".rule_simulation_details where flag is null";
		try {
			ps1 = conn.prepareStatement(query);
			rs = ps1.executeQuery();
			if (rs != null) {
				while (rs.next()) {
					ruleSimulationDetailsDTO.setRecId(rs.getInt("rec_id"));
					ruleSimulationDetailsDTO.setRuleId(rs.getInt("rule_id"));
					ruleSimulationDetailsDTO.setRuleName(rs
							.getString("rule_name"));

				}
			}

		} catch (SQLException e) {
			log.error("Exception occured in Methhod getRuleSimulationDetails for first prepared statement due to "
					+ e.getMessage());
		} finally {
			try {
				if (rs != null) {
					rs.close();
					rs = null;
				}
				if (ps1 != null) {
					ps1.close();
					ps1 = null;
				}
			} catch (SQLException e) {
				log.error("Exception occeure while closing first prepared statement in Method getRuleSimulationDetails due to: "
						+ e.getMessage());
			}
		}
		query = null;

		PreparedStatement ps2 = null;
		String update = "";
		update = "upsert into " + clientProp.getProperty("phoenix.schemaname")
				+ ".rule_simulation_details(rec_id,flag) values(?,'Y')";
		try {
			ps2 = conn.prepareStatement(update);
			ps2.setInt(1, ruleSimulationDetailsDTO.getRecId());
			ps2.executeUpdate();
			conn.commit();
		} catch (SQLException e) {
			log.error("Exception occured in Methhod getRuleSimulationDetails for second prepared statement due to"
					+ e.getMessage());
		} finally {
			try {
				if (ps2 != null) {
					ps2.close();
					ps2 = null;
				}
			} catch (SQLException e) {
				log.error("Exception occeure while closing second prepared statement in Method getRuleSimulationDetails due to: "
						+ e.getMessage());
			}
		}
		update = null;
		return ruleSimulationDetailsDTO;
	}
}
