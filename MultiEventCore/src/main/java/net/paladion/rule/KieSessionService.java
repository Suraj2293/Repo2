/**
 * 
 */
package net.paladion.rule;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.SiemRuleDaoImpl;
import net.paladion.model.RuleMatchDTO;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.drools.core.util.StringUtils;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.Message.Level;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;

/**
 * This class is used to initialize, load and reload the kiesession.
 * 
 * @author ankush
 *
 */
@Slf4j
public class KieSessionService implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Properties clientProp;
	private static String staticDrlFileStr;
	private static SimpleDateFormat formatter = new SimpleDateFormat(
			"dd-MM-yyyy hh:mm");
	private static Map<String, Integer> ruleNameIdMap = new HashMap<String, Integer>();
	static StatelessKieSession statelessKieSession;
	private static Map<String, Integer> ipTiFeedMap = new HashMap<String, Integer>();
	private static Map<String, Integer> hostTiFeedMap = new HashMap<String, Integer>();
	private static Map<String, Integer> ipTiFeedgte70Map = new HashMap<String, Integer>();
	private static Map<String, Integer> hostTiFeedgte70Map = new HashMap<String, Integer>();
	private static Map<String, Integer> ipTiFeedlt70Map = new HashMap<String, Integer>();
	private static Map<String, Integer> hostTiFeedlt70Map = new HashMap<String, Integer>();
	public static Map<String, String> networkZoneMap = new HashMap<String, String>();

	/**
	 * 
	 * @param clientProp1
	 * @param reloadRules
	 * @param reloadTiFeed
	 * @return
	 */
	public static StatelessKieSession getKieSession(Properties clientProp1,
			Boolean reloadRules, Boolean reloadTiFeed) {
		clientProp = clientProp1;

		if (statelessKieSession == null || reloadTiFeed) {
			getTiFeedMaps();
		}

		if (statelessKieSession == null) {
			statelessKieSession = getNewKieSession(null);
			RuleCustomCondition ruleCustomCondition = new RuleCustomCondition(
					clientProp);
			InActiveConditions inActiveConditions = new InActiveConditions(
					clientProp, ipTiFeedMap, hostTiFeedMap, ipTiFeedgte70Map,
					hostTiFeedgte70Map, ipTiFeedlt70Map, hostTiFeedlt70Map);
			statelessKieSession.setGlobal("RuleCustomCondition",
					ruleCustomCondition);
			statelessKieSession.setGlobal("InActiveConditions",
					inActiveConditions);
			// loadNetworkZoneMap();
		} else if (reloadRules) {
			statelessKieSession = getNewKieSession(statelessKieSession);
			RuleCustomCondition ruleCustomCondition = new RuleCustomCondition(
					clientProp);
			InActiveConditions inActiveConditions = new InActiveConditions(
					clientProp, ipTiFeedMap, hostTiFeedMap, ipTiFeedgte70Map,
					hostTiFeedgte70Map, ipTiFeedlt70Map, hostTiFeedlt70Map);
			statelessKieSession.setGlobal("RuleCustomCondition",
					ruleCustomCondition);
			statelessKieSession.setGlobal("InActiveConditions",
					inActiveConditions);
			// loadNetworkZoneMap();
		}

		return statelessKieSession;
	}

	/**
   * 
   */
	private static void loadNetworkZoneMap() {
		Connection connection = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			connection = DriverManager.getConnection(clientProp
					.getProperty("phoenix.database.url"));

			stmt = connection.createStatement();
			rs = stmt.executeQuery("select ipn, cu, zo from "
					+ clientProp.getProperty("phoenix.schemaname")
					+ ".network_zone_master");

			if (rs != null) {
				networkZoneMap = new HashMap<String, String>();
				while (rs.next()) {
					networkZoneMap.put(
							rs.getString("ipn") + "_" + rs.getString("cu"),
							rs.getString("zo"));
				}
			}
		} catch (SQLException | ClassNotFoundException e) {
			log.error("Error in loadNetworkZoneMap: " + e.getMessage());
		} catch (Exception e) {
			log.error("Exception in loadNetworkZoneMap: " + e.getMessage());
		} finally {
			if (rs != null) {
				try {
					rs.close();
					rs = null;
				} catch (Exception e) {
					log.error(e.getMessage());
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
					stmt = null;
				} catch (Exception e) {
					log.error(e.getMessage());
				}
			}
			if (connection != null) {
				try {
					connection.close();
					connection = null;
				} catch (Exception e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	/**
   * 
   */
	private static void getTiFeedMaps() {
		Connection connection = null;
		Statement stmt = null;
		ResultSet rsIp = null;
		ResultSet rsHost = null;

		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			connection = DriverManager.getConnection(clientProp
					.getProperty("phoenix.database.url"));

			stmt = connection.createStatement();
			rsIp = stmt.executeQuery("select ds,rs from "
					+ clientProp.getProperty("phoenix.schemaname")
					+ ".tifeed_repo where ln='IpList'");

			if (rsIp != null) {
				ipTiFeedMap = new HashMap<String, Integer>();
				while (rsIp.next()) {
					Integer rs = rsIp.getInt("rs");
					ipTiFeedMap.put(rsIp.getString("ds"), rs);
					if (rs >= 70) {
						ipTiFeedgte70Map.put(rsIp.getString("ds"), rs);
					} else {
						ipTiFeedlt70Map.put(rsIp.getString("ds"), rs);
					}
					rs = 0;
				}
			}

			rsHost = stmt.executeQuery("select ds,rs from "
					+ clientProp.getProperty("phoenix.schemaname")
					+ ".tifeed_repo where ln='HostList'");

			if (rsHost != null) {
				hostTiFeedMap = new HashMap<String, Integer>();
				while (rsHost.next()) {
					int rs = rsHost.getInt("rs");
					hostTiFeedMap.put(rsHost.getString("ds"), rs);
					if (rs >= 70) {
						hostTiFeedgte70Map.put(rsHost.getString("ds"), rs);
					} else {
						hostTiFeedlt70Map.put(rsHost.getString("ds"), rs);
					}
					rs = 0;
				}
			}
		} catch (SQLException | ClassNotFoundException e) {
			log.error("Error in getTiFeedMaps: " + e.getMessage());
		} catch (Exception e) {
			log.error("Exception in getTiFeedMaps: " + e.getMessage());
		} finally {
			if (rsIp != null) {
				try {
					rsIp.close();
					rsIp = null;
				} catch (Exception e) {
					log.error(e.getMessage());
				}
			}
			if (rsHost != null) {
				try {
					rsHost.close();
					rsHost = null;
				} catch (Exception e) {
					log.error(e.getMessage());
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
					stmt = null;
				} catch (Exception e) {
					log.error(e.getMessage());
				}
			}
			if (connection != null) {
				try {
					connection.close();
					connection = null;
				} catch (Exception e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	public static StatelessKieSession getNewKieSession(
			StatelessKieSession oldSession) {
		KieContainer kContainer = null;
		Connection conn = null;

		boolean csvMoveSuccessCheck = moveFilesToDeployedRule(clientProp);
		SiemRuleDaoImpl siemRuleDaoImpl = new SiemRuleDaoImpl(clientProp);
		if (csvMoveSuccessCheck || oldSession == null) {
			try {
				Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
				conn = DriverManager.getConnection(clientProp
						.getProperty("phoenix.database.url"));
				AddRule.newAddRule(clientProp);
				// AddRule.addRule(clientProp);
				staticDrlFileStr = readFile(clientProp
						.getProperty("drlFileTemplatePath"));
				String activePath = clientProp
						.getProperty("activeRulesCsvPath");

				List<RuleMatchDTO> ruleMatchDTOList = getRulesList(activePath,
						siemRuleDaoImpl, conn);
				String drlStr = AddRule.getRulesDRLString(clientProp,
						ruleMatchDTOList);

				drlStr = staticDrlFileStr + drlStr;

				log.error("==========drl string before compilation========"
						+ drlStr);

				KieServices kieServices = KieServices.Factory.get();

				KieFileSystem kieFileSystem = kieServices.newKieFileSystem();
				KieRepository kieRepository = kieServices.getRepository();

				kieFileSystem.write("src/main/resources/rulematch.drl", drlStr);

				KieBuilder kb = kieServices.newKieBuilder(kieFileSystem);

				kb.buildAll();

				if (kb.getResults().hasMessages(Level.ERROR)) {
					siemRuleDaoImpl.sendToRCEDiNotification(conn,
							clientProp.getProperty("phoenix.schemaname"),
							"Exception occured while compiling the rules exception is: "
									+ kb.getResults().toString(),
							clientProp.getProperty("exceptionEmailIds"));
					throw new RuntimeException("Drools Build Errors:\n"
							+ kb.getResults().toString());
				}

				log.error("==========loaded drl string========" + drlStr);

				kContainer = kieServices.newKieContainer(kieRepository
						.getDefaultReleaseId());
				writeDRLText(drlStr);
				ruleMatchDTOList = null;
				drlStr = null;
			} catch (Exception e) {
				siemRuleDaoImpl.sendToRCEDiNotification(conn,
						clientProp.getProperty("phoenix.schemaname"),
						"Exception occured while loading the rules exception is: "
								+ e.getMessage(),
						clientProp.getProperty("exceptionEmailIds"));
				log.error("Exception in getNewKieSession method: " + e);
			} finally {
				try {
					if (conn != null && !(conn.isClosed())) {
						conn.close();
						conn = null;
					}
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
			}
			return kContainer.newStatelessKieSession();
		} else {
			return oldSession;
		}
	}

	private static boolean validateRulesFromRulesToDeployFolder(
			Properties clientProp2, String path, Connection conn,
			SiemRuleDaoImpl siemRuleDaoImpl) {
		boolean checkCsv = false;
		String drlStr = "";
		try {
			staticDrlFileStr = readFile(clientProp2
					.getProperty("drlFileTemplatePath"));

			List<RuleMatchDTO> ruleMatchDTOList = getRulesList(path,
					siemRuleDaoImpl, conn);

			if (ruleMatchDTOList != null) {
				drlStr = AddRule.getRulesDRLString(clientProp2,
						ruleMatchDTOList);
				drlStr = staticDrlFileStr + drlStr;

				log.error("==========validateRulesFromRulesToDeployFolder : drl string before compilation========"
						+ drlStr + " from path: - " + path);

				KieServices kieServices = KieServices.Factory.get();

				KieFileSystem kieFileSystem = kieServices.newKieFileSystem();

				kieFileSystem.write("src/main/resources/rulematch.drl", drlStr);

				KieBuilder kb = kieServices.newKieBuilder(kieFileSystem);

				kb.buildAll();

				if (kb.getResults().hasMessages(Level.ERROR)) {
					throw new RuntimeException("Drools Build Errors:\n"
							+ kb.getResults().toString() + " from path: - "
							+ path);
				} else {
					checkCsv = true;
				}
				log.error("==========validateRulesFromRulesToDeployFolder : loaded drl string========"
						+ drlStr + " from path: - " + path);
			} else {
				checkCsv = false;
			}

			ruleMatchDTOList = null;
			drlStr = null;
		} catch (Exception e) {
			siemRuleDaoImpl.sendToRCEDiNotification(conn,
					clientProp.getProperty("phoenix.schemaname"),
					"Exception occured while compiling the manual rules exception is: "
							+ e.getMessage(),
					clientProp.getProperty("exceptionEmailIds"));
			log.error("Exception in validateRulesFromRulesToDeployFolder method: "
					+ e + " from path: - " + path);
		}
		return checkCsv;
	}

	/**
	 * This method is used to read the text file from given path
	 * 
	 * @param path
	 * @return
	 */
	public static String readFile(String path) {
		String drl = "";
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(path);
			drl = IOUtils.toString(inputStream, "UTF-8");
		} catch (IOException e) {
			log.error(e.getMessage());
		} catch (Exception e) {
			log.error(e.getMessage());
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				log.error(e.getMessage());
			}
		}
		return drl;
	}

	/**
	 * 
	 * @param conn
	 * @param siemRuleDaoImpl
	 * @return
	 */
	public static List<RuleMatchDTO> getRulesList(String filePath,
			SiemRuleDaoImpl siemRuleDaoImpl, Connection conn) {
		List<RuleMatchDTO> rulesList = new ArrayList<RuleMatchDTO>();
		try {
			FileInputStream rulesStream = new FileInputStream(
					new File(filePath));

			String rulesStr = IOUtils.toString(rulesStream, "UTF-8");

			if (!StringUtils.isEmpty(rulesStr)) {
				String[] rulesStrArr = rulesStr.split("\n");

				for (String rule : rulesStrArr) {
					if (!StringUtils.isEmpty(rule)) {
						Integer ruleId = 0;
						RuleMatchDTO ruleMatchDTO = new RuleMatchDTO();

						String[] ruleArr = rule.split("~#~");
						try {
							Date expiryDate = null;
							if (ruleArr.length > 11
									&& !StringUtils.isEmpty(ruleArr[11])) {
								try {
									expiryDate = formatter.parse(ruleArr[11]
											.trim());
								} catch (ParseException e) {
									log.error("Rule expiry date is not in currect format: "
											+ ruleArr[11]
											+ " from path : -"
											+ filePath
											+ " RuleName - "
											+ ruleArr[3]);
									return null;
								}
							}

							if (expiryDate == null
									|| expiryDate.after(new Date())) {
								ruleId = Integer.parseInt(ruleArr[0].trim());
								ruleMatchDTO.setRuleId(ruleId);
								ruleMatchDTO.setRuleType(ruleArr[1].trim());
								ruleMatchDTO.setRuleSubType(ruleArr[2].trim());
								ruleMatchDTO.setRuleName(ruleArr[3].trim());

								if (ruleArr[2]
										.trim()
										.equalsIgnoreCase(
												clientProp
														.getProperty("followedByRuleName"))) {
									ruleNameIdMap.put(ruleArr[3].trim()
											.toLowerCase(), ruleId);
								}

								String ruleCondition = "";

								if (!StringUtils.isEmpty(ruleArr[4])) {
									ruleCondition = ruleArr[4]
											.replaceAll(" InCSVActiveList",
													" RuleCustomCondition.InCSVActiveList")
											.replaceAll(" NotInCSVActiveList",
													" RuleCustomCondition.NotInCSVActiveList")
											.replaceAll(" InActiveList\\(",
													" InActiveConditions.InActiveList\\(\\$m,Connection,")
											.replaceAll(" NotInActiveList\\(",
													" InActiveConditions.NotInActiveList\\(\\$m,Connection,");
								}

								ruleMatchDTO.setRuleCondition(ruleCondition
										.trim());
								ruleCondition = null;

								if (ruleArr.length > 5
										&& !StringUtils.isEmpty(ruleArr[5])) {
									ruleMatchDTO
											.setThresholdApplicable(ruleArr[5]
													.trim());
								} else {
									ruleMatchDTO.setThresholdApplicable("No");
								}

								if (ruleArr.length > 6
										&& !StringUtils.isEmpty(ruleArr[6])) {
									ruleMatchDTO
											.setEventAThresholdValue(Integer
													.parseInt(ruleArr[6].trim()));
								} else {
									ruleMatchDTO.setEventAThresholdValue(0);
								}

								if (ruleArr.length > 7
										&& !StringUtils.isEmpty(ruleArr[7])) {
									ruleMatchDTO
											.setEventBThresholdValue(Integer
													.parseInt(ruleArr[7].trim()));
								} else {
									ruleMatchDTO.setEventBThresholdValue(1);
								}

								if (ruleArr.length > 8
										&& !StringUtils.isEmpty(ruleArr[8])) {
									String[] timeWindowApplicableArr = ruleArr[8]
											.trim().split("(?<=\\d)(?=\\D)");

									if (timeWindowApplicableArr != null
											&& timeWindowApplicableArr.length >= 2) {
										if (!StringUtils
												.isEmpty(timeWindowApplicableArr[0])) {
											ruleMatchDTO
													.setTimeWindowValue(Integer
															.parseInt(timeWindowApplicableArr[0]
																	.trim()));
										}
										ruleMatchDTO
												.setTimeWindowUnit(timeWindowApplicableArr[1]);
									} else {
										ruleMatchDTO.setTimeWindowUnit("NA");
										ruleMatchDTO.setTimeWindowValue(0);
									}
									timeWindowApplicableArr = null;
								} else {
									ruleMatchDTO.setTimeWindowUnit("NA");
									ruleMatchDTO.setTimeWindowValue(0);
								}

								if (ruleArr.length > 9
										&& !StringUtils.isEmpty(ruleArr[9])) {
									ruleMatchDTO.setRulePriority(Integer
											.parseInt(ruleArr[9].trim()));
								} else {
									ruleMatchDTO.setRulePriority(0);
								}

								if (ruleArr.length > 10
										&& !StringUtils.isEmpty(ruleArr[10])) {
									ruleMatchDTO
											.setThresholdCondition(ruleArr[10]
													.trim());
								} else {
									ruleMatchDTO.setThresholdCondition("NA");
								}

								if (ruleArr.length > 12
										&& !StringUtils.isEmpty(ruleArr[12])) {
									ruleMatchDTO.setIdenticalColStr(ruleArr[12]
											.trim());
								} else {
									ruleMatchDTO.setIdenticalColStr("NA");
								}

								if (ruleArr.length > 13
										&& !StringUtils.isEmpty(ruleArr[13])) {
									ruleMatchDTO.setDistinctColStr(ruleArr[13]
											.trim());
								} else {
									ruleMatchDTO.setDistinctColStr("NA");
								}

								if (ruleArr.length > 14
										&& !StringUtils.isEmpty(ruleArr[14])) {
									ruleMatchDTO.setLightweightStr(ruleArr[14]
											.trim());
									String[] lwStr = ruleArr[14].trim().split(
											"~####~");

									if (lwStr.length > 1) {
										Map<String, String> map = InActiveConditions
												.getInActiveListMap();
										if (map == null) {
											map = new HashMap<String, String>();
										}
										String[] fieldArr = lwStr[1]
												.split("~###~");

										StringBuilder sbr = new StringBuilder();
										if (fieldArr != null
												&& fieldArr.length > 0) {
											boolean first = true;
											for (String field : fieldArr) {
												if (!first) {
													sbr.append(",");
												}

												String fld = null;
												String[] arr = field
														.split("~##~");
												if (arr != null
														&& arr.length > 1) {
													fld = arr[2];
												} else {
													fld = field.trim();
												}
												sbr.append(fld.trim() + "."
														+ fld.trim());
												first = false;
											}
										}
										map.put(lwStr[0].trim(), sbr.toString());
										sbr.delete(0, sbr.length());
										sbr = null;

									}
									lwStr = null;
								} else {
									ruleMatchDTO.setLightweightStr("NA");
								}

								if (ruleArr.length > 15
										&& !StringUtils.isEmpty(ruleArr[15])) {
									Integer rId = ruleNameIdMap.get(ruleArr[15]
											.trim().toLowerCase());
									if (rId != null) {
										ruleMatchDTO
												.setFollwedByRuleName(String
														.valueOf(rId));
									} else {
										ruleMatchDTO.setFollwedByRuleName("NA");
									}
									rId = null;
								} else {
									ruleMatchDTO.setFollwedByRuleName("NA");
								}

								if (ruleArr.length > 16
										&& !StringUtils.isEmpty(ruleArr[16])) {
									ruleMatchDTO
											.setSeverity(ruleArr[16].trim());
								} else {
									ruleMatchDTO.setSeverity("NA");
								}

								if (ruleArr.length > 17
										&& !StringUtils.isEmpty(ruleArr[17])) {
									ruleMatchDTO.setRecommendations(ruleArr[17]
											.trim());
								} else {
									ruleMatchDTO.setRecommendations("NA");
								}

								if (ruleArr.length > 18
										&& !StringUtils.isEmpty(ruleArr[18])) {
									ruleMatchDTO.setActions(ruleArr[18].trim());
								} else {
									ruleMatchDTO.setActions("NA");
								}

								ruleArr = null;
								rulesList.add(ruleMatchDTO);
							}

						} catch (Exception e) {
							siemRuleDaoImpl.sendToRCEDiNotification(
									conn,
									clientProp
											.getProperty("phoenix.schemaname"),
									"Exception occured while loading the manual rule, rule is: "
											+ rule + " exception is: "
											+ e.getMessage(), clientProp
											.getProperty("exceptionEmailIds"));
							log.error("Exception in rule String: " + rule
									+ " Error is:" + e.getMessage()
									+ " from Path: - " + filePath);
							return null;
						}
					}
					rule = null;
				}
				rulesStrArr = null;
			}
			rulesStr = null;
			rulesStream.close();

		} catch (IOException e) {
			siemRuleDaoImpl.sendToRCEDiNotification(conn,
					clientProp.getProperty("phoenix.schemaname"),
					"Exception occured while loading the manual rules exception is: "
							+ e.getMessage(),
					clientProp.getProperty("exceptionEmailIds"));
			log.error("Exception in getRulesList method: " + e
					+ " from path : -" + filePath);
			return null;
		} catch (Exception e) {
			siemRuleDaoImpl.sendToRCEDiNotification(conn,
					clientProp.getProperty("phoenix.schemaname"),
					"Exception occured while loading the manual rules exception is: "
							+ e.getMessage(),
					clientProp.getProperty("exceptionEmailIds"));
			log.error("Exception in getRulesList method: " + e
					+ " from path : -" + filePath);
			return null;
		}
		return rulesList;
	}

	/**
	 * This method is used to create the backup file of running drls
	 * 
	 * @param str
	 */
	public static void writeDRLText(String str) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(
							clientProp.getProperty("drlBackUpPath"), false),
					StandardCharsets.UTF_8));

			bw.write(str);
		} catch (IOException e) {
			log.error("Error in writeDRLText method: " + e);
		} catch (Exception e) {
			log.error("Error in writeDRLText method: " + e);
		} finally {
			if (bw != null) {
				try {
					bw.flush();
					bw.close();
				} catch (IOException e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	/**
	 * 
	 * @param clntProp
	 * @return
	 */
	public static boolean moveFilesToDeployedRule(Properties clntProp) {
		Connection conn = null;
		SiemRuleDaoImpl siemRuleDaoImpl = new SiemRuleDaoImpl(clientProp);

		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			conn = DriverManager.getConnection(clientProp
					.getProperty("phoenix.database.url"));
		} catch (Exception e) {
			log.error("Error in moveFilesToDeployedRule method for connection object: "
					+ e);
		}

		boolean correctCSV = false;
		boolean needToMoveFile = false;
		String destPath = clntProp.getProperty("deployedRules");
		String srcPath = clntProp.getProperty("rulesToDeploy");

		String automaticCsvPath = clntProp.getProperty("securityRuleCSVPath");
		String manualCSVPath = clntProp.getProperty("manualRuleCSVPath");

		File automaticCsv = new File(automaticCsvPath);
		File manualCSV = new File(manualCSVPath);
		List<File> files = new ArrayList<File>();

		if (automaticCsv.exists()) {
			files.add(automaticCsv);
		}

		if (manualCSV.exists()) {
			files.add(manualCSV);
		}

		String logHistoryPath = clntProp.getProperty("rulesLogHistory");
		File sourceFolder = new File(srcPath);
		File destinationFolder = new File(destPath);

		if (!destinationFolder.exists()) {
			destinationFolder.mkdirs();
		}

		// Check weather source exists and it is folder.
		if (sourceFolder.exists() && sourceFolder.isDirectory()) {
			// Get list of the files and iterate over them
			if (files != null) {
				for (File fl : files) {
					try {
						correctCSV = validateRulesFromRulesToDeployFolder(
								clntProp, srcPath + fl.getName(), conn,
								siemRuleDaoImpl);
						if (correctCSV) {
							Files.copy(fl.toPath(),
									(new File(destPath + fl.getName()))
											.toPath(),
									StandardCopyOption.REPLACE_EXISTING);

							Date now = new Date();
							SimpleDateFormat dateFormat = new SimpleDateFormat(
									"yyyy-MM-ddhh:mm:ss");
							String time = dateFormat.format(now);
							File dir = new File(logHistoryPath + time);
							dir.mkdir();

							FileUtils.copyFileToDirectory(fl, dir);

							boolean del = fl.delete();

							if (del) {
								log.info("File " + fl.getName() + " deleted!");
							} else
								log.info("File " + fl.getName()
										+ "not deleted!");

							needToMoveFile = true;
							correctCSV = true;

						} else {
							correctCSV = false;
						}
					} catch (IOException e) {
						log.error("Error in moveFilesToDeployedRule method: "
								+ e);
					} catch (Exception e) {
						log.error("Error in moveFilesToDeployedRule method: "
								+ e);
					} finally {
						try {
							if (conn != null) {
								conn.close();
								conn = null;
							}
						} catch (SQLException e) {
							log.error(e.getMessage());
						}
					}
				}
			}
		} else {
			log.error(sourceFolder + "  Folder does not exists");
			correctCSV = false;
		}

		if (needToMoveFile) {
			return needToMoveFile;
		} else if (correctCSV) {
			return correctCSV;
		}
		return false;
	}
}
