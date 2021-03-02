/**
 * 
 */
package net.paladion.rule;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.SiemRuleDaoImpl;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.drools.core.util.StringUtils;

/**
 * @author ankush
 *
 */
@Slf4j
public class RuleCustomCondition implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Map<String, List<String>> csvActiveListMap = new HashMap<String, List<String>>();
	private static Properties clientProp;

	public RuleCustomCondition(Properties clientProp1) {
		super();
		clientProp = clientProp1;
		init();
	}

	/**
	 * @return
	 * 
	 */
	public void init() {
		File myDirectory = new File(clientProp.getProperty("activeListPath"));
		File[] containingFileNames = myDirectory.listFiles();
		Connection conn = null;
		SiemRuleDaoImpl siemRuleDaoImpl = new SiemRuleDaoImpl(clientProp);

		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			conn = DriverManager.getConnection(clientProp
					.getProperty("phoenix.database.url"));

			if (containingFileNames != null) {
				for (File file : containingFileNames) {
					String fileNameWithExt = file.getName();
					String fileName = FilenameUtils
							.removeExtension(fileNameWithExt);

					fileNameWithExt = null;
					FileInputStream fIStream = null;
					try {
						fIStream = new FileInputStream(file.getPath());

						String valStr = IOUtils.toString(fIStream);
						List<String> activeList = new ArrayList<String>();

						if (!StringUtils.isEmpty(valStr)) {
							String[] valStrArr = valStr.split("\n");
							for (String val : valStrArr) {
								if (!StringUtils.isEmpty(val)) {
									activeList.add(val.toLowerCase().trim());
								}
							}
							valStrArr = null;
						}
						valStr = null;
						csvActiveListMap
								.put(fileName.toLowerCase(), activeList);
					} catch (IOException e) {
						siemRuleDaoImpl
								.sendToRCEDiNotification(
										conn,
										clientProp
												.getProperty("phoenix.schemaname"),
										"Exception occured in ruleCustomCondition while loading csvActiveList in init() method exception is: "
												+ e.getMessage(),
										clientProp
												.getProperty("exceptionEmailIds"));
						log.error("Error while reading the Active List: "
								+ fileName + " Error is: " + e.getMessage());
					} finally {
						try {
							fIStream.close();
						} catch (IOException e) {
							log.error(e.getMessage());
						}
					}

					fileName = null;
				}
			}

			if (myDirectory != null) {
				myDirectory.delete();
			}
			containingFileNames = null;
		} catch (Exception e) {
			siemRuleDaoImpl
					.sendToRCEDiNotification(
							conn,
							clientProp.getProperty("phoenix.schemaname"),
							"Exception occured in ruleCustomCondition while loading init() method exception is: "
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
				if (conn != null) {
					conn.close();
					conn = null;
				}
			} catch (SQLException e) {
				log.error(e.getMessage());
			}
		}
	}

	/**
	 * 
	 * @param fileName
	 * @param content1
	 * @return
	 */
	public boolean InCSVActiveList(String fileName, Integer content1) {
		boolean found = false;
		String content = "";
		if (content1 != null) {
			content = String.valueOf(content1);
		}

		if (fileName != null && !StringUtils.isEmpty(content)) {
			found = InCSVActiveList(fileName, content);
		}

		return found;
	}

	/**
	 * 
	 * @param fileName
	 * @param content1
	 * @return
	 */
	public boolean InCSVActiveList(String fileName, Long content1) {
		boolean found = false;
		String content = "";
		if (content1 != null) {
			content = String.valueOf(content1);
		}

		if (fileName != null && !StringUtils.isEmpty(content)) {
			found = InCSVActiveList(fileName, content);
		}

		return found;
	}

	/**
	 * 
	 * @param fileName
	 * @param content
	 * @return
	 */
	public boolean InCSVActiveList(String fileName, String content) {
		boolean found = false;
		if (fileName != null && !StringUtils.isEmpty(content)) {
			List<String> list = csvActiveListMap.get(fileName.toLowerCase());

			if (list == null) {
				putNewList(fileName);
				list = csvActiveListMap.get(fileName.toLowerCase());
			}

			if (list != null && !list.isEmpty()) {
				if (list.contains(content.trim().toLowerCase())) {
					found = true;
				}
			}
		}

		return found;
	}

	/**
	 * 
	 * @param fileName
	 * @param content1
	 * @return
	 */
	public boolean NotInCSVActiveList(String fileName, Integer content1) {
		boolean found = false;
		String content = "";
		if (content1 != null) {
			content = String.valueOf(content1);
		}

		if (fileName != null && !StringUtils.isEmpty(content)) {
			found = NotInCSVActiveList(fileName, content);
		}

		return found;
	}

	/**
	 * 
	 * @param fileName
	 * @param content1
	 * @return
	 */
	public boolean NotInCSVActiveList(String fileName, Long content1) {
		boolean found = false;
		String content = "";
		if (content1 != null) {
			content = String.valueOf(content1);
		}

		if (fileName != null && !StringUtils.isEmpty(content)) {
			found = NotInCSVActiveList(fileName, content);
		}

		return found;
	}

	/**
	 * 
	 * @param fileName
	 * @param content
	 * @return
	 */
	public boolean NotInCSVActiveList(String fileName, String content) {
		boolean found = false;

		if (fileName != null && !StringUtils.isEmpty(content)) {
			List<String> list = csvActiveListMap.get(fileName.toLowerCase());

			if (list == null) {
				putNewList(fileName);
				list = csvActiveListMap.get(fileName.toLowerCase());
			}

			if (list != null && !list.isEmpty()
					&& !list.contains(content.trim().toLowerCase())) {
				found = true;
			}
		}

		return found;
	}

	/**
	 * 
	 * @param newFileName
	 */
	private void putNewList(String newFileName) {
		File myDirectory = new File(clientProp.getProperty("activeListPath"));
		File[] containingFileNames = myDirectory.listFiles();
		Connection conn = null;
		SiemRuleDaoImpl siemRuleDaoImpl = new SiemRuleDaoImpl(clientProp);

		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			conn = DriverManager.getConnection(clientProp
					.getProperty("phoenix.database.url"));
			if (containingFileNames != null) {
				for (File file : containingFileNames) {
					String fileNameWithExt = file.getName();
					String fileName = FilenameUtils
							.removeExtension(fileNameWithExt);

					if (fileName.equalsIgnoreCase(newFileName)) {
						FileInputStream fIStream = null;
						try {
							fIStream = new FileInputStream(file.getPath());

							String valStr = IOUtils.toString(fIStream);
							List<String> activeList = new ArrayList<String>();

							if (!StringUtils.isEmpty(valStr)) {
								String[] valStrArr = valStr.split("\n");
								for (String val : valStrArr) {
									if (!StringUtils.isEmpty(val)) {
										activeList
												.add(val.toLowerCase().trim());
									}
								}
								valStrArr = null;
							}
							valStr = null;
							csvActiveListMap.put(fileName.toLowerCase(),
									activeList);
						} catch (IOException e) {
							siemRuleDaoImpl
									.sendToRCEDiNotification(
											conn,
											clientProp
													.getProperty("phoenix.schemaname"),
											"Exception occured in ruleCustomCondition while loading csvActiveList in putNewList() method exception is: "
													+ e.getMessage(),
											clientProp
													.getProperty("exceptionEmailIds"));
							log.error("Error while reading the Active List: "
									+ fileName + " Error is: " + e.getMessage());
						} finally {
							try {
								fIStream.close();
							} catch (IOException e) {
								log.error(e.getMessage());
							}
						}

						fileName = null;
					}
				}
			}

			if (myDirectory != null) {
				myDirectory.delete();
			}
			containingFileNames = null;
		} catch (Exception e) {
			siemRuleDaoImpl
					.sendToRCEDiNotification(
							conn,
							clientProp.getProperty("phoenix.schemaname"),
							"Exception occured in ruleCustomCondition while loading putNewList() method exception is: "
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
