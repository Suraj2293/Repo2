package net.paladion.util;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.ThreatDroolsRuleMatch;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DTOConverter implements Serializable {
	private static final long serialVersionUID = 1L;
	private static Map<String, Map<String, String>> dataMap;
	private static Map<String, MultiMap> processedDataMap = new HashMap<String, MultiMap>();

	public static Map<String, Map<String, String>> getDataMap() {
		return dataMap;
	}

	public static void setDataMap(Map<String, Map<String, String>> dataMap) {
		DTOConverter.dataMap = dataMap;
	}

	public static Map<String, MultiMap> getProcessedDataMap() {
		return processedDataMap;
	}

	public static void setProcessedDataMap(
			Map<String, MultiMap> processedDataMap) {
		DTOConverter.processedDataMap = processedDataMap;
	}

	@SuppressWarnings({ "rawtypes" })
	public static MultiEventCoreDTO cefConverter(String dataStr,
			Properties clientProp, Map<String, String> fieldsMap) {
		String[] keyValuePairs = dataStr.split(clientProp.getProperty("dataSplitter"));
		boolean isLoggerData = false;
		if (!StringUtils.isEmpty(dataStr)
				&& dataStr.toLowerCase().replaceAll("\\s+", "")
						.contains("loggertype=no")) {
			isLoggerData = true;
		}

		Map<String, String> dataMap = new HashMap<String, String>();

		StringBuilder othersSbr = new StringBuilder();
		for (String pair : keyValuePairs) {
			try {
				String[] entry = pair.split("=", 2);
				String val = "";
				if (entry.length > 1) {
					val = entry[1];
				}

				if (val != null) {
					val = val.trim();
				}

				String key = null;
				if (entry.length > 0) {
					key = entry[0];
				}

				if (key != null) {
					key = key.trim();
				}

				String fMKey = null;

				if (isLoggerData) {
					fMKey = key.trim();
				} else {
					fMKey = fieldsMap.get(key.trim());
				}

				if (!fieldsMap.containsValue(fMKey)) {
					othersSbr.append(key + "=" + val + ",");
				} else {
					dataMap.put(fMKey, val);
				}

				entry = null;
				fMKey = null;
				val = null;
				key = null;
			} catch (Exception e) {
				log.error("Error while fetching the key values data: "
						+ e.getMessage());
			}
		}

		dataStr = null;
		Class cls = MultiEventCoreDTO.class;

		MultiEventCoreDTO dto = null;
		try {
			dto = (MultiEventCoreDTO) cls.newInstance();
		} catch (InstantiationException | IllegalAccessException e1) {
			log.error(e1.getMessage());
		}

		String[] tempDataArray = new String[fieldsMap.size() + 2];

		int i = 0;
		for (String fieldStr : fieldsMap.values()) {
			Field field = null;
			try {
				field = cls.getDeclaredField((String) fieldStr);
				field.setAccessible(true);
				if (field.getType().getName().equalsIgnoreCase("int")) {
					if (dataMap.get(fieldStr) != null) {
						if (fieldStr.equalsIgnoreCase("baseEventCount")
								&& (StringUtils.isEmpty(dataMap.get(fieldStr)) || dataMap
										.get(fieldStr).equals("0"))) {
							field.set(dto, 1);
							tempDataArray[i] = (fieldStr + "==" + 1);
						} else {
							if (!StringUtils.isEmpty(dataMap.get(fieldStr))) {
								field.set(dto,
										Integer.valueOf(dataMap.get(fieldStr)));
							}
							tempDataArray[i] = (fieldStr + "==" + dataMap
									.get(fieldStr));
						}
					} else {
						if (fieldStr.equalsIgnoreCase("baseEventCount")) {
							field.set(dto, 1);
							tempDataArray[i] = (fieldStr + "==" + 1);
						} else {
							tempDataArray[i] = (fieldStr + "==" + dataMap
									.get(fieldStr));
						}
					}
				} else if (field.getType().getName().equalsIgnoreCase("long")) {
					if (dataMap.get(fieldStr) != null
							&& !StringUtils.isEmpty(dataMap.get(fieldStr))) {
						field.set(dto, Long.valueOf(dataMap.get(fieldStr)));
					}
					tempDataArray[i] = (fieldStr + "==" + dataMap.get(fieldStr));
				} else if (field.getType().getName()
						.equalsIgnoreCase("java.lang.String")) {
					field.set(dto, dataMap.get(fieldStr));
					tempDataArray[i] = (fieldStr + "==" + dataMap.get(fieldStr));
				} else if (field.getType().getName()
						.equalsIgnoreCase("java.util.Date")) {
					if (dataMap.get(fieldStr) != null
							&& !StringUtils.isEmpty(dataMap.get(fieldStr))) {
						try {
							Date dt = new Date(Long.valueOf(dataMap
									.get(fieldStr)));
							field.set(dto, dt);
						} catch (Exception e) {
							log.error("Exception while parsing " + fieldStr
									+ " value is: " + dataMap.get(fieldStr));
						}
					}
					tempDataArray[i] = (fieldStr + "==" + dataMap.get(fieldStr));
				} else if (field.getType().getName()
						.equalsIgnoreCase("java.sql.Timestamp")) {
					if (dataMap.get(fieldStr) != null
							&& !StringUtils.isEmpty(dataMap.get(fieldStr))) {
						try {
							Timestamp dt = new Timestamp(Long.valueOf(dataMap
									.get(fieldStr)));
							field.set(dto, dt);
						} catch (Exception e) {
							log.error("Exception while parsing " + fieldStr
									+ " value is: " + dataMap.get(fieldStr));
						}
					}
					tempDataArray[i] = (fieldStr + "==" + dataMap.get(fieldStr));
				}
				fieldStr = null;
			} catch (Exception e) {
				// log.error("Error while parsing field: " + field.getName() +
				// "field value:"
				// + dataMap.get(fieldStr) + " error: " + e.getMessage());
			}

			i++;
			field = null;
		}

		keyValuePairs = null;
		fieldsMap = null;
		dataMap = null;

		if (othersSbr != null) {
			tempDataArray[i] = ("others==" + othersSbr.toString().replaceAll(
					",$", ""));

			dto.setOthers(othersSbr.toString().replaceAll(",$", ""));
			othersSbr.delete(0, othersSbr.length());
			othersSbr = null;
		}

		log.debug("tempDataArray  -->" + tempDataArray);

		dto.setDataArray(tempDataArray);

		tempDataArray = null;
		return dto;

	}

	public static MultiMap getMappingDetails(int mapId) {
		MultiMap multiMap = null;
		try {
			if (!getProcessedDataMap().isEmpty()) {
				multiMap = getProcessedDataMap().get(mapId + "~default");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return multiMap;
	}

	public static boolean contains(String[] arr, String item) {
		return Arrays.stream(arr).anyMatch(item::equals);
	}

	public static void convertToThreatDroolsRuleMatch(Properties clientProp,
			List<String> list, ThreatDroolsRuleMatch dto) {
		@SuppressWarnings("rawtypes")
		Class cls = ThreatDroolsRuleMatch.class;

		for (String data : list) {
			if (data != null) {
				String[] dataArr = data.split("==");

				Field field = null;
				String fName = dataArr[0];
				String dVal = null;
				if (fName != null) {
					fName = fName.trim();
				}
				if (dataArr.length > 1) {
					dVal = dataArr[1];
				}

				try {
					if (!StringUtils.isEmpty(fName)
							&& !StringUtils.isEmpty(dVal)) {
						field = cls.getDeclaredField(fName);
						field.setAccessible(true);
						if (dVal != null) {
							dVal = dVal.trim();
						}

						// log.debug(field + " " + dVal);

						if (field.getType().getName().equalsIgnoreCase("int")) {
							if (dVal != null) {
								field.set(dto, Integer.valueOf(dVal));
							}
						} else if (field.getType().getName()
								.equalsIgnoreCase("long")) {
							if (dVal != null) {
								field.set(dto, Long.valueOf(dVal));
							}
						} else if (field.getType().getName()
								.equalsIgnoreCase("java.lang.String")) {
							field.set(dto, dVal);
						} else if (field.getType().getName()
								.equalsIgnoreCase("java.util.Date")) {
							if (dVal != null && !StringUtils.isEmpty(dVal)) {
								try {
									Date dt = new Date(Long.valueOf(dVal));
									field.set(dto, dt);
								} catch (Exception e) {
									log.error("Exception while parsing "
											+ fName + " value is: " + dVal);
								}
							}
						} else if (field.getType().getName()
								.equalsIgnoreCase("java.sql.Timestamp")) {
							if (dVal != null && !StringUtils.isEmpty(dVal)) {
								try {
									Timestamp dt = new Timestamp(
											Long.valueOf(dVal));
									field.set(dto, dt);
								} catch (Exception e) {
									log.error("Exception while parsing "
											+ fName + " value is: " + dVal);
								}
							}
						}
					}
					dVal = null;
					field = null;
					fName = null;
				} catch (Exception e) {
					// log.error("Error while parsing field: " + field.getName()
					// + " data is: " + data
					// + " error: " + e.getMessage());
				}

				dataArr = null;
				data = null;
			}
		}
	}
}
