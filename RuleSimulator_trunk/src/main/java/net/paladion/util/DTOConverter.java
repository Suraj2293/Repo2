package net.paladion.util;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;

import org.apache.commons.lang3.StringUtils;

@SuppressWarnings("serial")
@Slf4j
public class DTOConverter implements Serializable {

	/**
	 * 
	 * @param clientProp
	 * @param dataStr
	 * @param fieldsMap
	 * @return dto
	 */
	public static MultiEventCoreDTO convertToMultiCore(Properties clientProp,
			String dataStr, Map<String, String> fieldsMap) {

		String data = dataStr.replace("null", "");
		String[] keyValuePairs = data.split(",");

		Map<String, String> dataMap = new HashMap<String, String>();

		StringBuilder othersSbr = new StringBuilder();
		for (String pair : keyValuePairs) {
			try {
				String[] entry = pair.split("==", 2);
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

				fMKey = fieldsMap.get(key.trim());

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

		@SuppressWarnings("rawtypes")
		Class cls = net.paladion.model.MultiEventCoreDTO.class;

		net.paladion.model.MultiEventCoreDTO dto = null;
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
				log.error("Error while parsing field: " + field.getName()
						+ "field value:" + dataMap.get(fieldStr) + " error: "
						+ e.getMessage());
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
}
