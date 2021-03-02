package net.paladion.util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class UtilityTools implements Serializable{

  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

/**
   * This method converts input stream data into string.
   * 
   * @param inputStream : input needs to convert
   * 
   * @return out
   */
  public static StringBuilder convertStream(InputStream inputStream) {

    final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    final StringBuilder out = new StringBuilder();
    StringWriter output = new StringWriter();
    char[] buffer = new char[4096];
    // long count = 0;
    int n = 0;
    try {
      while (-1 != (n = reader.read(buffer))) {
        output.write(buffer, 0, n);
        // count += n;
      }
    } catch (IOException e) {
      log.error("Exception ocurred {} ", e.getMessage());
    }
    return out.append(output.toString());

  }

  /**
   * This method reads the properties file and put fields into map.
   * 
   * @param propertyFilePath : input properties file needs to map
   * 
   * @return map
   */
  public static Map<String, String> convertPropertyToMap(String propertyFilePath) {

    Map<String, String> map = new TreeMap<String, String>();
    try {
      File file = new File(propertyFilePath);
      if (!file.exists()) {
        throw new FileNotFoundException(propertyFilePath);
      }
      FileInputStream fileInput = new FileInputStream(file.getAbsoluteFile());
      Properties properties = new Properties();
      properties.load(fileInput);
      fileInput.close();

      Enumeration<Object> enuKeys = properties.keys();
      while (enuKeys.hasMoreElements()) {
        String keyData = (String) enuKeys.nextElement();
        String valueData = properties.getProperty(keyData);
        map.put(keyData, valueData);
      }
    } catch (Exception e) {
      log.error("Exception ocurred {} ", e.getMessage());
    }
    return map;
  }
  /**
   * This method reads the connector data and put fields into map
   * 
   * @param rawData : input data needs to map
   * 
   * @return cefMap
   */
  static public Map<String, String> CEFToMapConverter(final String rawData,
      final String connectorName, String mappingpath) {
    final Map<String, String> cefMap = new HashMap<>();
    try {

      if (!StringUtils.isBlank(rawData)) {
        final int i = StringUtils.ordinalIndexOf(rawData, "|", 7);
        final String header = rawData.substring(0, i + 1);
        final String[] headerarry = header.split("\\|");
        if (headerarry.length >= 7) {

          if ("sentinel".equalsIgnoreCase(connectorName)) {
            String event_time = headerarry[0].substring(7, 27);
            cefMap.put("Event Time", event_time);
          } else if ("waf".equalsIgnoreCase(connectorName)) {
            cefMap.put("Version", headerarry[0]);
          }
          cefMap.put("Device Vendor", headerarry[1]);
          cefMap.put("Device Product", headerarry[2]);
          cefMap.put("Device Version", headerarry[3]);
          cefMap.put("Signature ID", headerarry[4]);
          cefMap.put("Threat Name", headerarry[5]);

          String stringSeverity = headerarry[6];
          log.info("Severity from Threat : "+stringSeverity);

          Map<String, String> map = UtilityTools.convertPropertyToMap(mappingpath);

          if ("sentinel".equalsIgnoreCase(connectorName)) {
            cefMap.put("severity", stringSeverity);
          } else {
            if (!map.isEmpty() && !StringUtils.isBlank(stringSeverity)) {
              if (stringSeverity != null && stringSeverity.matches("^-?\\d+$")) {
                cefMap.put("severity", stringSeverity);
              } else {
                if (map.get(stringSeverity.toLowerCase()) != null) {
                  cefMap.put("severity", map.get(stringSeverity.toLowerCase()));
                } else {
                  cefMap.put("severity", "1");
                }
              }
            } else {
              cefMap.put("severity", "1");
            }
          }

        }
        String ordata = rawData.substring(i + 1);
        ordata = StringUtils.replace(ordata, "\\=", "~equalto~");
        String od = ordata.replace("\\=", "~equalto~");

        if (ordata.equals(od)) {
          log.debug("true");
        }
        StringBuffer ordata1 = new StringBuffer(ordata);
        int eqp = 0;
        int spp = 0;

        for (int j = 0; j < ordata1.length(); j++) {

          final char c = ordata1.charAt(j);
          if (c == '=') {
            eqp = j;
          }
          if (c == ' ') {
            spp = j;
          }
          if (eqp != 0 && spp != 0) {
            if (spp < eqp) {
              ordata1 = ordata1.replace(spp, spp + 1, "~splitter~");
              eqp = 0;
              spp = 0;
            }
          }
        }

        final String[] newDataarry = ordata1.toString().split("~splitter~");
        for (final String record : newDataarry) {
          if (record != null && record.length() > 0) {
            final String[] keyvalue = record.split("=");
            if (keyvalue.length > 1) {
              cefMap.put(keyvalue[0].trim(), keyvalue[1].trim().replaceAll("~equalto~", "="));
            } else if (keyvalue.length == 1) {
              cefMap.put(keyvalue[0].trim(), "");
            }
          }

        }
      }
    } catch (final Exception e) {
      log.error("Exception ocurred {} ", e.getMessage());
    }
    log.info("Severity in Threat : "+cefMap.get("severity"));
    return cefMap;
  }

  /**
   * This method converts the map into multi map
   * 
   * @param map : input needs to convert into multimap
   * 
   * @return multiMap
   */
  public static MultiMap getMultiMap(Map<String, String> map) {
    MultiMap multiMap = null;
    if (map != null && !map.isEmpty()) {
      try {
        Set<String> entry = map.keySet();
        Iterator<String> itr = entry.iterator();
        multiMap = new MultiValueMap();
        while (itr.hasNext()) {
          String key = itr.next();
          multiMap.put(((String) map.get(key)).toUpperCase(), key);
        }
      } catch (Exception e) {
        log.error("Exception ocurred {} ", e.getMessage());
      }
    } else {
      log.debug("Data Mapping Is Empty" + map);
    }
    return multiMap;
  }

}
