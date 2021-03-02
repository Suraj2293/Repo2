/**
 * 
 */
package net.paladion.rule;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;

import org.apache.commons.beanutils.BeanUtils;
import org.drools.core.util.StringUtils;

import com.google.common.net.InternetDomainName;

/**
 * @author ankush
 *
 */
@Slf4j
public class InActiveConditions implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  @Getter
  @Setter
  public static Map<String, String> inActiveListMap = new HashMap<String, String>();
  private static Properties clientProp;
  private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static Map<String, Integer> ipTiFeedMap = new HashMap<String, Integer>();
  private static Map<String, Integer> hostTiFeedMap = new HashMap<String, Integer>();
  private static Map<String, Integer> ipTiFeedgte70Map = new HashMap<String, Integer>();
  private static Map<String, Integer> hostTiFeedgte70Map = new HashMap<String, Integer>();
  private static Map<String, Integer> ipTiFeedlt70Map = new HashMap<String, Integer>();
  private static Map<String, Integer> hostTiFeedlt70Map = new HashMap<String, Integer>();

  /**
   * 
   * @param clientProp1
   * @param hostTiFeedlt70Map
   * @param ipTiFeedlt70Map
   * @param hostTiFeedgte70Map
   * @param ipTiFeedgte70Map
   * @param hostTiFeedMap
   * @param ipTiFeedMap
   */
  public InActiveConditions(Properties clientProp1, Map<String, Integer> ipTiFeedMap1,
      Map<String, Integer> hostTiFeedMap1, Map<String, Integer> ipTiFeedgte70Map1,
      Map<String, Integer> hostTiFeedgte70Map1, Map<String, Integer> ipTiFeedlt70Map1,
      Map<String, Integer> hostTiFeedlt70Map1) {
    super();
    clientProp = clientProp1;
    ipTiFeedMap = ipTiFeedMap1;
    hostTiFeedMap = hostTiFeedMap1;
    ipTiFeedgte70Map = ipTiFeedgte70Map1;
    hostTiFeedgte70Map = hostTiFeedgte70Map1;
    ipTiFeedlt70Map = ipTiFeedlt70Map1;
    hostTiFeedlt70Map = hostTiFeedlt70Map1;
  }

  /**
   * 
   * @param m
   * @param data
   * @return
   */
  public boolean InActiveList(MultiEventCoreDTO m, Connection connection, String data, long count,
      String comparator) {
    boolean found = false;
    if (!StringUtils.isEmpty(data)) {
      String lName = "";
      String fnames = "";
      if (data.contains(":")) {
        String[] dataArr = data.split(":");

        if (dataArr.length > 0) {
          lName = dataArr[0].trim();
        }

        if (dataArr.length > 1) {
          fnames = dataArr[1].trim();

          if (StringUtils.isEmpty(fnames)) {
            fnames = inActiveListMap.get(lName);
          }
        }
        dataArr = null;
      } else {
        lName = data.trim();
        fnames = inActiveListMap.get(lName);
      }
      data = null;

      Statement stmt = null;
      ResultSet rs = null;
      StringBuilder sqlSbr = new StringBuilder();
      StringBuilder conditionSbr = new StringBuilder();

      try {
        if (Boolean.parseBoolean(clientProp.getProperty("convertDateToUtc"))) {
          sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        sqlSbr.append("select cu from ");
        sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
        sqlSbr.append("." + lName);

        String[] fnamesArr = fnames.split(",");
        conditionSbr.append(" where ");

        boolean first = true;
        @SuppressWarnings("rawtypes")
        Class cls = MultiEventCoreDTO.class;
        for (int i = 0; i < fnamesArr.length; i++) {
          if (!StringUtils.isEmpty(fnamesArr[i])) {
            String[] maapedFields = fnamesArr[i].split("\\.");

            if (maapedFields != null && maapedFields.length > 1) {
              String adrFieldName = maapedFields[0].trim();
              String activeListField = clientProp.getProperty(maapedFields[1].trim());
              Field field = null;

              if (!first) {
                conditionSbr.append(" and ");
              }

              try {
                field = cls.getDeclaredField(adrFieldName);
                field.setAccessible(true);
                if (field.getType().getName().equalsIgnoreCase("int")
                    || field.getType().getName().equalsIgnoreCase("long")) {
                  String val1 = BeanUtils.getProperty(m, adrFieldName);

                  conditionSbr.append(activeListField);
                  conditionSbr.append("=");
                  if (!StringUtils.isEmpty(val1)) {
                    conditionSbr.append(val1.trim());
                  } else {
                    conditionSbr.append("0");
                  }
                  val1 = null;
                } else if (field.getType().getName().equalsIgnoreCase("java.lang.String")
                    || field.getType().getName().equalsIgnoreCase("java.util.Date")
                    || field.getType().getName().equalsIgnoreCase("java.sql.Timestamp")) {
                  String val1 = BeanUtils.getProperty(m, adrFieldName);

                  conditionSbr.append(activeListField);
                  conditionSbr.append("='");
                  if (!StringUtils.isEmpty(val1)) {
                    conditionSbr.append(val1.trim());
                  } else {
                    conditionSbr.append("-");
                  }
                  conditionSbr.append("'");
                  val1 = null;
                }
                field = null;
                adrFieldName = null;
                activeListField = null;
              } catch (Exception e) {
                log.error("Exception in InActiveList: " + e.getMessage() + " parameters was: "
                    + data + " " + count + " " + comparator + " " + m);
              }
            }
            maapedFields = null;
          }

          first = false;
        }

        if (count != 0 && !StringUtils.isEmpty(comparator)) {
          conditionSbr.append(" and cu ");
          conditionSbr.append(comparator);
          conditionSbr.append(count);
        }

        if (!StringUtils.isEmpty(conditionSbr.toString())) {
          log.debug(sqlSbr.toString() + conditionSbr.toString());
          stmt = connection.createStatement();
          rs = stmt.executeQuery(sqlSbr.toString() + conditionSbr.toString());

          if (rs != null && rs.next()) {
            found = true;
          }
        }

        fnamesArr = null;
      } catch (SQLException e) {
        log.error("Error in InActiveList step: " + e.getMessage() + " SQL is: " + sqlSbr.toString()
            + conditionSbr.toString());
      } catch (Exception e) {
        log.error("Error in InActiveList step: " + e.getMessage() + " SQL is: " + sqlSbr.toString()
            + conditionSbr.toString());
      } finally {
        if (stmt != null) {
          try {
            stmt.close();
            stmt = null;
          } catch (Exception e) {
            log.error(e.getMessage());
          }
        }
        if (rs != null) {
          try {
            rs.close();
            rs = null;
          } catch (Exception e) {
            log.error(e.getMessage());
          }
        }

        sqlSbr.delete(0, sqlSbr.length());
        sqlSbr = null;
        conditionSbr.delete(0, conditionSbr.length());
        conditionSbr = null;

        fnames = null;
        lName = null;
      }
    }

    return found;
  }

  /**
   * 
   * @param m
   * @param data
   * @return
   */
  public boolean NotInActiveList(MultiEventCoreDTO m, Connection connection, String data,
      long count, String comparator) {
    boolean found = true;
    if (!StringUtils.isEmpty(data)) {
      String lName = "";
      String fnames = "";
      if (data.contains(":")) {
        String[] dataArr = data.split(":");

        if (dataArr.length > 0) {
          lName = dataArr[0].trim();
        }

        if (dataArr.length > 1) {
          fnames = dataArr[1].trim();

          if (StringUtils.isEmpty(fnames)) {
            fnames = inActiveListMap.get(lName);
          }
        }
        dataArr = null;
      } else {
        lName = data.trim();
        fnames = inActiveListMap.get(lName);
      }
      data = null;

      Statement stmt = null;
      ResultSet rs = null;
      StringBuilder sqlSbr = new StringBuilder();
      StringBuilder conditionSbr = new StringBuilder();

      try {
        if (Boolean.parseBoolean(clientProp.getProperty("convertDateToUtc"))) {
          sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        sqlSbr.append("select cu from ");
        sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
        sqlSbr.append("." + lName);

        String[] fnamesArr = fnames.split(",");
        conditionSbr.append(" where ");

        boolean first = true;
        @SuppressWarnings("rawtypes")
        Class cls = MultiEventCoreDTO.class;
        for (int i = 0; i < fnamesArr.length; i++) {
          if (!StringUtils.isEmpty(fnamesArr[i])) {
            String[] maapedFields = fnamesArr[i].split("\\.");

            if (maapedFields != null && maapedFields.length > 1) {
              String adrFieldName = maapedFields[0].trim();
              String activeListField = clientProp.getProperty(maapedFields[1].trim());
              Field field = null;

              if (!first) {
                conditionSbr.append(" and ");
              }

              try {
                field = cls.getDeclaredField(adrFieldName);
                field.setAccessible(true);
                if (field.getType().getName().equalsIgnoreCase("int")
                    || field.getType().getName().equalsIgnoreCase("long")) {
                  String val1 = BeanUtils.getProperty(m, adrFieldName);

                  conditionSbr.append(activeListField);
                  conditionSbr.append("=");
                  if (!StringUtils.isEmpty(val1)) {
                    conditionSbr.append(val1.trim());
                  } else {
                    conditionSbr.append("0");
                  }
                  val1 = null;
                } else if (field.getType().getName().equalsIgnoreCase("java.lang.String")
                    || field.getType().getName().equalsIgnoreCase("java.util.Date")
                    || field.getType().getName().equalsIgnoreCase("java.sql.Timestamp")) {
                  String val1 = BeanUtils.getProperty(m, adrFieldName);

                  conditionSbr.append(activeListField);
                  conditionSbr.append("='");
                  if (!StringUtils.isEmpty(val1)) {
                    conditionSbr.append(val1.trim());
                  } else {
                    conditionSbr.append("-");
                  }
                  conditionSbr.append("'");
                  val1 = null;
                }
                field = null;
                adrFieldName = null;
                activeListField = null;
              } catch (Exception e) {
                log.error("Exception in InActiveList: " + e.getMessage() + " parameters was: "
                    + data + " " + count + " " + comparator + " " + m);
              }
            }
            maapedFields = null;
          }

          first = false;
        }

        if (count != 0 && !StringUtils.isEmpty(comparator)) {
          conditionSbr.append(" and cu ");
          conditionSbr.append(comparator);
          conditionSbr.append(count);
        }

        if (!StringUtils.isEmpty(conditionSbr.toString())) {
          log.debug(sqlSbr.toString() + conditionSbr.toString());
          stmt = connection.createStatement();
          rs = stmt.executeQuery(sqlSbr.toString() + conditionSbr.toString());

          if (rs != null && rs.next()) {
            found = false;
          }
        }

        fnamesArr = null;
      } catch (SQLException e) {
        log.error("Error in NotInActiveList step: " + e.getMessage() + " SQL is: "
            + sqlSbr.toString() + conditionSbr.toString());
      } catch (Exception e) {
        log.error("Error in NotInActiveList step: " + e.getMessage() + " SQL is: "
            + sqlSbr.toString() + conditionSbr.toString());
      } finally {
        if (stmt != null) {
          try {
            stmt.close();
            stmt = null;
          } catch (Exception e) {
            log.error(e.getMessage());
          }
        }
        if (rs != null) {
          try {
            rs.close();
            rs = null;
          } catch (Exception e) {
            log.error(e.getMessage());
          }
        }

        sqlSbr.delete(0, sqlSbr.length());
        sqlSbr = null;
        conditionSbr.delete(0, conditionSbr.length());
        conditionSbr = null;

        fnames = null;
        lName = null;
      }
    }

    return found;
  }

  /**
   * 
   * @param m
   * @param connection
   * @param lName
   * @param fVAl
   * @return
   */
  public boolean InActiveList(MultiEventCoreDTO m, Connection connection, String lName, String fVAl) {
    if (!StringUtils.isEmpty(lName) && !StringUtils.isEmpty(fVAl)) {
      return InActiveListCommon(connection, lName, fVAl, null, null);
    }
    return false;
  }

  /**
   * 
   * @param m
   * @param connection
   * @param lName
   * @param fVAl
   * @return
   */
  public boolean InActiveList(MultiEventCoreDTO m, Connection connection, String lName, int fVAl) {
    if (!StringUtils.isEmpty(lName) && fVAl != 0) {
      return InActiveListCommon(connection, lName, String.valueOf(fVAl), null, null);
    }
    return false;
  }

  /**
   * 
   * @param m
   * @param connection
   * @param lName
   * @param fVAl
   * @return
   */
  public boolean InActiveList(MultiEventCoreDTO m, Connection connection, String lName, long fVAl) {
    if (!StringUtils.isEmpty(lName) && fVAl != 0) {
      return InActiveListCommon(connection, lName, String.valueOf(fVAl), null, null);
    }
    return false;
  }

  /**
   * 
   * @param m
   * @param connection
   * @param lName
   * @param fVAl
   * @param comparator
   * @param reputationScore
   * @return
   */
  public boolean InActiveList(MultiEventCoreDTO m, Connection connection, String lName,
      String fVAl, String comparator, int reputationScore) {
    if (!StringUtils.isEmpty(lName) && !StringUtils.isEmpty(fVAl)) {
      return InActiveListCommon(connection, lName, fVAl, comparator, reputationScore);
    }
    return false;
  }

  /**
   * 
   * @param m
   * @param connection
   * @param lName
   * @param fVAl
   * @param comparator
   * @param reputationScore
   * @return
   */
  public boolean InActiveList(MultiEventCoreDTO m, Connection connection, String lName, int fVAl,
      String comparator, Integer reputationScore) {
    if (!StringUtils.isEmpty(lName) && fVAl != 0) {
      return InActiveListCommon(connection, lName, String.valueOf(fVAl), comparator,
          reputationScore);
    }
    return false;
  }

  /**
   * 
   * @param m
   * @param connection
   * @param lName
   * @param fVAl
   * @param comparator
   * @param reputationScore
   * @return
   */
  public boolean InActiveList(MultiEventCoreDTO m, Connection connection, String lName, long fVAl,
      String comparator, Integer reputationScore) {
    if (!StringUtils.isEmpty(lName) && fVAl != 0) {
      return InActiveListCommon(connection, lName, String.valueOf(fVAl), comparator,
          reputationScore);
    }
    return false;
  }

  /**
   * 
   * @param connection
   * @param lName
   * @param fVAl
   * @param comparator
   * @param reputationScore
   * @return
   */
  public boolean InActiveListCommon(Connection connection, String lName, String fVAl,
      String comparator, Integer reputationScore) {
    boolean found = false;

    if (!StringUtils.isEmpty(lName) && !StringUtils.isEmpty(fVAl)) {
      lName = lName.trim().toLowerCase();
      fVAl = fVAl.trim();
      switch (lName) {
        case "iplist":
          found = checkInIpList(fVAl, ipTiFeedMap, comparator, reputationScore);
          break;

        case "hostlist":
          found = checkInHostList(fVAl, hostTiFeedMap, comparator, reputationScore);
          break;

        case "iplistgte70":
          found = checkInIpList(fVAl, ipTiFeedgte70Map, comparator, reputationScore);
          break;

        case "hostlistgte70":
          found = checkInHostList(fVAl, hostTiFeedgte70Map, comparator, reputationScore);
          break;

        case "iplistlt70":
          found = checkInIpList(fVAl, ipTiFeedlt70Map, comparator, reputationScore);
          break;

        case "hostlistlt70":
          found = checkInHostList(fVAl, hostTiFeedlt70Map, comparator, reputationScore);
          break;
      }
    }
    return found;
  }

  /**
   * 
   * @param fVAl
   * @param hostTiFeedMap
   * @param reputationScore
   * @param comparator
   * @return
   */
  private boolean checkInHostList(String fVAl, Map<String, Integer> hostTiFeedMap,
      String comparator, Integer reputationScore) {
    boolean found = false;
    Integer score = null;
    if (Boolean.parseBoolean(clientProp.getProperty("domainTiFullMatch"))) {
      score = hostTiFeedMap.get(fVAl.trim());
      if (score == null && Boolean.parseBoolean(clientProp.getProperty("domainTiPartialMatch"))) {
        score = getValueFromCleandHost(fVAl, hostTiFeedMap);
      }
    } else if (Boolean.parseBoolean(clientProp.getProperty("domainTiPartialMatch"))) {
      score = getValueFromCleandHost(fVAl, hostTiFeedMap);
    }
    if (score != null) {
      if (!StringUtils.isEmpty(comparator) && reputationScore != null) {
        found = checkScore(score, comparator, reputationScore);
      } else {
        found = true;
      }
    }
    score = null;

    return found;
  }

  /**
   * 
   * @param fVAl
   * @param ipTiFeedMap
   * @param reputationScore
   * @param comparator
   * @return
   */
  private boolean checkInIpList(String fVAl, Map<String, Integer> ipTiFeedMap, String comparator,
      Integer reputationScore) {
    Integer score = null;
    boolean found = false;
    score = ipTiFeedMap.get(fVAl.trim());
    if (score != null) {
      if (!StringUtils.isEmpty(comparator) && reputationScore != null) {
        found = checkScore(score, comparator, reputationScore);
      } else {
        found = true;
      }
    }
    score = null;
    return found;
  }

  /**
   * 
   * @param comparator
   * @param reputationScore
   * @return
   */
  private boolean checkScore(Integer score, String comparator, Integer reputationScore) {
    switch (comparator.trim()) {
      case "=":
        if (score.equals(reputationScore)) {
          return true;
        }
        break;
      case ">":
        if (score > reputationScore) {
          return true;
        }
        break;
      case "<":
        if (score < reputationScore) {
          return true;
        }
        break;
      case "<=":
        if (score <= reputationScore) {
          return true;
        }
        break;
      case ">=":
        if (score >= reputationScore) {
          return true;
        }
        break;
    }
    return false;
  }

  /**
   * 
   * @param fVAl
   * @param hostTiFeedMap
   * @return
   */
  private Integer getValueFromCleandHost(String fVAl, Map<String, Integer> hostTiFeedMap) {
    Integer score = null;
    try {
      URL urlObj = null;
      try {
        if (!fVAl.startsWith("http://") && !fVAl.startsWith("https://")) {
          fVAl = "http://" + fVAl;
        }
        urlObj = new URL(fVAl.trim());
      } catch (MalformedURLException e) {
        log.error("Error in getValueFromCleandHost() method Error is: " + e.getMessage());
      }
      String host = urlObj.getHost();

      InternetDomainName internetDomainName = InternetDomainName.from(host).topPrivateDomain();
      if (internetDomainName != null) {
        score = hostTiFeedMap.get(internetDomainName.name());
      }
      internetDomainName = null;
      urlObj = null;
    } catch (Exception e) {
      log.error("Error in getValueFromCleandHost() method Error is: " + e.getMessage());
    }
    return score;
  }

  /**
   * 
   * @param connection
   * @param lName
   * @param fVAl
   * @return
   */
  public boolean NotInActiveList(MultiEventCoreDTO m, Connection connection, String lName,
      String fVAl) {
    if (!StringUtils.isEmpty(lName) && !StringUtils.isEmpty(fVAl)) {
      return NotInActiveListCommon(connection, lName, fVAl, null, null);
    }
    return false;
  }

  /**
   * 
   * @param connection
   * @param lName
   * @param fVAl
   * @return
   */
  public boolean NotInActiveList(MultiEventCoreDTO m, Connection connection, String lName, int fVAl) {
    if (!StringUtils.isEmpty(lName) && fVAl != 0) {
      return NotInActiveListCommon(connection, lName, String.valueOf(fVAl), null, null);
    }
    return false;
  }

  /**
   * 
   * @param connection
   * @param lName
   * @param fVAl
   * @return
   */
  public boolean NotInActiveList(MultiEventCoreDTO m, Connection connection, String lName, long fVAl) {
    if (!StringUtils.isEmpty(lName) && fVAl != 0) {
      return NotInActiveListCommon(connection, lName, String.valueOf(fVAl), null, null);
    }
    return false;
  }

  /**
   * 
   * @param connection
   * @param lName
   * @param fVAl
   * @return
   */
  public boolean NotInActiveList(MultiEventCoreDTO m, Connection connection, String lName,
      String fVAl, String comparator, Integer reputationScore) {
    if (!StringUtils.isEmpty(lName) && !StringUtils.isEmpty(fVAl)) {
      return NotInActiveListCommon(connection, lName, fVAl, comparator, reputationScore);
    }
    return false;
  }

  /**
   * 
   * @param connection
   * @param lName
   * @param fVAl
   * @return
   */
  public boolean NotInActiveList(MultiEventCoreDTO m, Connection connection, String lName,
      int fVAl, String comparator, Integer reputationScore) {
    if (!StringUtils.isEmpty(lName) && fVAl != 0) {
      return NotInActiveListCommon(connection, lName, String.valueOf(fVAl), comparator,
          reputationScore);
    }
    return false;
  }

  /**
   * 
   * @param connection
   * @param lName
   * @param fVAl
   * @return
   */
  public boolean NotInActiveList(MultiEventCoreDTO m, Connection connection, String lName,
      long fVAl, String comparator, Integer reputationScore) {
    if (!StringUtils.isEmpty(lName) && fVAl != 0) {
      return NotInActiveListCommon(connection, lName, String.valueOf(fVAl), comparator,
          reputationScore);
    }
    return false;
  }

  /**
   * 
   * @param m
   * @param connection
   * @param lName
   * @param fName
   * @return
   */
  public boolean NotInActiveListCommon(Connection connection, String lName, String fVAl,
      String comparator, Integer reputationScore) {
    boolean found = false;

    if (!StringUtils.isEmpty(lName) && !StringUtils.isEmpty(fVAl)) {
      lName = lName.trim().toLowerCase();
      fVAl = fVAl.trim();
      switch (lName) {
        case "iplist":
          found = checkInIpList(fVAl, ipTiFeedMap, comparator, reputationScore);
          break;

        case "hostlist":
          found = checkInHostList(fVAl, hostTiFeedMap, comparator, reputationScore);
          break;

        case "iplistgte70":
          found = checkInIpList(fVAl, ipTiFeedgte70Map, comparator, reputationScore);
          break;

        case "hostlistgte70":
          found = checkInHostList(fVAl, hostTiFeedgte70Map, comparator, reputationScore);
          break;

        case "iplistlt70":
          found = checkInIpList(fVAl, ipTiFeedlt70Map, comparator, reputationScore);
          break;

        case "hostlistlt70":
          found = checkInHostList(fVAl, hostTiFeedlt70Map, comparator, reputationScore);
          break;
      }
    }

    if (found) {
      return false;
    } else {
      return true;
    }
  }
}
