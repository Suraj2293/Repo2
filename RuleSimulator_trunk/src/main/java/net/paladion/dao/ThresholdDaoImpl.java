/**
 * 
 */
package net.paladion.dao;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.RuleMatchDTO;
import net.paladion.util.DaoUtils;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author ankush
 *
 */
@Slf4j
public class ThresholdDaoImpl extends DaoUtils implements Serializable {

  private static final long serialVersionUID = 1L;
  private final Properties clientProp;
  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  public ThresholdDaoImpl(Properties clientProp1) {
    this.clientProp = clientProp1;
  }

  /**
   * 
   * @param multiEventCoreDTO
   * @param connection
   * @return
   */
  public Boolean threshodApplicable(MultiEventCoreDTO multiEventCoreDTO, Connection connection) {
    Boolean tApplied = false;
    RuleMatchDTO ruleMatchDTO = multiEventCoreDTO.getRuleMatchDTO();
    Integer tWAVal = ruleMatchDTO.getTimeWindowValue();
    String tWAUnit = ruleMatchDTO.getTimeWindowUnit();

    StringBuilder iCSqlSbr = new StringBuilder();
    String iColStr = ruleMatchDTO.getIdenticalColStr();
    if (!StringUtils.isEmpty(iColStr) && !iColStr.equalsIgnoreCase("na")
        && !iColStr.equalsIgnoreCase("null")) {
      String[] iColArr = iColStr.split(",");
      if (iColArr != null && iColArr.length > 0) {
        for (String iCol : iColArr) {
          String colName = clientProp.getProperty(iCol.trim());

          try {
            Class<?> type =
                new PropertyDescriptor(iCol.trim(), MultiEventCoreDTO.class).getPropertyType();
            if (type.getName().equalsIgnoreCase("int") || type.getName().equalsIgnoreCase("long")) {
              String val = BeanUtils.getProperty(multiEventCoreDTO, iCol.trim());

              if (StringUtils.isEmpty(val)) {
                iCSqlSbr.append(" and " + colName + " is null ");
              } else {
                iCSqlSbr.append(" and " + colName + "= " + val.trim() + " ");
                val = null;
              }
            } else if (type.getName().equalsIgnoreCase("java.lang.String")) {
              String val = BeanUtils.getProperty(multiEventCoreDTO, iCol.trim());

              if (StringUtils.isEmpty(val)) {
                iCSqlSbr.append(" and " + colName + " is null ");
              } else {
                val = StringEscapeUtils.escapeSql(val);

                iCSqlSbr.append(" and " + colName + "= '" + val + "'");
                val = null;
              }

            } else if (type.getName().equalsIgnoreCase("java.util.Date")
                || type.getName().equalsIgnoreCase("java.sql.Timestamp")) {

              String val = BeanUtils.getProperty(multiEventCoreDTO, iCol.trim());

              if (StringUtils.isEmpty(val)) {
                iCSqlSbr.append(" and " + colName + " is null ");
              } else {
                try {
                  String time = sdf.format(val);
                  iCSqlSbr.append(" and " + colName + "= '" + time + "' ");

                  val = null;
                  time = null;
                } catch (Exception e) {
                  log.error(e.getMessage());
                }
              }
            }
          } catch (IllegalAccessException e) {
            log.error(e.getMessage());
          } catch (IllegalArgumentException e) {
            log.error(e.getMessage());
          } catch (InvocationTargetException e) {
            log.error(e.getMessage());
          } catch (IntrospectionException e) {
            log.error(e.getMessage());
          } catch (NoSuchMethodException e) {
            log.error(e.getMessage());
          }
          colName = null;
        }
      }
      iColArr = null;
    }
    iColStr = null;

    StringBuilder dSCSbr = new StringBuilder();
    String dCStr = ruleMatchDTO.getDistinctColStr();
    if (!StringUtils.isEmpty(dCStr) && !dCStr.equalsIgnoreCase("na")
        && !dCStr.equalsIgnoreCase("null")) {
      String[] dCStrArr = dCStr.split(",");
      if (dCStrArr != null && dCStrArr.length > 0) {
        boolean first = true;
        for (String dCol : dCStrArr) {
          String cName = clientProp.getProperty(dCol.trim());
          if (!first) {
            dSCSbr.append(",");
          }

          dSCSbr.append(cName);

          Class<?> type;
          try {
            type = new PropertyDescriptor(dCol.trim(), MultiEventCoreDTO.class).getPropertyType();
            if (type.getName().equalsIgnoreCase("int") || type.getName().equalsIgnoreCase("long")) {
              iCSqlSbr.append(" and " + cName + " != 0");
            } else {
              iCSqlSbr.append(" and " + cName + " is not null");
            }
          } catch (IntrospectionException e) {
            log.error(e.getMessage());
          }

          cName = null;
          first = false;
        }
      }
      dCStrArr = null;
    }
    dCStr = null;

    StringBuilder tACSbr = new StringBuilder();
    StringBuilder tACDistinctSbr = new StringBuilder();
    if (tWAVal != null && tWAVal != 0 && !StringUtils.isEmpty(tWAUnit)) {
      if (Boolean.parseBoolean(clientProp.getProperty("convertDateToUtc"))) {
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      }
      String eventTimeStr = sdf.format(multiEventCoreDTO.getEventTime());

      String multiplyStr = "";
      if (tWAUnit.equalsIgnoreCase("s")) {
        multiplyStr = "/(24.0*60*60)";
      } else if (tWAUnit.equalsIgnoreCase("m")) {
        multiplyStr = "/(24.0*60)";
      } else if (tWAUnit.equalsIgnoreCase("h")) {
        multiplyStr = "/(24.0)";
      } else if (tWAUnit.equalsIgnoreCase("d")) {
        multiplyStr = "";
      } else if (tWAUnit.equalsIgnoreCase("mo")) {
        multiplyStr = "*30";
      }

      tACSbr.append(" et >= to_date('");
      tACSbr.append(eventTimeStr);
      tACSbr.append("')-(");
      tACSbr.append(tWAVal);
      tACSbr.append(")");
      tACSbr.append(multiplyStr);
      tACSbr.append(" and et <= to_date('");
      tACSbr.append(eventTimeStr);
      tACSbr.append("')");

      if (!StringUtils.isEmpty(dSCSbr.toString())) {
        tACDistinctSbr.append(" a.et >= to_date('");
        tACDistinctSbr.append(eventTimeStr);
        tACDistinctSbr.append("')-(");
        tACDistinctSbr.append(tWAVal);
        tACDistinctSbr.append(")");
        tACDistinctSbr.append(multiplyStr);
        tACDistinctSbr.append(" and a.et <= to_date('");
        tACDistinctSbr.append(eventTimeStr);
        tACDistinctSbr.append("')");
      }

      multiplyStr = null;
      eventTimeStr = null;
    }

    if (ruleMatchDTO.getRuleSubType()
        .equalsIgnoreCase(clientProp.getProperty("followedByRuleName"))) {
      int baseEventCount = 0;
      if (ruleMatchDTO.getEventBThresholdValue() > 1) {
        baseEventCount =
            checkThresholdCondition(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr, tACDistinctSbr,
                ruleMatchDTO.getEventBThresholdValue(), connection);
      } else {
        baseEventCount = multiEventCoreDTO.getBaseEventCount();
      }

      if (baseEventCount != 0) {
        boolean followedByRuleThreshold =
            checkFollowedByRuleThreshold(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr,
                tACDistinctSbr, connection);

        if (followedByRuleThreshold) {
          tApplied = true;
          multiEventCoreDTO.setBaseEventCount(baseEventCount);
          if (multiEventCoreDTO.getRuleMatchDTO().getThresholdCondition()
              .equalsIgnoreCase("everymore")) {
            List<Long> childIdList = new ArrayList<Long>();
            if (ruleMatchDTO.getEventBThresholdValue() > 1) {
              childIdList =
                  getChildIdList(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr, tACDistinctSbr,
                      ruleMatchDTO.getEventBThresholdValue(), connection, multiEventCoreDTO
                          .getRuleMatchDTO().getRuleId());
            }

            List<Long> childIdListFollowedBy =
                getChildIdList(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr, tACDistinctSbr,
                    ruleMatchDTO.getEventAThresholdValue(), connection,
                    Integer.parseInt(multiEventCoreDTO.getRuleMatchDTO().getFollwedByRuleName()));

            childIdList.addAll(childIdListFollowedBy);

            multiEventCoreDTO.getRuleMatchDTO().setChildIdList(childIdList);
          } else if (multiEventCoreDTO.getRuleMatchDTO().getThresholdCondition()
              .equalsIgnoreCase("every")) {
            List<Long> childIdList = new ArrayList<Long>();
            if (ruleMatchDTO.getEventBThresholdValue() > 1) {
              childIdList =
                  updateParentIdAndGetChildList(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr,
                      tACDistinctSbr, connection, multiEventCoreDTO.getRuleMatchDTO().getRuleId());
            }

            List<Long> childIdListFollowedBy =
                getChildIdList(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr, tACDistinctSbr,
                    ruleMatchDTO.getEventAThresholdValue(), connection,
                    Integer.parseInt(multiEventCoreDTO.getRuleMatchDTO().getFollwedByRuleName()));

            childIdList.addAll(childIdListFollowedBy);

            multiEventCoreDTO.getRuleMatchDTO().setChildIdList(childIdList);
          }
        }
      }
    } else {
      int baseEventCount =
          checkThresholdCondition(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr, tACDistinctSbr,
              ruleMatchDTO.getEventAThresholdValue(), connection);

      if (baseEventCount != 0) {
        tApplied = true;
        multiEventCoreDTO.setBaseEventCount(baseEventCount);
        if (multiEventCoreDTO.getRuleMatchDTO().getThresholdCondition()
            .equalsIgnoreCase("everymore")) {
          List<Long> childIdList =
              getChildIdList(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr, tACDistinctSbr,
                  ruleMatchDTO.getEventAThresholdValue(), connection, multiEventCoreDTO
                      .getRuleMatchDTO().getRuleId());
          multiEventCoreDTO.getRuleMatchDTO().setChildIdList(childIdList);
        } else if (multiEventCoreDTO.getRuleMatchDTO().getThresholdCondition()
            .equalsIgnoreCase("every")
            || multiEventCoreDTO.getRuleMatchDTO().getThresholdCondition()
                .equalsIgnoreCase("onfirstthreshold")) {
          List<Long> childIdList =
              updateParentIdAndGetChildList(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr,
                  tACDistinctSbr, connection, multiEventCoreDTO.getRuleMatchDTO().getRuleId());
          multiEventCoreDTO.getRuleMatchDTO().setChildIdList(childIdList);
        }
      }
    }

    tACDistinctSbr.delete(0, tACSbr.length());
    tACDistinctSbr = null;
    tACSbr.delete(0, tACSbr.length());
    tACSbr = null;
    iCSqlSbr.delete(0, iCSqlSbr.length());
    iCSqlSbr = null;
    dSCSbr.delete(0, dSCSbr.length());
    dSCSbr = null;

    return tApplied;
  }

  private List<Long> getChildIdList(StringBuilder dSCSbr, StringBuilder tACSbr,
      MultiEventCoreDTO multiEventCoreDTO, StringBuilder iCSqlSbr, StringBuilder tACDistinctSbr,
      int tVal, Connection connection, int ruleId) {
    List<Long> childIdList = new ArrayList<Long>();
    StringBuilder sqlSbr = new StringBuilder();
    if (StringUtils.isEmpty(dSCSbr.toString())) {
      sqlSbr.append("SELECT ti from ");
      sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
      sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName") + " where ");
      sqlSbr.append(tACSbr.toString());
      sqlSbr.append(" and ct='");
      sqlSbr.append(multiEventCoreDTO.getCustomer());
      sqlSbr.append("' and mr=");
      sqlSbr.append(ruleId);
      sqlSbr.append(" and ti!=");
      sqlSbr.append(multiEventCoreDTO.getThreatId());
      sqlSbr.append(" and mp is null ");
      sqlSbr.append(iCSqlSbr.toString());
      sqlSbr.append(" limit ");
      sqlSbr.append(tVal);
    } else {
      sqlSbr.append(" select a.ti as ti from ");
      sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
      sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName")
          + " a, (SELECT (FIRST_VALUE (ti) WITHIN GROUP ( order by et desc)) as ti1 from ");
      sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
      sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName") + " where ");
      sqlSbr.append(tACSbr.toString());
      sqlSbr.append(" and ct='");
      sqlSbr.append(multiEventCoreDTO.getCustomer());
      sqlSbr.append("' and mr=");
      sqlSbr.append(ruleId);
      sqlSbr.append(" and ti!=");
      sqlSbr.append(multiEventCoreDTO.getThreatId());
      sqlSbr.append(" and mp is null ");
      sqlSbr.append(iCSqlSbr.toString());
      sqlSbr.append(" group by ");
      sqlSbr.append(dSCSbr.toString());
      sqlSbr.append(" limit ");
      sqlSbr.append(tVal);
      sqlSbr.append(") b where a.ti=b.ti1 and ");
      sqlSbr.append(tACDistinctSbr);
      sqlSbr.append(" and a.mr=");
      sqlSbr.append(ruleId);
    }

    log.debug("List query in getChildIdList method: " + sqlSbr);

    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = connection.prepareStatement(sqlSbr.toString());
      rs = ps.executeQuery();
      while (rs.next()) {
        childIdList.add(rs.getLong("ti"));
      }
    } catch (Exception e) {
      log.error("Error in getChildIdList method: " + e.getMessage() + " SQL is: "
          + sqlSbr.toString());
    } finally {
      if (rs != null) {
        try {
          rs.close();
          rs = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
      if (ps != null) {
        try {
          ps.close();
          ps = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
    }
    sqlSbr.delete(0, sqlSbr.length());
    sqlSbr = null;

    return childIdList;
  }

  /**
   * 
   * @param dSCSbr
   * @param tACSbr
   * @param multiEventCoreDTO
   * @param iCSqlSbr
   * @param tACDistinctSbr
   * @param connection
   * @return
   */
  private boolean checkFollowedByRuleThreshold(StringBuilder dSCSbr, StringBuilder tACSbr,
      MultiEventCoreDTO multiEventCoreDTO, StringBuilder iCSqlSbr, StringBuilder tACDistinctSbr,
      Connection connection) {
    boolean thresholdMatch = false;

    PreparedStatement ps = null;
    ResultSet rs = null;
    String sql = null;
    try {
      sql =
          createCountSqlQuery(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr, tACDistinctSbr,
              multiEventCoreDTO.getRuleMatchDTO().getEventAThresholdValue(),
              Integer.parseInt(multiEventCoreDTO.getRuleMatchDTO().getFollwedByRuleName()));

      if (!StringUtils.isEmpty(sql)) {
        ps = connection.prepareStatement(sql);
        rs = ps.executeQuery();

        if (rs != null) {
          while (rs.next()) {
            thresholdMatch = true;
          }
        }
      }
    } catch (Exception e) {
      log.error("Error in checkFollowedByRuleThreshold method for followed by rule query: "
          + e.getMessage() + " SQL is: " + sql);
    } finally {
      sql = null;
      if (rs != null) {
        try {
          rs.close();
          rs = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
      if (ps != null) {
        try {
          ps.close();
          ps = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
    }

    return thresholdMatch;
  }

  /**
   * 
   * @param dSCSbr
   * @param tACSbr
   * @param multiEventCoreDTO
   * @param iCSqlSbr
   * @param tACDistinctSbr
   * @param connection
   * @param isFollowedByRule
   */
  private List<Long> updateParentIdAndGetChildList(StringBuilder dSCSbr, StringBuilder tACSbr,
      MultiEventCoreDTO multiEventCoreDTO, StringBuilder iCSqlSbr, StringBuilder tACDistinctSbr,
      Connection connection, int ruleId) {
    List<Long> childIdList = new ArrayList<Long>();
    StringBuilder sqlSbr = new StringBuilder();
    if (StringUtils.isEmpty(dSCSbr.toString())) {
      sqlSbr.append("SELECT ti as t1,et from ");
      sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
      sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName") + " where ");
      sqlSbr.append(tACSbr.toString());
      sqlSbr.append(" and ct='");
      sqlSbr.append(multiEventCoreDTO.getCustomer());
      sqlSbr.append("' and mr=");
      sqlSbr.append(ruleId);
      sqlSbr.append(" and ti!=");
      sqlSbr.append(multiEventCoreDTO.getThreatId());
      sqlSbr.append(" and mp is null ");
      sqlSbr.append(iCSqlSbr.toString());
    } else {
      sqlSbr.append(" select a.ti as t1,a.et as et from ");
      sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
      sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName")
          + " a, (SELECT (FIRST_VALUE (ti) WITHIN GROUP ( order by et desc)) as ti1 from ");
      sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
      sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName") + " where ");
      sqlSbr.append(tACSbr.toString());
      sqlSbr.append(" and ct='");
      sqlSbr.append(multiEventCoreDTO.getCustomer());
      sqlSbr.append("' and mr=");
      sqlSbr.append(ruleId);
      sqlSbr.append(" and ti!=");
      sqlSbr.append(multiEventCoreDTO.getThreatId());
      sqlSbr.append(" and mp is null ");
      sqlSbr.append(iCSqlSbr.toString());
      sqlSbr.append(" group by ");
      sqlSbr.append(dSCSbr.toString());
      sqlSbr.append(") b where a.ti=b.ti1 and ");
      sqlSbr.append(tACDistinctSbr);
      sqlSbr.append(" and a.mr=");
      sqlSbr.append(ruleId);
    }

    log.debug("List query in updateParentIdAndGetChildList method: " + sqlSbr);

    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = connection.prepareStatement(sqlSbr.toString());
      rs = ps.executeQuery();

      if (rs != null) {
        String sqlUpdate = "";
        PreparedStatement psUpdate = null;

        sqlUpdate =
            "upsert into " + clientProp.getProperty("phoenix.schemaname") + "."
                + clientProp.getProperty("partialRuleMatch.tableName")
                + "(et,ct,mr,ti,mp) values (?,?,?,?,?)";

        psUpdate = connection.prepareStatement(sqlUpdate);

        try {
          while (rs.next()) {
            try {
              childIdList.add(rs.getLong("t1"));
              TimestampOrNull(1, psUpdate, new Timestamp(rs.getDate("et").getTime()));
              psUpdate.setString(2, multiEventCoreDTO.getCustomer());
              IntOrNull(3, psUpdate, ruleId);
              LongOrNull(4, psUpdate, rs.getLong("t1"));
              LongOrNull(5, psUpdate, multiEventCoreDTO.getThreatId());
              psUpdate.executeUpdate();
            } catch (Exception e) {
              log.error("Error in updateParentId method while updating the record: "
                  + e.getMessage() + " event time: " + new Timestamp(rs.getDate("et").getTime())
                  + " threat id: " + rs.getLong("t1") + " ruleMatchDTO: "
                  + multiEventCoreDTO.getRuleMatchDTO());
            }
          }

          TimestampOrNull(1, psUpdate, multiEventCoreDTO.getEventTime());
          psUpdate.setString(2, multiEventCoreDTO.getCustomer());
          IntOrNull(3, psUpdate, ruleId);
          LongOrNull(4, psUpdate, multiEventCoreDTO.getThreatId());
          LongOrNull(5, psUpdate, multiEventCoreDTO.getThreatId());
          psUpdate.executeUpdate();

          connection.commit();
        } catch (Exception e) {
          log.error("Error in updateParentId method while updating the record: " + e.getMessage()
              + " ruleMatchDTO: " + multiEventCoreDTO.getRuleMatchDTO());
        } finally {
          if (psUpdate != null) {
            try {
              psUpdate.close();
              psUpdate = null;
            } catch (Exception e) {
              log.error(e.getMessage());
            }
          }
        }
        sqlUpdate = null;
      }
    } catch (Exception e) {
      log.error("Error in updateParentId method: " + e.getMessage() + " SQL is: "
          + sqlSbr.toString());
    } finally {
      if (rs != null) {
        try {
          rs.close();
          rs = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
      if (ps != null) {
        try {
          ps.close();
          ps = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
    }
    sqlSbr.delete(0, sqlSbr.length());
    sqlSbr = null;

    return childIdList;
  }

  /**
   * 
   * @param dSCSbr
   * @param tACSbr
   * @param multiEventCoreDTO
   * @param iCSqlSbr
   * @param tACDistinctSbr
   * @param thresholdValue
   * @param connection
   * @return
   */
  private int checkThresholdCondition(StringBuilder dSCSbr, StringBuilder tACSbr,
      MultiEventCoreDTO multiEventCoreDTO, StringBuilder iCSqlSbr, StringBuilder tACDistinctSbr,
      int thresholdValue, Connection connection) {
    int count = 0;
    PreparedStatement ps = null;
    ResultSet rs = null;
    String sql = null;
    try {
      if (multiEventCoreDTO.getRuleMatchDTO().getThresholdCondition().equalsIgnoreCase("everymore")
          || multiEventCoreDTO.getRuleMatchDTO().getThresholdCondition().equalsIgnoreCase("every")) {
        sql =
            createCountSqlQuery(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr, tACDistinctSbr,
                thresholdValue, multiEventCoreDTO.getRuleMatchDTO().getRuleId());
      } else if (multiEventCoreDTO.getRuleMatchDTO().getThresholdCondition()
          .equalsIgnoreCase("onfirstthreshold")) {
        sql =
            createFirstThesholdSqlQuery(dSCSbr, tACSbr, multiEventCoreDTO, iCSqlSbr,
                tACDistinctSbr, thresholdValue, multiEventCoreDTO.getRuleMatchDTO().getRuleId());
      } else if (multiEventCoreDTO.getRuleMatchDTO().getThresholdCondition()
          .equalsIgnoreCase("onfirstevent")) {
        sql = createFirstEventSqlQuery(tACSbr, multiEventCoreDTO);
      }

      log.debug("Sql in checkThresholdCondition method: " + sql);

      if (!StringUtils.isEmpty(sql)) {
        ps = connection.prepareStatement(sql);
        rs = ps.executeQuery();

        if (rs != null) {
          while (rs.next()) {
            count = rs.getInt("cu");
          }
        }
      }
    } catch (Exception e) {
      log.error("Error in checkThresholdCondition method: " + e.getMessage() + " SQL is: " + sql);
    } finally {
      if (rs != null) {
        try {
          rs.close();
          rs = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
      if (ps != null) {
        try {
          ps.close();
          ps = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
    }

    sql = null;
    return count;
  }

  /**
   * 
   * @param dSCSbr
   * @param tACSbr
   * @param multiEventCoreDTO
   * @param iCSqlSbr
   * @param tACDistinctSbr
   * @param tVal
   * @param ruleId
   * @return
   */
  private String createFirstThesholdSqlQuery(StringBuilder dSCSbr, StringBuilder tACSbr,
      MultiEventCoreDTO multiEventCoreDTO, StringBuilder iCSqlSbr, StringBuilder tACDistinctSbr,
      int tVal, int ruleId) {
    StringBuilder sqlSbr = new StringBuilder();

    if (StringUtils.isEmpty(dSCSbr.toString())) {
      sqlSbr.append("SELECT sum(ab) as cu from ");
      sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
      sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName") + " where ");
      sqlSbr.append(tACSbr.toString());
      sqlSbr.append(" and ct='");
      sqlSbr.append(multiEventCoreDTO.getCustomer());
      sqlSbr.append("' and mr=");
      sqlSbr.append(ruleId);
      sqlSbr.append(iCSqlSbr.toString());
      sqlSbr.append(" group by mr having sum(ab) >=");
      sqlSbr.append(tVal);
      sqlSbr.append(" and count(mp)=0 ");
    } else {
      sqlSbr.append(" select sum(ab) as cu from ");
      sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
      sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName")
          + " a, (SELECT (FIRST_VALUE (ti) WITHIN GROUP ( order by et desc)) as ti1 from ");
      sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
      sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName") + " where ");
      sqlSbr.append(tACSbr.toString());
      sqlSbr.append(" and ct='");
      sqlSbr.append(multiEventCoreDTO.getCustomer());
      sqlSbr.append("' and mr=");
      sqlSbr.append(ruleId);
      sqlSbr.append(iCSqlSbr.toString());
      sqlSbr.append(" group by ");
      sqlSbr.append(dSCSbr.toString());
      sqlSbr.append(") b where a.ti=b.ti1 and ");
      sqlSbr.append(tACDistinctSbr);
      sqlSbr.append(" and a.mr=");
      sqlSbr.append(ruleId);
      sqlSbr.append(" group by mr having sum(ab) >= ");
      sqlSbr.append(tVal);
      sqlSbr.append(" and count(mp)=0 ");
      sqlSbr.append(" and count(b.ti1) >= ");
      sqlSbr.append(tVal);
    }

    log.debug(sqlSbr.toString());

    return sqlSbr.toString();
  }

  /**
   * 
   * @param dSCSbr
   * @param tACSbr
   * @param multiEventCoreDTO
   * @param iCSqlSbr
   * @param tACDistinctSbr
   * @param ruleId
   * @return
   */
  private String createFirstEventSqlQuery(StringBuilder tACSbr, MultiEventCoreDTO multiEventCoreDTO) {
    StringBuilder sqlSbr = new StringBuilder();
    sqlSbr.append("SELECT count(mr) as cu from ");
    sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
    sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName") + " where ");
    sqlSbr.append(tACSbr.toString());
    sqlSbr.append(" and ct='");
    sqlSbr.append(multiEventCoreDTO.getCustomer());
    sqlSbr.append("' and mr=");
    sqlSbr.append(multiEventCoreDTO.getRuleMatchDTO().getRuleId());
    sqlSbr.append(" and mp is null ");
    sqlSbr.append(" group by mr having count(mr) = 1");

    return sqlSbr.toString();
  }

  /**
   * 
   * @param dSCSbr
   * @param tACSbr
   * @param multiEventCoreDTO
   * @param iCSqlSbr
   * @param tACDistinctSbr
   * @param tVal
   * @param ruleId
   * @param condition
   * @return
   */
  public String createCountSqlQuery(StringBuilder dSCSbr, StringBuilder tACSbr,
      MultiEventCoreDTO multiEventCoreDTO, StringBuilder iCSqlSbr, StringBuilder tACDistinctSbr,
      Integer tVal, int ruleId) {
    StringBuilder sqlSbr = new StringBuilder();

    try {
      if (StringUtils.isEmpty(dSCSbr.toString())) {
        sqlSbr.append("SELECT sum(ab) as cu from ");
        sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
        sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName") + " where ");
        sqlSbr.append(tACSbr.toString());
        sqlSbr.append(" and ct='");
        sqlSbr.append(multiEventCoreDTO.getCustomer());
        sqlSbr.append("' and mr=");
        sqlSbr.append(ruleId);
        sqlSbr.append(" and mp is null ");
        sqlSbr.append(iCSqlSbr.toString());
        sqlSbr.append(" group by mr having sum(ab) >= ");
        sqlSbr.append(tVal);
      } else {
        sqlSbr.append(" select sum(ab) as cu from ");
        sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
        sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName")
            + " a, (SELECT (FIRST_VALUE (ti) WITHIN GROUP ( order by et desc)) as ti1 from ");
        sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
        sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName") + " where ");
        sqlSbr.append(tACSbr.toString());
        sqlSbr.append(" and ct='");
        sqlSbr.append(multiEventCoreDTO.getCustomer());
        sqlSbr.append("' and mr=");
        sqlSbr.append(ruleId);
        sqlSbr.append(" and mp is null ");
        sqlSbr.append(iCSqlSbr.toString());
        sqlSbr.append(" group by ");
        sqlSbr.append(dSCSbr.toString());
        sqlSbr.append(") b where a.ti=b.ti1 and ");
        sqlSbr.append(tACDistinctSbr);
        sqlSbr.append(" and a.mr=");
        sqlSbr.append(ruleId);
        sqlSbr.append(" group by mr having sum(ab) >= ");
        sqlSbr.append(tVal);
        sqlSbr.append(" and count(b.ti1) >= ");
        sqlSbr.append(tVal);
      }
    } catch (Exception e) {
      log.error("Error in createCountSqlQuery: " + e.getMessage());
    }
    log.debug("Sql in createCountSqlQuery method: " + sqlSbr.toString());
    return sqlSbr.toString();
  }
}
