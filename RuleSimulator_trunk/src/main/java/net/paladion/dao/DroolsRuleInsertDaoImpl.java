/**
 * 
 */
package net.paladion.dao;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.RuleMatchDTO;
import net.paladion.util.DaoUtils;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.schema.TableNotFoundException;

/**
 * @author ankush
 *
 */
@Slf4j
public class DroolsRuleInsertDaoImpl extends DaoUtils implements Serializable {
  private static final long serialVersionUID = 1L;
  private final Properties clientProp;
  private final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private final DateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  public DroolsRuleInsertDaoImpl(Properties clientProp) {
    this.clientProp = clientProp;
  }

  public void insertDroolsRuleData(MultiEventCoreDTO multiEventCoreDTO, Connection connection,
      String[] variables, PreparedStatement psRuleMatch, RuleMatchDTO ruleMatchDTO) {
    try {
      ruleMatchDTO.setThreatId(multiEventCoreDTO.getThreatId());
      LongOrNull(1, psRuleMatch, multiEventCoreDTO.getThreatId());
      TimestampOrNull(2, psRuleMatch, multiEventCoreDTO.getEventTime());
      
      @SuppressWarnings("rawtypes")
      Class cls = MultiEventCoreDTO.class;
      Integer psValue = 3;
      for (int i = 0; i < variables.length; i++) {
        Field field = null;
        field = cls.getDeclaredField(variables[i]);
        try {
          field = cls.getDeclaredField(variables[i]);
          field.setAccessible(true);
          if (field.getType().getName().equalsIgnoreCase("int")) {
            String val1 = BeanUtils.getProperty(multiEventCoreDTO, field.getName());
            Integer intVal = null;

            if (!StringUtils.isEmpty(val1)) {
              intVal = Integer.parseInt(val1.trim());
            }

            IntOrNull(psValue, psRuleMatch, intVal);
            intVal = null;
            val1 = null;
          } else if (field.getType().getName().equalsIgnoreCase("long")) {

            String val1 = BeanUtils.getProperty(multiEventCoreDTO, field.getName());

            Long longVal = null;
            if (!StringUtils.isEmpty(val1)) {
              longVal = Long.parseLong(val1.trim());
            }
            LongOrNull(psValue, psRuleMatch, longVal);
            longVal = null;
            val1 = null;
          } else if (field.getType().getName().equalsIgnoreCase("java.lang.String")) {
            psRuleMatch.setString(psValue,
                BeanUtils.getProperty(multiEventCoreDTO, field.getName()));
          } else if (field.getType().getName().equalsIgnoreCase("java.util.Date")
              || field.getType().getName().equalsIgnoreCase("java.sql.Timestamp")) {
            String dtStr = BeanUtils.getProperty(multiEventCoreDTO, field.getName());

            Date dt = null;
            if (!StringUtils.isEmpty(dtStr)) {
              try {
                dt = formatter.parse(dtStr.trim());
              } catch (ParseException e) {
                log.error(e.getMessage());
              }
            }

            TimestampOrNull(psValue, psRuleMatch, dt);
            dtStr = null;
            dt = null;
          }
          field = null;
        } catch (Exception e) {
          e.getMessage();
        }
        psValue++;
      }

      IntOrNull(psValue, psRuleMatch, ruleMatchDTO.getRuleId());
      psRuleMatch.setArray(psValue + 1,
          connection.createArrayOf("VARCHAR", multiEventCoreDTO.getDataArray()));

      psRuleMatch.executeUpdate();
      psValue = null;
    } catch (Exception e) {
      log.error("Error in insertDroolsRuleData method while inserting the drools rule: "
          + e.getMessage());
    }
  }

  /**
   * 
   * @param r
   * @param multiEventCoreDTO
   * @param connection
   */
  public void saveLightWeightRule(RuleMatchDTO r, MultiEventCoreDTO multiEventCoreDTO,
      Connection connection) {
    if (!StringUtils.isEmpty(r.getLightweightStr())) {
      try {
        String[] arr = r.getLightweightStr().split("~####~");
        if (arr.length > 1) {
          String lName = arr[0];
          String fNames = arr[1];

          if (!StringUtils.isEmpty(fNames) && !StringUtils.isEmpty(lName)) {
            String[] fNamesArr = fNames.split("~###~");

            StringBuilder sblrVar = new StringBuilder();
            StringBuilder whereSbr = new StringBuilder();
            StringBuilder sqlUpdateSbr = new StringBuilder();

            if (fNamesArr != null && fNamesArr.length > 0) {
              @SuppressWarnings("rawtypes")
              Class cls = MultiEventCoreDTO.class;
              Statement stmt = null;
              try {
                boolean first = true;

                for (String fName : fNamesArr) {
                  String[] regexFieldArr = fName.split("~##~");

                  String getField = "";
                  String regex = "";
                  String putField = "";
                  if (regexFieldArr != null && regexFieldArr.length > 2) {
                    getField = regexFieldArr[0];
                    regex = regexFieldArr[1];
                    putField = regexFieldArr[2];
                  } else {
                    getField = regexFieldArr[0];
                    putField = regexFieldArr[0];
                  }


                  getField = getField.trim();
                  putField = putField.trim();

                  String data = clientProp.getProperty(putField);
                  sblrVar.append(data + ",");

                  Field field = null;
                  try {
                    if (!first) {
                      whereSbr.append(" and ");
                    }

                    field = cls.getDeclaredField(getField);
                    field.setAccessible(true);
                    if (field.getType().getName().equalsIgnoreCase("int")
                        || field.getType().getName().equalsIgnoreCase("long")) {
                      String val1 = BeanUtils.getProperty(multiEventCoreDTO, field.getName());
                      if (StringUtils.isEmpty(val1)) {
                        val1 = "0";
                      }
                      whereSbr.append(data + "=" + val1);
                      val1 = null;
                    } else if (field.getType().getName().equalsIgnoreCase("java.lang.String")) {
                      String data1 = BeanUtils.getProperty(multiEventCoreDTO, field.getName());
                      if (StringUtils.isEmpty(data1)) {
                        data1 = "-";
                      } else if (!StringUtils.isEmpty(regex)) {
                        try {
                          Pattern pattern = Pattern.compile(regex);
                          Matcher matcher = pattern.matcher(data1);
                          if (matcher.find()) {
                            data1 = matcher.group(1);
                          }
                        } catch (Exception e) {
                          log.error("Regex failed, list name is: " + lName + " data is: " + data1
                              + " regex is: " + regex);
                        }
                      }

                      whereSbr.append(data + "='" + data1 + "'");
                      data1 = null;
                    }
                    data = null;
                    field = null;
                  } catch (Exception e) {
                    e.getMessage();
                  }

                  first = false;
                  getField = null;
                  regex = null;
                  putField = null;
                }
    
                sqlUpdateSbr.append("upsert into ");
                sqlUpdateSbr.append(clientProp.getProperty("phoenix.schemaname"));
                sqlUpdateSbr.append(".");
                sqlUpdateSbr.append(lName);
                sqlUpdateSbr.append(" (");
                sqlUpdateSbr.append(sblrVar.toString());
                sqlUpdateSbr.append("md,cu ) select ");
                sqlUpdateSbr.append(sblrVar.toString());
                sqlUpdateSbr.append("to_date('");

                if (Boolean.parseBoolean(clientProp.getProperty("convertDateToUtc"))) {
                  formatter2.setTimeZone(TimeZone.getTimeZone("UTC"));
                }
                sqlUpdateSbr.append(formatter2.format(multiEventCoreDTO.getEventTime()));
                sqlUpdateSbr.append("'),cu+1");
                sqlUpdateSbr.append(" from ");
                sqlUpdateSbr.append(clientProp.getProperty("phoenix.schemaname"));
                sqlUpdateSbr.append(".");
                sqlUpdateSbr.append(lName);
                sqlUpdateSbr.append(" where ");

                stmt = connection.createStatement();

                int i = stmt.executeUpdate(sqlUpdateSbr.toString() + " " + whereSbr.toString());

                if (i == 0) {
                  insertRecord(multiEventCoreDTO, fNamesArr, lName, connection);
                }
              } catch (TableNotFoundException e) {
                log.error("TableNotFoundException: " + e.getMessage());

                StringBuilder createTableSbr = new StringBuilder();
                createTableSbr.append("create table IF NOT EXISTS ");
                createTableSbr.append(clientProp.getProperty("phoenix.schemaname"));
                createTableSbr.append(".");
                createTableSbr.append(lName);
                createTableSbr.append(" (");

                StringBuilder tableFiledSbr = new StringBuilder();
                StringBuilder tablePKSbr = new StringBuilder();

                boolean first = true;
                for (String fName : fNamesArr) {
                  String[] regexFieldArr = fName.split("~##~");

                  if (regexFieldArr != null && regexFieldArr.length > 2) {
                    fName = regexFieldArr[2];
                  } else {
                    fName = regexFieldArr[0];
                  }

                  fName = fName.trim();
                  Field field = null;
                  if (!first) {
                    tablePKSbr.append(",");
                  }
                  try {
                    field = cls.getDeclaredField(fName);
                    field.setAccessible(true);
                    if (field.getType().getName().equalsIgnoreCase("int")) {
                      tableFiledSbr
                          .append(clientProp.getProperty(fName) + " integer(11) not null ");
                    } else if (field.getType().getName().equalsIgnoreCase("long")) {
                      tableFiledSbr.append(clientProp.getProperty(fName) + " BIGINT not null ");
                    } else if (field.getType().getName().equalsIgnoreCase("java.lang.String")) {
                      tableFiledSbr.append(clientProp.getProperty(fName)
                          + " varchar(200) not null ");
                    }
                    tablePKSbr.append(clientProp.getProperty(fName));
                    field = null;
                  } catch (Exception ex) {
                    ex.getMessage();
                  }
                  tableFiledSbr.append(",");

                  first = false;
                }
                tableFiledSbr.append("cu integer(11), md timestamp ");

                createTableSbr.append(tableFiledSbr.toString());
                createTableSbr.append(", CONSTRAINT PK PRIMARY KEY(");
                createTableSbr.append(tablePKSbr);
                createTableSbr.append(")) IMMUTABLE_ROWS=true, COMPRESSION='GZ'");

                PreparedStatement psmt = null;
                try {
                  psmt = connection.prepareStatement(createTableSbr.toString());
                  psmt.executeUpdate();
                } catch (SQLException e1) {
                  log.error("Error in creating the table: " + e1.getMessage());
                } finally {
                  if (psmt != null) {
                    try {
                      psmt.close();
                      psmt = null;
                    } catch (Exception e2) {
                      log.error(e2.getMessage());
                    }
                  }
                }
                insertRecord(multiEventCoreDTO, fNamesArr, lName, connection);

                createTableSbr.delete(0, createTableSbr.length());
                createTableSbr = null;
                tableFiledSbr.delete(0, tableFiledSbr.length());
                tableFiledSbr = null;
                tablePKSbr.delete(0, tablePKSbr.length());
                tablePKSbr = null;
              } catch (SQLException e) {
                log.error("Error in saveLightWeightRule while saving the data: " + e.getMessage());
              } finally {
                if (stmt != null) {
                  try {
                    stmt.close();
                    stmt = null;
                  } catch (Exception e) {
                    log.error(e.getMessage());
                  }
                }
              }
              sblrVar.delete(0, sblrVar.length());
              sblrVar = null;
              sqlUpdateSbr.delete(0, sqlUpdateSbr.length());
              sqlUpdateSbr = null;

            }
            fNamesArr = null;
          }
          lName = null;
          fNames = null;
        }
        arr = null;
      } catch (Exception e) {
        log.error("Error in saveLightWeightRule method" + e.getMessage());
      }
    }
  }

  /**
   * Insert data into lightweight table
   * 
   * @param multiEventCoreDTO
   * @param fNamesArr
   * @param lName
   * @param connection
   */
  public void insertRecord(MultiEventCoreDTO multiEventCoreDTO, String[] fNamesArr, String lName,
      Connection connection) {
    StringBuilder sblrVar = new StringBuilder();
    StringBuilder sblrdata = new StringBuilder();

    sblrVar.append("md,cu");
    sblrdata.append("?,?");

    for (String fName : fNamesArr) {
      String[] regexFieldArr = fName.split("~##~");

      String putField = "";
      if (regexFieldArr != null && regexFieldArr.length > 2) {
        putField = regexFieldArr[2];
      } else {
        putField = regexFieldArr[0];
      }
      String data = clientProp.getProperty(putField);
      sblrVar.append("," + data);
      sblrdata.append(",?");
    }

    StringBuilder sqlSbr = new StringBuilder();

    sqlSbr.append("upsert into ");
    sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
    sqlSbr.append(".");
    sqlSbr.append(lName);
    sqlSbr.append(" (");
    sqlSbr.append(sblrVar.toString());
    sqlSbr.append(") values (");
    sqlSbr.append(sblrdata.toString());
    sqlSbr.append(")");

    @SuppressWarnings("rawtypes")
    Class cls = MultiEventCoreDTO.class;
    PreparedStatement psmt = null;
    try {
      psmt = connection.prepareStatement(sqlSbr.toString());
      TimestampOrNull(1, psmt, multiEventCoreDTO.getEventTime());
      IntOrNull(2, psmt, 1);

      Integer psValue = 3;
      for (String fName : fNamesArr) {
        String[] regexFieldArr = fName.split("~##~");

        String getField = "";
        String regex = "";
        String putField = "";
        if (regexFieldArr != null && regexFieldArr.length > 2) {
          getField = regexFieldArr[0];
          regex = regexFieldArr[1];
          putField = regexFieldArr[2];
        } else {
          getField = regexFieldArr[0];
          putField = regexFieldArr[0];
        }

        getField = getField.trim();
        putField = putField.trim();

        String data = clientProp.getProperty(putField);

        sblrVar.append("," + data);
        sblrdata.append(",?");

        Field field = null;
        try {
          field = cls.getDeclaredField(getField);
          field.setAccessible(true);
          if (field.getType().getName().equalsIgnoreCase("int")) {
            String val1 = BeanUtils.getProperty(multiEventCoreDTO, field.getName());
            int intVal = 0;

            if (!StringUtils.isEmpty(val1)) {
              intVal = Integer.parseInt(val1.trim());
            }

            IntOrNull(psValue, psmt, intVal);
            intVal = 0;
            val1 = null;
          } else if (field.getType().getName().equalsIgnoreCase("long")) {

            String val1 = BeanUtils.getProperty(multiEventCoreDTO, field.getName());

            long longVal = 0;
            if (!StringUtils.isEmpty(val1)) {
              longVal = Long.parseLong(val1.trim());
            }
            LongOrNull(psValue, psmt, longVal);
            longVal = 0;
            val1 = null;
          } else if (field.getType().getName().equalsIgnoreCase("java.lang.String")) {
            String data1 = BeanUtils.getProperty(multiEventCoreDTO, field.getName());
            if (StringUtils.isEmpty(data1)) {
              data1 = "-";
            } else if (!StringUtils.isEmpty(regex)) {
              try {
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(data1);
                if (matcher.find()) {
                  data1 = matcher.group(1);
                }
              } catch (Exception e) {
                log.error("Regex failed, list name is: " + lName + " data is: " + data1
                    + " regex is: " + regex);
              }
            }
            psmt.setString(psValue, data1);
            data1 = null;
          }

          field = null;
        } catch (Exception e) {
          e.getMessage();
        }
        psValue++;

        getField = null;
        regex = null;
        putField = null;
      }
      psmt.executeUpdate();
      psValue = null;
    } catch (Exception e) {
      log.error("Error in saveLightWeightRule while saving the data: " + e.getMessage());
    } finally {
      if (psmt != null) {
        try {
          psmt.close();
          psmt = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
    }
  }
}
