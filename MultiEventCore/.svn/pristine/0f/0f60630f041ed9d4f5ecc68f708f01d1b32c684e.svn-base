/**
 * 
 */
package net.paladion.steps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.DroolsRuleInsertDaoImpl;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.RuleMatchDTO;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.paladion.racommon.data.BaseDataCarrier;
import com.paladion.racommon.data.DataCarrier;

/**
 * @author ankush
 *
 */
@Slf4j
public class DroolsRuleInsertStep extends
    SparkStep<DataCarrier<JavaRDD<MultiEventCoreDTO>>, DataCarrier<JavaRDD<MultiEventCoreDTO>>> {

  private static final long serialVersionUID = 1L;

  private final Properties clientProp;
  private DroolsRuleInsertDaoImpl droolsRuleInsertDaoImpl;

  public DroolsRuleInsertStep(Properties clientProp1) {
    super();
    clientProp = clientProp1;
    this.droolsRuleInsertDaoImpl = new DroolsRuleInsertDaoImpl(clientProp);
  }

  @Override
  public DataCarrier<JavaRDD<MultiEventCoreDTO>> customTransform(
      DataCarrier<JavaRDD<MultiEventCoreDTO>> dataCarrier) {
    JavaRDD<MultiEventCoreDTO> dtoStream = dataCarrier.getPayload();

    JavaRDD<MultiEventCoreDTO> dtoStreamNew =
        dtoStream
            .mapPartitions(new FlatMapFunction<Iterator<MultiEventCoreDTO>, MultiEventCoreDTO>() {
              private static final long serialVersionUID = 1L;

              @Override
              public Iterator<MultiEventCoreDTO> call(Iterator<MultiEventCoreDTO> multiEventCoreDTOs)
                  throws Exception {
                List<MultiEventCoreDTO> multiEventCoreDTOList = new ArrayList<MultiEventCoreDTO>();
                Connection connection = null;
                PreparedStatement psRuleMatch = null;
                try {
                  Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                  connection =
                      DriverManager.getConnection(clientProp.getProperty("phoenix.database.url"));
                  connection.setAutoCommit(false);

                  StringBuilder sblrVariable = new StringBuilder();
                  StringBuilder sblrOperator = new StringBuilder();

                  String[] variables = clientProp.getProperty("variable_sequence").split(",");

                  sblrVariable.append("tI,et,");
                  sblrOperator.append("?,?,");

                  for (int i = 0; i < variables.length; i++) {
                    String data = clientProp.getProperty(variables[i]);

                    if (!StringUtils.isEmpty(data)) {
                      sblrVariable.append(data);
                      sblrOperator.append("?");
                    }
                    data = null;

                    if (i + 1 != variables.length) {
                      sblrVariable.append(",");
                      sblrOperator.append(",");
                    }
                  }

                  sblrVariable.append(",mR,dT");
                  sblrOperator.append(",?,?");

                  StringBuilder sqlSbr = new StringBuilder();

                  sqlSbr.append("upsert into ");
                  sqlSbr.append(clientProp.getProperty("phoenix.schemaname"));
                  sqlSbr.append("." + clientProp.getProperty("partialRuleMatch.tableName") + " (");
                  sqlSbr.append(sblrVariable.toString());
                  sqlSbr.append(") values (");
                  sqlSbr.append(sblrOperator.toString());
                  sqlSbr.append(")");
                  
                  psRuleMatch = connection.prepareStatement(sqlSbr.toString());
         
                  sblrVariable.delete(0, sblrVariable.length());
                  sblrVariable = null;
                  sblrOperator.delete(0, sblrOperator.length());
                  sblrOperator = null;
                  sqlSbr.delete(0, sqlSbr.length());
                  sqlSbr = null;

                  while (multiEventCoreDTOs.hasNext()) {
                    MultiEventCoreDTO multiEventCoreDTO = multiEventCoreDTOs.next();

                    RuleMatchDTO r = multiEventCoreDTO.getRuleMatchDTO();
                    if (r != null) {

                      if (r != null
                          && !StringUtils.isEmpty(r.getActions())
                          && !r.getActions().equalsIgnoreCase("null")
                          && !r.getActions().equalsIgnoreCase("na")
                          && r.getActions().toLowerCase()
                              .contains(clientProp.getProperty("activeListAction"))) {
                        droolsRuleInsertDaoImpl.saveLightWeightRule(r, multiEventCoreDTO,
                            connection);
                      }

                      if (r.getRuleType().equalsIgnoreCase(clientProp.getProperty("batchRuleName"))) {
                        multiEventCoreDTOList.add(multiEventCoreDTO);
                      } else {
                        if (r != null
                            && !StringUtils.isEmpty(r.getActions())
                            && !r.getActions().equalsIgnoreCase("null")
                            && !r.getActions().equalsIgnoreCase("na")
                            && r.getActions().toLowerCase()
                                .contains(clientProp.getProperty("alertTriggerAction"))) {
                          if (!StringUtils.isEmpty(r.getThresholdApplicable())
                              && r.getThresholdApplicable().equalsIgnoreCase("yes")) {
                            droolsRuleInsertDaoImpl.insertDroolsRuleData(multiEventCoreDTO,
                                connection, variables, psRuleMatch, r);
                          }
                          multiEventCoreDTOList.add(multiEventCoreDTO);
                        }
                      }
                      r = null;
                    }
                  }
                  variables = null;
                  connection.commit();
                } catch (SQLException | ClassNotFoundException e) {
                  log.error("Error in DroolsRuleInsertStep step: " + e.getMessage());
                } finally {
                  if (connection != null) {
                    try {
                      connection.close();
                      connection = null;
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
                return multiEventCoreDTOList.iterator();
              }
            });

    // dtoStreamNew.persist(StorageLevel.MEMORY_ONLY_SER());
    // dtoStreamNew.print(1);
    DataCarrier<JavaRDD<MultiEventCoreDTO>> newDataCarrier =
        new BaseDataCarrier<JavaRDD<MultiEventCoreDTO>>(dtoStreamNew, null);
    return newDataCarrier;
  }
}
