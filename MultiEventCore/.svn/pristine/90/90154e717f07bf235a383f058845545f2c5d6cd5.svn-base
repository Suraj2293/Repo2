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
import net.paladion.dao.FinalInsertDaoImpl;
import net.paladion.model.MultiEventCoreDTO;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.paladion.racommon.data.BaseDataCarrier;
import com.paladion.racommon.data.DataCarrier;

/**
 * @author ankush
 *
 */
@Slf4j
public class FinalInsertStep extends
    SparkStep<DataCarrier<JavaRDD<MultiEventCoreDTO>>, DataCarrier<JavaRDD<MultiEventCoreDTO>>> {

  private static final long serialVersionUID = 1L;

  private final Properties clientProp;
  private FinalInsertDaoImpl finalInsertDaoImpl;

  public FinalInsertStep(Properties clientProp1) {
    super();
    clientProp = clientProp1;
    this.finalInsertDaoImpl = new FinalInsertDaoImpl(clientProp);
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
                
                PreparedStatement psRuleMatch = null;
                PreparedStatement psParentChild = null;
                Connection connection = null;
                
                try {
                  Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                  connection =
                      DriverManager.getConnection(clientProp.getProperty("phoenix.database.url"));
                  connection.setAutoCommit(false);

                  StringBuilder sbr1 = new StringBuilder();

                  sbr1.append("upsert into ");
                  sbr1.append(clientProp.getProperty("phoenix.schemaname"));
                  sbr1.append(".threat_rule_match (id, ti, mr, et, cu, bh, bd) values ");
                  sbr1.append("(next value for ");
                  sbr1.append(clientProp.getProperty("phoenix.schemaname"));
                  sbr1.append(".threat_rule_match_id,?,?,?,?,?,?)");

                  psRuleMatch = connection.prepareStatement(sbr1.toString());
                  sbr1.delete(0, sbr1.length());
                  sbr1 = null;
                  
                  StringBuilder sbr2 = new StringBuilder();

                  sbr2.append("upsert into ");
                  sbr2.append(clientProp.getProperty("phoenix.schemaname"));
                  sbr2.append(".threat_rule_parent_child_mapping (ti, mr, dt, et, tn, tt, rr) values ");
                  sbr2.append("(?,?,?,?,?,?,?)");

                  psParentChild = connection.prepareStatement(sbr2.toString());
                  sbr2.delete(0, sbr2.length());
                  sbr2 = null;
                  
                  while (multiEventCoreDTOs.hasNext()) {
                    MultiEventCoreDTO multiEventCoreDTO = multiEventCoreDTOs.next();
                    if (!multiEventCoreDTO.getRuleMatchDTO().getRuleType()
                        .equalsIgnoreCase(clientProp.getProperty("batchRuleName"))) {
                      finalInsertDaoImpl.insertData(multiEventCoreDTO, connection, psParentChild,
                          psRuleMatch);
                    }

                    multiEventCoreDTOList.add(multiEventCoreDTO);
                  }
                  connection.commit();
                } catch (SQLException | ClassNotFoundException e) {
                  log.error("Error in insertThreatRawData method: " + e.getMessage());
                } finally {
                  if (connection != null) {
                    try {
                      connection.close();
                      connection = null;
                    } catch (Exception e) {
                      log.error(e.getMessage());
                    }
                  }
                  if (psParentChild != null) {
                    try {
                      psParentChild.close();
                      psParentChild = null;
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
