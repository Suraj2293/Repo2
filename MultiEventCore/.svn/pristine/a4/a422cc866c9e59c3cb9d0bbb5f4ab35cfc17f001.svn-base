/**
 * 
 */
package net.paladion.steps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.ThresholdDaoImpl;
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
public class ThresholdExecuterStep extends
    SparkStep<DataCarrier<JavaRDD<MultiEventCoreDTO>>, DataCarrier<JavaRDD<MultiEventCoreDTO>>> {

  private static final long serialVersionUID = 1L;

  private final Properties clientProp;
  private ThresholdDaoImpl thresholdDaoImpl;

  public ThresholdExecuterStep(Properties clientProp1) {
    super();
    clientProp = clientProp1;
    this.thresholdDaoImpl = new ThresholdDaoImpl(clientProp);
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

                try {
                  Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                  connection =
                      DriverManager.getConnection(clientProp.getProperty("phoenix.database.url"));
                  connection.setAutoCommit(false);

                  while (multiEventCoreDTOs.hasNext()) {
                    MultiEventCoreDTO multiEventCoreDTO = multiEventCoreDTOs.next();
                    RuleMatchDTO ruleMatchDTO = multiEventCoreDTO.getRuleMatchDTO();

                    if (!ruleMatchDTO.getRuleType().equalsIgnoreCase(
                        clientProp.getProperty("batchRuleName"))
                        && !StringUtils.isEmpty(ruleMatchDTO.getThresholdApplicable())
                        && ruleMatchDTO.getThresholdApplicable().equalsIgnoreCase("yes")
                        && !StringUtils.isEmpty(ruleMatchDTO.getTimeWindowUnit())
                        && ruleMatchDTO.getTimeWindowValue() != 0) {

                      Boolean thresholdApplied =
                          thresholdDaoImpl.threshodApplicable(multiEventCoreDTO,
                              connection);
                      if (thresholdApplied) {
                        multiEventCoreDTOList.add(multiEventCoreDTO);
                      }
                    } else {
                      if (!ruleMatchDTO.getRuleSubType().equalsIgnoreCase(
                          clientProp.getProperty("followedByRuleName"))) {
                        multiEventCoreDTOList.add(multiEventCoreDTO);
                      }
                    }
                  }
                  connection.commit();
                } catch (SQLException | ClassNotFoundException e) {
                  log.error("Error in insertThresholdRuleMatch: " + e.getMessage());
                } finally {
                  if (connection != null) {
                    try {
                      connection.close();
                      connection = null;
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
