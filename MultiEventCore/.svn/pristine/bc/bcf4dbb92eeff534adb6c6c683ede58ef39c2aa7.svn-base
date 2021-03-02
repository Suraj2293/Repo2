/**
 * 
 */
package net.paladion.steps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.RuleMatchDTO;
import net.paladion.rule.KieSessionService;
import net.paladion.util.ConvertIpToLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.kie.api.runtime.StatelessKieSession;

import com.paladion.racommon.data.BaseDataCarrier;
import com.paladion.racommon.data.DataCarrier;

/**
 * @author ankush
 *
 */
@Slf4j
public class RuleExecuterStep extends
    SparkStep<DataCarrier<JavaRDD<MultiEventCoreDTO>>, DataCarrier<JavaRDD<MultiEventCoreDTO>>> {

  private static final long serialVersionUID = 1L;

  private final Properties clientProp;
  private final Broadcast<Date> broadStartDate;

  public RuleExecuterStep(Properties clientProp1, Broadcast<Date> broadStartDate1) {
    super();
    clientProp = clientProp1;
    broadStartDate = broadStartDate1;
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
                Date startDate = (Date) broadStartDate.value();
                Date currentDate = new Date();

                Long duration = currentDate.getTime() - startDate.getTime();
                Long diffInMinutes = TimeUnit.MILLISECONDS.toMinutes(duration);

                Boolean reloadRules = false;
                if (diffInMinutes
                    % Integer.parseInt(clientProp.getProperty("reloadDroolsInMinutes")) == 0) {
                  reloadRules = true;
                }

                Boolean reloadTiFeed = false;
                if (diffInMinutes
                    % Integer.parseInt(clientProp.getProperty("reloadTiFeedInMinutes")) == 0) {
                  reloadTiFeed = true;
                }

                StatelessKieSession ksession =
                    KieSessionService.getKieSession(clientProp, reloadRules, reloadTiFeed);

                startDate = null;
                currentDate = null;
                duration = null;
                diffInMinutes = null;
                reloadRules = null;

                Connection connection = null;
                // PreparedStatement psmtZone = null;
                // PreparedStatement psmtGeo = null;

                try {
                  Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
                  connection =
                      DriverManager.getConnection(clientProp.getProperty("phoenix.database.url"));

                  ksession.setGlobal("Connection", connection);

                  Map<String, String> networkZoneMap = KieSessionService.networkZoneMap;
                  // psmtZone =
                  // connection
                  // .prepareStatement("select * from (select 'src' as at, zo from "
                  // + clientProp.getProperty("phoenix.schemaname")
                  // +
                  // ".network_zone_master WHERE tin >= ? and cu like ? ORDER BY tin ASC LIMIT 1)as a union all select * from (select 'dest' as at,zo from "
                  // + clientProp.getProperty("phoenix.schemaname")
                  // +
                  // ".network_zone_master WHERE tin >= ? and cu like ? ORDER BY tin ASC LIMIT 1)as b");
                  //
                  // psmtGeo =
                  // connection
                  // .prepareStatement("select * from (select 'src' as at, con, cin from "
                  // + clientProp.getProperty("phoenix.schemaname")
                  // +
                  // ".geo_location_master WHERE tin >= ? ORDER BY tin ASC LIMIT 1)as a union all select * from (select 'dest' as at, con, cin from "
                  // + clientProp.getProperty("phoenix.schemaname")
                  // + ".geo_location_master WHERE tin >= ? ORDER BY tin ASC LIMIT 1)as b");

                  while (multiEventCoreDTOs.hasNext()) {
                    MultiEventCoreDTO multiEventCoreDTO = null;
                    try {
                      multiEventCoreDTO = multiEventCoreDTOs.next();

                      if (!StringUtils.isEmpty(multiEventCoreDTO.getCustomerName())) {
                        multiEventCoreDTO.setSourceAddressLong(ConvertIpToLong
                            .stringIpToLong(multiEventCoreDTO.getSourceAddress()));

                        multiEventCoreDTO.setDestinationAddressLong(ConvertIpToLong
                            .stringIpToLong(multiEventCoreDTO.getDestinationAddress()));
                        
                        if (networkZoneMap != null && !networkZoneMap.isEmpty()) {
                          try {
                            multiEventCoreDTO.setSourceZone(networkZoneMap.get(ConvertIpToLong
                                .stringIpToLong(multiEventCoreDTO.getSourceAddress())
                                + "_"
                                + multiEventCoreDTO.getCustomerId().substring(0, 3)));

                            multiEventCoreDTO.setDestinationZone(networkZoneMap.get(ConvertIpToLong
                                .stringIpToLong(multiEventCoreDTO.getDestinationAddress())
                                + "_"
                                + multiEventCoreDTO.getCustomerId().substring(0, 3)));
                          } catch (Exception e) {
                            log.error("Error while fetching network zone details: "
                                + e.getMessage());
                          }
                        }

                        ksession.execute(multiEventCoreDTO);

                        if (multiEventCoreDTO.getRuleMatchDTOList() != null
                            && !multiEventCoreDTO.getRuleMatchDTOList().isEmpty()) {
                          int listCount = multiEventCoreDTO.getRuleMatchDTOList().size();
                          for (RuleMatchDTO r : multiEventCoreDTO.getRuleMatchDTOList()) {
                            if (listCount > 1) {
                              MultiEventCoreDTO m = (MultiEventCoreDTO) multiEventCoreDTO.clone();
                              m.setRuleMatchDTO(r);
                              multiEventCoreDTOList.add(m);
                            } else {
                              multiEventCoreDTO.setRuleMatchDTO(r);
                              multiEventCoreDTO.setRuleMatchDTOList(null);
                              multiEventCoreDTOList.add(multiEventCoreDTO);
                            }
                          }
                          listCount = 0;
                        }
                      }
                    } catch (NumberFormatException e) {
                      log.error("NumberFormatException in Rule Execution: " + e.getMessage());
                    } catch (Exception e) {
                      log.error("Error in Rule Execution: " + e.getMessage());
                    }
                  }

                  networkZoneMap = null;
                } catch (Exception e) {
                  log.error("Error: " + e.getMessage());
                } finally {
                  // if (psmtGeo != null) {
                  // try {
                  // psmtGeo.close();
                  // psmtGeo = null;
                  // } catch (Exception e) {
                  // log.error(e.getMessage());
                  // }
                  // }
                  // if (psmtZone != null) {
                  // try {
                  // psmtZone.close();
                  // psmtZone = null;
                  // } catch (Exception e) {
                  // log.error(e.getMessage());
                  // }
                  // }
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
