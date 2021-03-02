/**
 * 
 */
package net.paladion.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.RuleMatchDTO;
import net.paladion.util.DaoUtils;

/**
 * @author ankush
 *
 */
@Slf4j
public class FinalInsertDaoImpl extends DaoUtils implements Serializable {
  private static final long serialVersionUID = 1L;
  private final Properties clientProp;

  public FinalInsertDaoImpl(Properties clientProp) {
    this.clientProp = clientProp;
  }

  public void insertData(MultiEventCoreDTO multiEventCoreDTO, Connection connection,
      PreparedStatement psParentChild, PreparedStatement psRuleMatch) {
    try {
      RuleMatchDTO ruleMatchDTO = multiEventCoreDTO.getRuleMatchDTO();

      LongOrNull(1, psRuleMatch, multiEventCoreDTO.getThreatId());
      IntOrNull(2, psRuleMatch, ruleMatchDTO.getRuleId());
      TimestampOrNull(3, psRuleMatch, multiEventCoreDTO.getEventTime());
      psRuleMatch.setString(4, multiEventCoreDTO.getCustomerName());
      psRuleMatch.setLong(5, multiEventCoreDTO.getEventId());
			psRuleMatch.setTimestamp(6,
					multiEventCoreDTO.getDeviceReceiptTime());
      psRuleMatch.executeUpdate();
    } catch (Exception e) {
      log.error("Exception in FinalInsertDaoImpl, method: insertData, Error is: " + e.getMessage());
    }

    if (multiEventCoreDTO.getRuleMatchDTO().getChildIdList() != null
        && !multiEventCoreDTO.getRuleMatchDTO().getChildIdList().isEmpty()) {
      try {
        LongOrNull(1, psParentChild, multiEventCoreDTO.getThreatId());
        IntOrNull(2, psParentChild, multiEventCoreDTO.getRuleMatchDTO().getRuleId());
        psParentChild.setArray(
            3,
            connection.createArrayOf("BIGINT", multiEventCoreDTO.getRuleMatchDTO().getChildIdList()
                .toArray()));
        TimestampOrNull(4, psParentChild, multiEventCoreDTO.getEventTime());
        psParentChild.setString(5, clientProp.getProperty("threatRaw.tableName"));
        psParentChild.setString(6, multiEventCoreDTO.getRuleMatchDTO().getTimeWindowValue()
            + multiEventCoreDTO.getRuleMatchDTO().getTimeWindowUnit());
        psParentChild.setString(7, "Stream");
        psParentChild.executeUpdate();
      } catch (SQLException e) {
        log.error("Exception in FinalInsertDaoImpl, method: insertData while inserting parent child data, Error is: "
            + e.getMessage());
      }
    }
  }
}
