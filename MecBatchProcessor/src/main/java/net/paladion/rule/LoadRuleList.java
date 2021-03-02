/**
 * 
 */
package net.paladion.rule;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.RuleMatchDTO;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author ankush
 *
 */
@Slf4j
public class LoadRuleList implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy hh:mm");
  private static List<RuleMatchDTO> ruleList;
  private static Map<String, Integer> ruleNameIdMap = new HashMap<String, Integer>();

  /**
   * 
   * @param clientProp1
   * @param reloadRules
   * @return
   */
  public static List<RuleMatchDTO> getRuleList(Properties clientProp1, Boolean reloadRules) {
    if (ruleList == null || reloadRules) {
      ruleList = getRulesList(clientProp1);
    }
    return ruleList;
  }


  /**
   * 
   * @return
   */
  public static List<RuleMatchDTO> getRulesList(Properties clientProp) {
    List<RuleMatchDTO> rulesList = new ArrayList<RuleMatchDTO>();
    try {
      FileInputStream rulesStream =
          new FileInputStream(new File(clientProp.getProperty("activeRulesCsvPath")));

      String rulesStr = IOUtils.toString(rulesStream,"UTF-8");

      if (!StringUtils.isEmpty(rulesStr)) {
        String[] rulesStrArr = rulesStr.split("\n");
        
        for (String rule : rulesStrArr) {
          if (!StringUtils.isEmpty(rule)) {
            RuleMatchDTO ruleMatchDTO = new RuleMatchDTO();

            String[] ruleArr = rule.split("~#~");
            try {
              Date expiryDate = null;
              if (ruleArr.length > 11 && !StringUtils.isEmpty(ruleArr[11])) {
                try {
                  expiryDate = formatter.parse(ruleArr[11].trim());
                } catch (ParseException e) {
                  log.error("Rule expiry date is not in currect format: " + ruleArr[11]);
                }
              }
              Integer ruleId = 0;
              String ruleType = ruleArr[1].trim();

              if (ruleType.equalsIgnoreCase(clientProp.getProperty("batchRuleName"))
                  && (expiryDate == null || expiryDate.after(new Date()))) {
                ruleId = Integer.parseInt(ruleArr[0].trim());
                ruleMatchDTO.setRuleId(ruleId);
                ruleMatchDTO.setRuleType(ruleArr[1].trim());
                ruleMatchDTO.setRuleSubType(ruleArr[2].trim());

                if (ruleArr[2].trim()
                    .equalsIgnoreCase(clientProp.getProperty("followedByRuleName"))) {
                  ruleNameIdMap.put(ruleArr[3].trim().toLowerCase(), ruleId);
                }

                ruleMatchDTO.setRuleName(ruleArr[3].trim());

                if (ruleArr.length > 5 && !StringUtils.isEmpty(ruleArr[5])) {
                  ruleMatchDTO.setThresholdApplicable(ruleArr[5].trim());
                } else {
                  ruleMatchDTO.setThresholdApplicable("No");
                }

                if (ruleArr.length > 6 && !StringUtils.isEmpty(ruleArr[6])) {
                  ruleMatchDTO.setEventAThresholdValue(Integer.parseInt(ruleArr[6].trim()));
                } else {
                  ruleMatchDTO.setEventAThresholdValue(0);
                }
                
                if (ruleArr.length > 7 && !StringUtils.isEmpty(ruleArr[7])) {
                  ruleMatchDTO.setEventBThresholdValue(Integer.parseInt(ruleArr[7].trim()));
                } else {
                  ruleMatchDTO.setEventBThresholdValue(0);
                }

                if (ruleArr.length > 8 && !StringUtils.isEmpty(ruleArr[8])) {
                  String[] timeWindowApplicableArr = ruleArr[8].trim().split("(?<=\\d)(?=\\D)");

                  if (timeWindowApplicableArr != null && timeWindowApplicableArr.length >= 2) {
                    if (!StringUtils.isEmpty(timeWindowApplicableArr[0])) {
                      ruleMatchDTO.setTimeWindowValue(Integer.parseInt(timeWindowApplicableArr[0]
                          .trim()));
                    }
                    ruleMatchDTO.setTimeWindowUnit(timeWindowApplicableArr[1]);
                  }
                  timeWindowApplicableArr = null;
                } else {
                  ruleMatchDTO.setTimeWindowUnit("NA");
                  ruleMatchDTO.setTimeWindowValue(0);
                }

                if (ruleArr.length > 9 && !StringUtils.isEmpty(ruleArr[9])) {
                  ruleMatchDTO.setRulePriority(Integer.parseInt(ruleArr[9].trim()));
                } else {
                  ruleMatchDTO.setRulePriority(0);
                }

                if (ruleArr.length > 10 && !StringUtils.isEmpty(ruleArr[10])) {
                  ruleMatchDTO.setThresholdCondition(ruleArr[10].trim());
                } else {
                  ruleMatchDTO.setThresholdCondition("NA");
                }

                if (ruleArr.length > 12 && !StringUtils.isEmpty(ruleArr[12])) {
                  ruleMatchDTO.setIdenticalColStr(ruleArr[12].trim());
                } else {
                  ruleMatchDTO.setIdenticalColStr("NA");
                }

                if (ruleArr.length > 13 && !StringUtils.isEmpty(ruleArr[13])) {
                  ruleMatchDTO.setDistinctColStr(ruleArr[13].trim());
                } else {
                  ruleMatchDTO.setDistinctColStr("NA");
                }

                if (ruleArr.length > 14 && !StringUtils.isEmpty(ruleArr[14])) {
                  ruleMatchDTO.setLightweightStr(ruleArr[14].trim());
                } else {
                  ruleMatchDTO.setLightweightStr("NA");
                }

                if (ruleArr.length > 15 && !StringUtils.isEmpty(ruleArr[15])) {
                  Integer rId = ruleNameIdMap.get(ruleArr[15].trim().toLowerCase());
                  if (rId != null) {
                    ruleMatchDTO.setFollwedByRuleName(String.valueOf(rId));
                  } else {
                    ruleMatchDTO.setFollwedByRuleName("NA");
                  }
                  rId = null;
                } else {
                  ruleMatchDTO.setFollwedByRuleName("NA");
                }

                if (ruleArr.length > 16 && !StringUtils.isEmpty(ruleArr[16])) {
                  ruleMatchDTO.setSeverity(ruleArr[16].trim());
                } else {
                  ruleMatchDTO.setSeverity("NA");
                }

                if (ruleArr.length > 17 && !StringUtils.isEmpty(ruleArr[17])) {
                  ruleMatchDTO.setRecommendations(ruleArr[17].trim());
                } else {
                  ruleMatchDTO.setRecommendations("NA");
                }

                if (ruleArr.length > 18 && !StringUtils.isEmpty(ruleArr[18])) {
                  ruleMatchDTO.setActions(ruleArr[18].trim());
                } else {
                  ruleMatchDTO.setActions("NA");
                }

                ruleArr = null;
                rulesList.add(ruleMatchDTO);
              }

            } catch (Exception e) {
              log.error("Exception in rule String: " + rule + " Error is:" + e.getMessage());
            }
          }
          rule = null;
        }
        rulesStrArr = null;
      }
      rulesStr = null;
      rulesStream.close();

    } catch (IOException e) {
      log.error("Exception in getRulesList method: " + e);
    }

    return rulesList;
  }
}
