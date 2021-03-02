/**
 * 
 */
package net.paladion.model;

import java.util.Date;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * @author ankush
 *
 */
public class RuleMatchDTO {
  @Getter
  @Setter
  private String ruleType;
  @Getter
  @Setter
  private String ruleSubType;
  @Getter
  @Setter
  private String ruleName;
  @Getter
  @Setter
  private int ruleId;
  @Getter
  @Setter
  private String thresholdApplicable;
  @Getter
  @Setter
  private int eventAThresholdValue;
  @Getter
  @Setter
  private int eventBThresholdValue;
  @Getter
  @Setter
  private String ruleCondition;
  @Getter
  @Setter
  private int timeWindowValue;
  @Getter
  @Setter
  private String timeWindowUnit;
  @Getter
  @Setter
  public int rulePriority;
  @Getter
  @Setter
  private String thresholdCondition;
  @Getter
  @Setter
  private long threatId;
  @Getter
  @Setter
  private String identicalColStr;
  @Getter
  @Setter
  private String distinctColStr;
  @Getter
  @Setter
  private Date expiryDate;
  @Getter
  @Setter
  private String lightweightStr;
  @Getter
  @Setter
  private String follwedByRuleName;
  @Getter
  @Setter
  private String severity;
  @Getter
  @Setter
  private String recommendations;
  @Getter
  @Setter
  private String actions;
  @Getter
  @Setter
  private List<Long> childIdList;

  public RuleMatchDTO(String ruleType, String ruleSubType, String ruleName, int ruleId,
      String thresholdApplicable, int eventAThresholdValue, int eventBThresholdValue,
      String timeWindowUnit, int timeWindowValue, int rulePriority, String thresholdCondition,
      String identicalColStr, String distinctColStr, String lightweightStr,
      String follwedByRuleName, String severity, String recommendations, String actions) {
    super();
    this.ruleType = ruleType;
    this.ruleSubType = ruleSubType;
    this.ruleName = ruleName;
    this.ruleId = ruleId;
    this.thresholdApplicable = thresholdApplicable;
    this.eventAThresholdValue = eventAThresholdValue;
    this.eventBThresholdValue = eventBThresholdValue;
    this.timeWindowUnit = timeWindowUnit;
    this.timeWindowValue = timeWindowValue;
    this.rulePriority = rulePriority;
    this.thresholdCondition = thresholdCondition;
    this.identicalColStr = identicalColStr;
    this.distinctColStr = distinctColStr;
    this.lightweightStr = lightweightStr;
    this.follwedByRuleName = follwedByRuleName;
    this.severity = severity;
    this.recommendations = recommendations;
    this.actions = actions;
  }

  public RuleMatchDTO() {}

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "RuleMatchDTO [ruleType=" + ruleType + ",ruleSubType=" + ruleSubType + ", ruleName="
        + ruleName + ", ruleId=" + ruleId + ", thresholdApplicable=" + thresholdApplicable
        + ", eventAThresholdValue=" + eventAThresholdValue + ", eventBThresholdValue="
        + eventBThresholdValue + ", ruleCondition=" + ruleCondition + ", rulePriority="
        + rulePriority + ", thresholdCondition=" + thresholdCondition + ", threatId=" + threatId
        + ", identicalColStr=" + identicalColStr + ", distinctColStr=" + distinctColStr
        + ", expiryDate=" + expiryDate + ", lightweightStr=" + lightweightStr
        + ", follwedByRuleName=" + follwedByRuleName + ", severity=" + severity
        + ", recommendations=" + recommendations + ", timeWindowUnit=" + timeWindowUnit
        + ", timeWindowValue=" + timeWindowValue + ", actions=" + actions + ", childIdList="
        + childIdList + "]";
  }
}
