/**
 * 
 */
package net.paladion.model;

import java.sql.Timestamp;

import lombok.Getter;
import lombok.Setter;

/**
 * @author ankush
 *
 */
public class ThreatDroolsRuleMatch {
  @Getter
  @Setter
  private long threatId;
  @Getter
  @Setter
  private Timestamp eventTime;
  @Getter
  @Setter
  private int baseEventCount;
  @Getter
  @Setter
  private String deviceVendor;
  @Getter
  @Setter
  private String categoryBehavior;
  @Getter
  @Setter
  private String categoryOutcome;
  @Getter
  @Setter
  private String message;
  @Getter
  @Setter
  private String deviceProcessName;
  @Getter
  @Setter
  private String deviceProduct;
  @Getter
  @Setter
  private String destinationUserName;
  @Getter
  @Setter
  private String deviceAddress;
  @Getter
  @Setter
  private String destinationAddress;
  @Getter
  @Setter
  private String customerURI;
  @Getter
  @Setter
  private String sourceAddress;
  @Getter
  @Setter
  private String sourceUserName;
  @Getter
  @Setter
  private String name;
  @Getter
  @Setter
  private String deviceEventClassId;
  @Getter
  @Setter
  private String applicationProtocol;
  @Getter
  @Setter
  private String destinationHostName;
  @Getter
  @Setter
  private int destinationPort;
  @Getter
  @Setter
  private String deviceAction;
  @Getter
  @Setter
  private String deviceCustomString1;
  @Getter
  @Setter
  private String deviceCustomString2;
  @Getter
  @Setter
  private String deviceCustomString3;
  @Getter
  @Setter
  private String deviceCustomString4;
  @Getter
  @Setter
  private String deviceCustomString5;
  @Getter
  @Setter
  private String deviceCustomString6;
  @Getter
  @Setter
  private String deviceDirection;
  @Getter
  @Setter
  private String deviceEventCategory;
  @Getter
  @Setter
  private String deviceHostName;
  @Getter
  @Setter
  private String deviceInboundInterface;
  @Getter
  @Setter
  private String deviceOutboundInterface;
  @Getter
  @Setter
  private Timestamp deviceReceiptTime;
  @Getter
  @Setter
  private String deviceSeverity;
  @Getter
  @Setter
  private int externalId;
  @Getter
  @Setter
  private String fileName;
  @Getter
  @Setter
  private String requestUrl;
  @Getter
  @Setter
  private String sourceHostName;
  @Getter
  @Setter
  private int sourcePort;
  @Getter
  @Setter
  private String transportProtocol;
  @Getter
  @Setter
  private int bytesIn;
  @Getter
  @Setter
  private int bytesOut;
  @Getter
  @Setter
  private String requestContext;
  @Getter
  @Setter
  private String requestClientApplication;
  @Getter
  @Setter
  private String destinationProcessName;
  @Getter
  @Setter
  private String destinationServiceName;
  @Getter
  @Setter
  private int deviceCustomNumber1;
  @Getter
  @Setter
  private int deviceCustomNumber2;
  @Getter
  @Setter
  private int deviceCustomNumber3;
  @Getter
  @Setter
  private Timestamp endTime;
  @Getter
  @Setter
  private long eventId;
  @Getter
  @Setter
  private String filePath;
  @Getter
  @Setter
  private String flexString1;
  @Getter
  @Setter
  private String reason;
  @Getter
  @Setter
  private String requestCookie;
  @Getter
  @Setter
  private String requestCookies;
  @Getter
  @Setter
  private String requestMethod;
  @Getter
  @Setter
  private String requestUrlFileName;
  @Getter
  @Setter
  private String sourceNtDomain;
  @Getter
  @Setter
  private Timestamp startTime;
  @Getter
  @Setter
  private String adrCustomStr1;
  @Getter
  @Setter
  private String adrCustomStr2;
  @Getter
  @Setter
  private String adrCustomStr3;
  @Getter
  @Setter
  private String adrCustomStr4;
  @Getter
  @Setter
  private int adrCustomeNumber1;
  @Getter
  @Setter
  private long matchedParentId;
  @Getter
  @Setter
  private int matchedRuleId;
  @Getter
  @Setter
  private long ruleIdActed;
  @Getter
  @Setter
  private String customerName;
  @Getter
  @Setter
  private String severity;
  @Getter
  @Setter
  private String others;
  @Getter
  @Setter
  private String[] dataArray;

  public ThreatDroolsRuleMatch() {
    super();
  }

  @Override
  public String toString() {
    return "ThreatDroolsRuleMatch [threatId=" + threatId + ", eventTime=" + eventTime
        + ", baseEventCount=" + baseEventCount + ", deviceVendor=" + deviceVendor
        + ", categoryBehavior=" + categoryBehavior + ", categoryOutcome=" + categoryOutcome
        + ", message=" + message + ", deviceProcessName=" + deviceProcessName + ", deviceProduct="
        + deviceProduct + ", destinationUserName=" + destinationUserName + ", deviceAddress="
        + deviceAddress + ", destinationAddress=" + destinationAddress + ", customerURI="
        + customerURI + ", sourceAddress=" + sourceAddress + ", sourceUserName=" + sourceUserName
        + ", name=" + name + ", deviceEventClassId=" + deviceEventClassId
        + ", applicationProtocol=" + applicationProtocol + ", destinationHostName="
        + destinationHostName + ", destinationPort=" + destinationPort + ", deviceAction="
        + deviceAction + ", deviceCustomString1=" + deviceCustomString1 + ", deviceCustomString2="
        + deviceCustomString2 + ", deviceCustomString3=" + deviceCustomString3
        + ", deviceCustomString4=" + deviceCustomString4 + ", deviceCustomString5="
        + deviceCustomString5 + ", deviceCustomString6=" + deviceCustomString6
        + ", deviceDirection=" + deviceDirection + ", deviceEventCategory=" + deviceEventCategory
        + ", deviceHostName=" + deviceHostName + ", deviceInboundInterface="
        + deviceInboundInterface + ", deviceOutboundInterface=" + deviceOutboundInterface
        + ", deviceReceiptTime=" + deviceReceiptTime + ", deviceSeverity=" + deviceSeverity
        + ", externalId=" + externalId + ", fileName=" + fileName + ", requestUrl=" + requestUrl
        + ", sourceHostName=" + sourceHostName + ", sourcePort=" + sourcePort
        + ", transportProtocol=" + transportProtocol + ", bytesIn=" + bytesIn + ", bytesOut="
        + bytesOut + ", requestContext=" + requestContext + ", requestClientApplication="
        + requestClientApplication + ", destinationProcessName=" + destinationProcessName
        + ", destinationServiceName=" + destinationServiceName + ", deviceCustomNumber1="
        + deviceCustomNumber1 + ", deviceCustomNumber2=" + deviceCustomNumber2
        + ", deviceCustomNumber3=" + deviceCustomNumber3 + ", endTime=" + endTime + ", eventId="
        + eventId + ", filePath=" + filePath + ", flexString1=" + flexString1 + ", reason="
        + reason + ", requestCookie=" + requestCookie + ", requestCookies=" + requestCookies
        + ", requestMethod=" + requestMethod + ", requestUrlFileName=" + requestUrlFileName
        + ", sourceNtDomain=" + sourceNtDomain + ", startTime=" + startTime + ", adrCustomStr1="
        + adrCustomStr1 + ", adrCustomStr2=" + adrCustomStr2 + ", adrCustomStr3=" + adrCustomStr3
        + ", adrCustomStr4=" + adrCustomStr4 + ", adrCustomeNumber1=" + adrCustomeNumber1
        + ", others=" + others + ", ruleIdActed=" + ruleIdActed + ", customerName=" + customerName
        + ", severity=" + severity + "]";
  }
}
