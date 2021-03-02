package net.paladion.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.model.MultiEventThresholdDTO;
import net.paladion.model.RuleMatchDTO;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

@Slf4j
public class UtilityTools implements Serializable {
  private static final long serialVersionUID = 1L;
//  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  /**
   * 
   * @param path
   * @return
   */
  public static Properties loadProperty(String path) {
    Properties prop = new Properties();
    InputStream input = null;

    try {
      input = new FileInputStream(path);
      prop.load(input);
    } catch (IOException ex) {
      log.error(ex.getMessage());
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          log.error(e.getMessage());
        }
      }
    }
    return prop;
  }

  /**
   * 
   * @param t1
   * @return
   */
  public static MultiEventThresholdDTO convertJsonToMultiEventThresholdDTO(String t1) {
	  MultiEventThresholdDTO multiEventThresholdDTO = new MultiEventThresholdDTO();

		String[] keyValuePairs = t1.split(",(?=(?:[^\"*\"},{\"*\"]))");
		for (String pair : keyValuePairs) {
			try {
				String[] entry = pair.split("=");
				if (entry[0].contains("topic")
						|| entry[0].contains("partition")
						|| entry[0].contains("offset")) {
					String val = "";
					if (entry.length > 1) {
						val = entry[1];
					}

					if (val != null) {
						val = val.trim();
					}

					String key = null;
					if (entry.length > 0) {
						key = entry[0];
					}

					if (key != null) {
						key = key.trim();
					}
					if (key.contains("topic")) {
						multiEventThresholdDTO.setTopic(val);
					} else if (key.contains("partition")) {
						multiEventThresholdDTO.setPartition(Integer
								.parseInt(val));
					} else if (key.contains("offset")) {
						multiEventThresholdDTO.setOffset(Long.parseLong(val));
					}
				}
			} catch (Exception e) {
				log.error("Error in parsing the values for key value pairs: " + e.getMessage());
			}
		}

    try {
      JSONObject jsonObj = new JSONObject(t1);
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      
      if (jsonObj.has("eventTime") && !StringUtils.isEmpty(jsonObj.getString("eventTime"))) {
        Date parsedDate = null;
        try {
          parsedDate = sdf.parse(jsonObj.getString("eventTime").trim());
        } catch (ParseException e) {
          log.error("ParseException while parsing the eventTime date: "
              + jsonObj.getString("eventTime"));
        } catch (NumberFormatException e) {
          log.error("NumberFormatException while parsing the eventTime date: "
              + jsonObj.getString("eventTime"));
        }

        if (parsedDate == null) {
          parsedDate = new Date();
        }

        multiEventThresholdDTO.setEt(new java.sql.Timestamp(parsedDate.getTime()));

        multiEventThresholdDTO.setEl(parsedDate.getTime());
        parsedDate = null;
      }

      if (jsonObj.has("threatId")) {
        multiEventThresholdDTO.setTi(jsonObj.getLong("threatId"));
      }

      if (jsonObj.has("applicationProtocol")) {
        multiEventThresholdDTO.setAa(jsonObj.getString("applicationProtocol"));
      }

      if (jsonObj.has("baseEventCount")) {
        multiEventThresholdDTO.setAb(jsonObj.getInt("baseEventCount"));
      }

      if (jsonObj.has("bytesIn")) {
        multiEventThresholdDTO.setAc(jsonObj.getInt("bytesIn"));
      }

      if (jsonObj.has("bytesOut")) {
        multiEventThresholdDTO.setAd(jsonObj.getInt("bytesOut"));
      }

      if (jsonObj.has("destinationAddress")) {
        multiEventThresholdDTO.setAe(jsonObj.getString("destinationAddress"));
      }

      if (jsonObj.has("destinationHostName")) {
        multiEventThresholdDTO.setAf(jsonObj.getString("destinationHostName"));
      }

      if (jsonObj.has("destinationPort")) {
        multiEventThresholdDTO.setAg(jsonObj.getInt("destinationPort"));
      }

      if (jsonObj.has("destinationProcessName")) {
        multiEventThresholdDTO.setAh(jsonObj.getString("destinationProcessName"));
      }

      if (jsonObj.has("destinationServiceName")) {
        multiEventThresholdDTO.setAi(jsonObj.getString("destinationServiceName"));
      }

      if (jsonObj.has("destinationUserName")) {
        multiEventThresholdDTO.setAj(jsonObj.getString("destinationUserName"));
      }

      if (jsonObj.has("deviceAction")) {
        multiEventThresholdDTO.setAk(jsonObj.getString("deviceAction"));
      }

      if (jsonObj.has("deviceAddress")) {
        multiEventThresholdDTO.setAl(jsonObj.getString("deviceAddress"));
      }

      if (jsonObj.has("deviceCustomNumber1")) {
        multiEventThresholdDTO.setAm(jsonObj.getLong("deviceCustomNumber1"));
      }

      if (jsonObj.has("deviceCustomNumber2")) {
        multiEventThresholdDTO.setAn(jsonObj.getLong("deviceCustomNumber2"));
      }

      if (jsonObj.has("deviceCustomNumber3")) {
        multiEventThresholdDTO.setAo(jsonObj.getLong("deviceCustomNumber3"));
      }

      if (jsonObj.has("deviceCustomString1")) {
        multiEventThresholdDTO.setAp(jsonObj.getString("deviceCustomString1"));
      }

      if (jsonObj.has("deviceCustomString2")) {
        multiEventThresholdDTO.setAq(jsonObj.getString("deviceCustomString2"));
      }

      if (jsonObj.has("deviceCustomString3")) {
        multiEventThresholdDTO.setAr(jsonObj.getString("deviceCustomString3"));
      }

      if (jsonObj.has("deviceCustomString4")) {
        multiEventThresholdDTO.setDa(jsonObj.getString("deviceCustomString4"));
      }

      if (jsonObj.has("deviceCustomString5")) {
        multiEventThresholdDTO.setAt(jsonObj.getString("deviceCustomString5"));
      }

      if (jsonObj.has("deviceCustomString6")) {
        multiEventThresholdDTO.setAu(jsonObj.getString("deviceCustomString6"));
      }

      if (jsonObj.has("deviceDirection")) {
        multiEventThresholdDTO.setAv(jsonObj.getString("deviceDirection"));
      }

      if (jsonObj.has("deviceEventCategory")) {
        multiEventThresholdDTO.setAw(jsonObj.getString("deviceEventCategory"));
      }

      if (jsonObj.has("deviceEventClassId")) {
        multiEventThresholdDTO.setAx(jsonObj.getString("deviceEventClassId"));
      }

      if (jsonObj.has("deviceHostName")) {
        multiEventThresholdDTO.setAy(jsonObj.getString("deviceHostName"));
      }

      if (jsonObj.has("deviceInboundInterface")) {
        multiEventThresholdDTO.setAz(jsonObj.getString("deviceInboundInterface"));
      }

      if (jsonObj.has("deviceOutboundInterface")) {
        multiEventThresholdDTO.setBa(jsonObj.getString("deviceOutboundInterface"));
      }

      if (jsonObj.has("deviceProcessName")) {
        multiEventThresholdDTO.setBb(jsonObj.getString("deviceProcessName"));
      }

      if (jsonObj.has("deviceProduct")) {
        multiEventThresholdDTO.setBc(jsonObj.getString("deviceProduct"));
      }

      if (jsonObj.has("deviceReceiptTime")
          && !StringUtils.isEmpty(jsonObj.getString("deviceReceiptTime"))) {
        Date parsedDate = null;
        try {
          parsedDate = sdf.parse(jsonObj.getString("deviceReceiptTime").trim());
          multiEventThresholdDTO.setBd(new java.sql.Timestamp(parsedDate.getTime()));
        } catch (ParseException e) {
          log.error("ParseException while parsing the deviceReceiptTime date: "
              + jsonObj.getString("deviceReceiptTime"));
        } catch (NumberFormatException e) {
          log.error("NumberFormatException while parsing the deviceReceiptTime date: "
              + jsonObj.getString("deviceReceiptTime"));
        }
        parsedDate = null;
      }

      if (jsonObj.has("deviceSeverity")) {
        multiEventThresholdDTO.setBe(jsonObj.getString("deviceSeverity"));
      }

      if (jsonObj.has("deviceVendor")) {
        multiEventThresholdDTO.setBf(jsonObj.getString("deviceVendor"));
      }

      if (jsonObj.has("endTime") && !StringUtils.isEmpty(jsonObj.getString("endTime"))) {
        Date parsedDate = null;
        try {
          parsedDate = sdf.parse(jsonObj.getString("endTime").trim());
          multiEventThresholdDTO.setBg(new java.sql.Timestamp(parsedDate.getTime()));
        } catch (ParseException e) {
          log.error("ParseException while parsing the endTime date: "
              + jsonObj.getString("endTime"));
        } catch (NumberFormatException e) {
          log.error("NumberFormatException while parsing the endTime date: "
              + jsonObj.getString("endTime"));
        }
        parsedDate = null;
      }

      if (jsonObj.has("eventId")) {
        multiEventThresholdDTO.setBh(jsonObj.getLong("eventId"));
      }

      if (jsonObj.has("externalId")) {
        multiEventThresholdDTO.setBi(jsonObj.getString("externalId"));
      }

      if (jsonObj.has("fileName")) {
        multiEventThresholdDTO.setBj(jsonObj.getString("fileName"));
      }

      if (jsonObj.has("filePath")) {
        multiEventThresholdDTO.setBk(jsonObj.getString("filePath"));
      }

      if (jsonObj.has("flexString1")) {
        multiEventThresholdDTO.setBl(jsonObj.getString("flexString1"));
      }

      if (jsonObj.has("message")) {
        multiEventThresholdDTO.setBm(jsonObj.getString("message"));
      }

      if (jsonObj.has("name")) {
        multiEventThresholdDTO.setBn(jsonObj.getString("name"));
      }

      if (jsonObj.has("reason")) {
        multiEventThresholdDTO.setBo(jsonObj.getString("reason"));
      }

      if (jsonObj.has("requestClientApplication")) {
        multiEventThresholdDTO.setBp(jsonObj.getString("requestClientApplication"));
      }

      if (jsonObj.has("requestContext")) {
        multiEventThresholdDTO.setBq(jsonObj.getString("requestContext"));
      }

      if (jsonObj.has("requestCookie")) {
        multiEventThresholdDTO.setBr(jsonObj.getString("requestCookie"));
      }

      if (jsonObj.has("requestCookies")) {
        multiEventThresholdDTO.setBs(jsonObj.getString("requestCookies"));
      }

      if (jsonObj.has("requestMethod")) {
        multiEventThresholdDTO.setBt(jsonObj.getString("requestMethod"));
      }

      if (jsonObj.has("requestUrl")) {
        multiEventThresholdDTO.setBu(jsonObj.getString("requestUrl"));
      }

      if (jsonObj.has("requestUrlFileName")) {
        multiEventThresholdDTO.setBv(jsonObj.getString("requestUrlFileName"));
      }

      if (jsonObj.has("sourceAddress")) {
        multiEventThresholdDTO.setBw(jsonObj.getString("sourceAddress"));
      }

      if (jsonObj.has("sourceHostName")) {
        multiEventThresholdDTO.setBx(jsonObj.getString("sourceHostName"));
      }

      if (jsonObj.has("sourceNtDomain")) {
        multiEventThresholdDTO.setDb(jsonObj.getString("sourceNtDomain"));
      }

      if (jsonObj.has("sourcePort")) {
        multiEventThresholdDTO.setDc(jsonObj.getInt("sourcePort"));
      }

      if (jsonObj.has("sourceUserName")) {
        multiEventThresholdDTO.setBz(jsonObj.getString("sourceUserName"));
      }

      if (jsonObj.has("startTime") && !StringUtils.isEmpty(jsonObj.getString("startTime"))) {
        Date parsedDate = null;
        try {
          parsedDate = sdf.parse(jsonObj.getString("startTime").trim());
          multiEventThresholdDTO.setCa(new java.sql.Timestamp(parsedDate.getTime()));
        } catch (ParseException e) {
          log.error("ParseException while parsing the startTime date: "
              + jsonObj.getString("startTime"));
        } catch (NumberFormatException e) {
          log.error("NumberFormatException while parsing the startTime date: "
              + jsonObj.getString("startTime"));
        }
        parsedDate = null;
      }

      if (jsonObj.has("transportProtocol")) {
        multiEventThresholdDTO.setCb(jsonObj.getString("transportProtocol"));
      }

      if (jsonObj.has("categoryBehavior")) {
        multiEventThresholdDTO.setCg(jsonObj.getString("categoryBehavior"));
      }

      if (jsonObj.has("categoryOutcome")) {
        multiEventThresholdDTO.setCh(jsonObj.getString("categoryOutcome"));
      }

      if (jsonObj.has("customerURI")) {
        multiEventThresholdDTO.setCi(jsonObj.getString("customerURI"));
      }

      if (jsonObj.has("adrCustomStr1")) {
        multiEventThresholdDTO.setCj(jsonObj.getString("adrCustomStr1"));
      }

      if (jsonObj.has("adrCustomStr2")) {
        multiEventThresholdDTO.setCk(jsonObj.getString("adrCustomStr2"));
      }

      if (jsonObj.has("adrCustomStr3")) {
        multiEventThresholdDTO.setCl(jsonObj.getString("adrCustomStr3"));
      }

      if (jsonObj.has("adrCustomStr4")) {
        multiEventThresholdDTO.setCm(jsonObj.getString("adrCustomStr4"));
      }

      if (jsonObj.has("adrCustomeNumber1")) {
        multiEventThresholdDTO.setCn(jsonObj.getInt("adrCustomeNumber1"));
      }

      if (jsonObj.has("customerName")) {
        multiEventThresholdDTO.setCu(jsonObj.getString("customerName"));
      }

      if (jsonObj.has("severity")) {
        multiEventThresholdDTO.setSe(jsonObj.getString("severity"));
      }

      if (jsonObj.has("recommendations")) {
        multiEventThresholdDTO.setRe(jsonObj.getString("recommendations"));
      }

      if (jsonObj.has("clientTopic")) {
        multiEventThresholdDTO.setCt(jsonObj.getString("clientTopic"));
      }

      if (jsonObj.has("sourceZone")) {
        multiEventThresholdDTO.setSz(jsonObj.getString("sourceZone"));
      }

      if (jsonObj.has("destinationZone")) {
        multiEventThresholdDTO.setDz(jsonObj.getString("destinationZone"));
      }

      if (jsonObj.has("sourceGeoCityName")) {
        multiEventThresholdDTO.setCp(jsonObj.getString("sourceGeoCityName"));
      }

      if (jsonObj.has("sourceGeoCountryName")) {
        multiEventThresholdDTO.setCq(jsonObj.getString("sourceGeoCountryName"));
      }

      if (jsonObj.has("destinationGeoCityName")) {
        multiEventThresholdDTO.setCr(jsonObj.getString("destinationGeoCityName"));
      }

      if (jsonObj.has("destinationGeoCountryName")) {
        multiEventThresholdDTO.setCs(jsonObj.getString("destinationGeoCountryName"));
      }

      if (jsonObj.has("customer")) {
        multiEventThresholdDTO.setCt(jsonObj.getString("customer"));
      }

      if (jsonObj.has("others")) {
        multiEventThresholdDTO.setCo(jsonObj.getString("others"));
      }

      JSONObject ruleMatchDTOJson = jsonObj.getJSONObject("ruleMatchDTO");

      if (ruleMatchDTOJson.has("ruleId")) {
        multiEventThresholdDTO.setMr(ruleMatchDTOJson.getInt("ruleId"));
      }

      if (ruleMatchDTOJson.has("identicalColStr")) {
        multiEventThresholdDTO.setRi(ruleMatchDTOJson.getString("identicalColStr"));
      }

      if (ruleMatchDTOJson.has("distinctColStr")) {
        multiEventThresholdDTO.setRd(ruleMatchDTOJson.getString("distinctColStr"));
      }

      if (ruleMatchDTOJson.has("eventAThresholdValue")) {
        multiEventThresholdDTO.setRt(ruleMatchDTOJson.getInt("eventAThresholdValue"));
      }

      if (ruleMatchDTOJson.has("timeWindowUnit")) {
        multiEventThresholdDTO.setRu(ruleMatchDTOJson.getString("timeWindowUnit"));
      }

      if (ruleMatchDTOJson.has("timeWindowValue")) {
        multiEventThresholdDTO.setRv(ruleMatchDTOJson.getInt("timeWindowValue"));
      }

      if (ruleMatchDTOJson.has("ruleName")) {
        multiEventThresholdDTO.setRn(ruleMatchDTOJson.getString("ruleName"));
      }

      if (ruleMatchDTOJson.has("follwedByRuleName")) {
        multiEventThresholdDTO.setRf(ruleMatchDTOJson.getString("follwedByRuleName"));
      }

      if (ruleMatchDTOJson.has("severity")) {
        multiEventThresholdDTO.setSe(ruleMatchDTOJson.getString("severity"));
      }

      if (ruleMatchDTOJson.has("recommendations")) {
        multiEventThresholdDTO.setRre(ruleMatchDTOJson.getString("recommendations"));
      }

      if (ruleMatchDTOJson.has("follwedByRuleName")) {
        multiEventThresholdDTO.setRf(ruleMatchDTOJson.getString("follwedByRuleName"));
      }

      ruleMatchDTOJson = null;
      jsonObj = null;

      java.util.Date date = new java.util.Date();
      Timestamp eventTime = new Timestamp(date.getTime());
      multiEventThresholdDTO.setSd(eventTime);
      date = null;
      eventTime = null;
      
      sdf = null;
    } catch (JSONException e) {
      log.error("Error while parsing the Json String: " + t1 + " Error is: " + e.getMessage());
    }
    return multiEventThresholdDTO;
  }

  public static MultiEventCoreDTO convertToMultiEventCoreDTO(
      MultiEventThresholdDTO multiEventThresholdDTO) {
    MultiEventCoreDTO multiEventCoreDTO = new MultiEventCoreDTO();

    try {
      multiEventCoreDTO.setEventTime(multiEventThresholdDTO.getEt());
      multiEventCoreDTO.setThreatId(multiEventThresholdDTO.getTi());
      multiEventCoreDTO.setApplicationProtocol(multiEventThresholdDTO.getAa());
      multiEventCoreDTO.setBaseEventCount(Integer.parseInt(String.valueOf(multiEventThresholdDTO
          .getCount())));
      multiEventCoreDTO.setBytesIn(multiEventThresholdDTO.getAc());
      multiEventCoreDTO.setBytesOut(multiEventThresholdDTO.getAd());
      multiEventCoreDTO.setDestinationAddress(multiEventThresholdDTO.getAe());
      multiEventCoreDTO.setDestinationHostName(multiEventThresholdDTO.getAf());
      multiEventCoreDTO.setDestinationPort(multiEventThresholdDTO.getAg());
      multiEventCoreDTO.setDestinationProcessName(multiEventThresholdDTO.getAh());
      multiEventCoreDTO.setDestinationServiceName(multiEventThresholdDTO.getAi());
      multiEventCoreDTO.setDestinationUserName(multiEventThresholdDTO.getAj());
      multiEventCoreDTO.setDeviceAction(multiEventThresholdDTO.getAk());
      multiEventCoreDTO.setDeviceAddress(multiEventThresholdDTO.getAl());
      multiEventCoreDTO.setDeviceCustomNumber1(multiEventThresholdDTO.getAm());
      multiEventCoreDTO.setDeviceCustomNumber2(multiEventThresholdDTO.getAn());
      multiEventCoreDTO.setDeviceCustomNumber3(multiEventThresholdDTO.getAo());
      multiEventCoreDTO.setDeviceCustomString1(multiEventThresholdDTO.getAp());
      multiEventCoreDTO.setDeviceCustomString2(multiEventThresholdDTO.getAq());
      multiEventCoreDTO.setDeviceCustomString3(multiEventThresholdDTO.getAr());
      multiEventCoreDTO.setDeviceCustomString4(multiEventThresholdDTO.getDa());
      multiEventCoreDTO.setDeviceCustomString5(multiEventThresholdDTO.getAt());
      multiEventCoreDTO.setDeviceCustomString6(multiEventThresholdDTO.getAu());
      multiEventCoreDTO.setDeviceDirection(multiEventThresholdDTO.getAv());
      multiEventCoreDTO.setDeviceEventCategory(multiEventThresholdDTO.getAw());
      multiEventCoreDTO.setDeviceEventClassId(multiEventThresholdDTO.getAx());
      multiEventCoreDTO.setDeviceHostName(multiEventThresholdDTO.getAy());
      multiEventCoreDTO.setDeviceInboundInterface(multiEventThresholdDTO.getAz());
      multiEventCoreDTO.setDeviceOutboundInterface(multiEventThresholdDTO.getBa());
      multiEventCoreDTO.setDeviceProcessName(multiEventThresholdDTO.getBb());
      multiEventCoreDTO.setDeviceProduct(multiEventThresholdDTO.getBc());
      multiEventCoreDTO.setDeviceReceiptTime(multiEventThresholdDTO.getBd());
      multiEventCoreDTO.setDeviceSeverity(multiEventThresholdDTO.getBe());
      multiEventCoreDTO.setDeviceVendor(multiEventThresholdDTO.getBf());
      multiEventCoreDTO.setEndTime(multiEventThresholdDTO.getBg());
      multiEventCoreDTO.setEventId(multiEventThresholdDTO.getBh());
      multiEventCoreDTO.setExternalId(multiEventThresholdDTO.getBi());
      multiEventCoreDTO.setFileName(multiEventThresholdDTO.getBj());
      multiEventCoreDTO.setFilePath(multiEventThresholdDTO.getBk());
      multiEventCoreDTO.setFlexString1(multiEventThresholdDTO.getBl());
      multiEventCoreDTO.setMessage(multiEventThresholdDTO.getBm());
      multiEventCoreDTO.setName(multiEventThresholdDTO.getBn());
      multiEventCoreDTO.setReason(multiEventThresholdDTO.getBo());
      multiEventCoreDTO.setRequestClientApplication(multiEventThresholdDTO.getBp());
      multiEventCoreDTO.setRequestContext(multiEventThresholdDTO.getBq());
      multiEventCoreDTO.setRequestCookie(multiEventThresholdDTO.getBr());
      multiEventCoreDTO.setRequestCookies(multiEventThresholdDTO.getBs());
      multiEventCoreDTO.setRequestMethod(multiEventThresholdDTO.getBt());
      multiEventCoreDTO.setRequestUrl(multiEventThresholdDTO.getBu());
      multiEventCoreDTO.setRequestUrlFileName(multiEventThresholdDTO.getBv());
      multiEventCoreDTO.setSourceAddress(multiEventThresholdDTO.getBw());
      multiEventCoreDTO.setSourceHostName(multiEventThresholdDTO.getBx());
      multiEventCoreDTO.setSourceNtDomain(multiEventThresholdDTO.getDb());
      multiEventCoreDTO.setSourcePort(multiEventThresholdDTO.getDc());
      multiEventCoreDTO.setSourceUserName(multiEventThresholdDTO.getBz());
      multiEventCoreDTO.setStartTime(multiEventThresholdDTO.getCa());
      multiEventCoreDTO.setTransportProtocol(multiEventThresholdDTO.getCb());
      multiEventCoreDTO.setCategoryBehavior(multiEventThresholdDTO.getCg());
      multiEventCoreDTO.setCategoryOutcome(multiEventThresholdDTO.getCh());
      multiEventCoreDTO.setCustomerURI(multiEventThresholdDTO.getCi());
      multiEventCoreDTO.setAdrCustomStr1(multiEventThresholdDTO.getCj());
      multiEventCoreDTO.setAdrCustomStr2(multiEventThresholdDTO.getCk());
      multiEventCoreDTO.setAdrCustomStr3(multiEventThresholdDTO.getCl());
      multiEventCoreDTO.setAdrCustomStr4(multiEventThresholdDTO.getCm());
      multiEventCoreDTO.setAdrCustomeNumber1(multiEventThresholdDTO.getCn());
      multiEventCoreDTO.setCustomerName(multiEventThresholdDTO.getCu());
      multiEventCoreDTO.setSeverity(multiEventThresholdDTO.getSe());
      multiEventCoreDTO.setRecommendations(multiEventThresholdDTO.getRe());
      multiEventCoreDTO.setClientTopic(multiEventThresholdDTO.getCt());
      multiEventCoreDTO.setOthers(multiEventThresholdDTO.getCo());
      multiEventCoreDTO.setSourceZone(multiEventThresholdDTO.getSz());
      multiEventCoreDTO.setDestinationZone(multiEventThresholdDTO.getDz());
      multiEventCoreDTO.setSourceGeoCityName(multiEventThresholdDTO.getCp());
      multiEventCoreDTO.setSourceGeoCountryName(multiEventThresholdDTO.getCq());
      multiEventCoreDTO.setDestinationGeoCityName(multiEventThresholdDTO.getCr());
      multiEventCoreDTO.setDestinationGeoCountryName(multiEventThresholdDTO.getCs());
      multiEventCoreDTO.setCustomer(multiEventThresholdDTO.getCt());
      multiEventCoreDTO.setMatchedRuleId(multiEventThresholdDTO.getMr());
      multiEventCoreDTO.setRuleThesholdApplicable("yes");

      RuleMatchDTO ruleMatchDTO = new RuleMatchDTO();
      ruleMatchDTO.setRuleId(multiEventThresholdDTO.getMr());
      ruleMatchDTO.setIdenticalColStr(multiEventThresholdDTO.getRi());
      ruleMatchDTO.setDistinctColStr(multiEventThresholdDTO.getRd());
      ruleMatchDTO.setEventAThresholdValue(multiEventThresholdDTO.getRt());
      ruleMatchDTO.setTimeWindowUnit(multiEventThresholdDTO.getRu());
      ruleMatchDTO.setTimeWindowValue(multiEventThresholdDTO.getRv());
      ruleMatchDTO.setRuleName(multiEventThresholdDTO.getRn());
      ruleMatchDTO.setSeverity(multiEventThresholdDTO.getSe());
      ruleMatchDTO.setRecommendations(multiEventThresholdDTO.getRre());
      ruleMatchDTO.setFollwedByRuleName(multiEventThresholdDTO.getRf());

      ruleMatchDTO.setChildIdList(multiEventThresholdDTO.getChl());

      multiEventCoreDTO.setRuleMatchDTO(ruleMatchDTO);
    } catch (Exception e) {
      log.error("Exception while creating multiEventCoreDTO in UtilityTools: " + e.getMessage()
          + " multiEventThresholdDTO is: " + multiEventThresholdDTO);
    }
    return multiEventCoreDTO;
  }
}
