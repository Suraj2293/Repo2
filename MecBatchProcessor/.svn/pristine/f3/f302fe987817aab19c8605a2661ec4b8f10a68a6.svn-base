package net.paladion.model;

import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Slf4j
public class KafkaProducerImpl {

  static final Gson gson = new GsonBuilder().disableHtmlEscaping()
      .setDateFormat("yyyy-MM-dd HH:mm:ss.SSS").setExclusionStrategies(new ExclusionStrategy() {
        @Override
        public boolean shouldSkipField(FieldAttributes f) {
          if (f.getName().equalsIgnoreCase("ruleMatchDTOList")
              || f.getName().equalsIgnoreCase("sourceAddressLong")
              || f.getName().equalsIgnoreCase("destinationAddressLong")
              || f.getName().equalsIgnoreCase("ruleMatchIdList")
              || f.getName().equalsIgnoreCase("dataArray")
              || f.getName().equalsIgnoreCase("customer")) {
            return true;
          }
          return false;
        }

        @Override
        public boolean shouldSkipClass(Class<?> arg0) {
          return false;
        }
      }).create();

  public static void sendToKafka(Properties clientProp, MultiEventCoreDTO dto) {
    String jsonInString = "";
    dto.setMatchedRuleName(dto.getRuleMatchDTO().getRuleName());

    try {
      jsonInString = gson.toJson(dto);
      // System.out.println(dto.getClientTopic() + ": " + jsonInString);

      KafkaProducerSingleton kfh = KafkaProducerSingleton.getInstance(clientProp);

      if (dto.getClientTopic() != null) {
        kfh.send1(dto.getClientTopic(), jsonInString);
      } else {
        kfh.send1(clientProp.getProperty("kafka.send.topic"), jsonInString);
      }
    } catch (Exception e) {
      log.error("Error occured while generating the kafka alert: " + e.getMessage());
    }
  }
}
