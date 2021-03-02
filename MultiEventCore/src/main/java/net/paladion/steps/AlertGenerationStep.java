/**
 * 
 */
package net.paladion.steps;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.KafkaProducerSingleton;
import net.paladion.model.MultiEventCoreDTO;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.paladion.racommon.data.DataCarrier;

/**
 * @author ankush
 *
 */
@Slf4j
public class AlertGenerationStep extends
    SparkStep<DataCarrier<JavaRDD<MultiEventCoreDTO>>, DataCarrier<JavaRDD<MultiEventCoreDTO>>> {

  private static final long serialVersionUID = 1L;
  private final Properties clientProp;

  public AlertGenerationStep(Properties clientProp1) {
    super();
    clientProp = clientProp1;
  }

  static final Gson gson = new GsonBuilder().disableHtmlEscaping()
      .setDateFormat("yyyy-MM-dd HH:mm:ss.SSS").setExclusionStrategies(new ExclusionStrategy() {
        @Override
        public boolean shouldSkipField(FieldAttributes f) {
          if (f.getName().equalsIgnoreCase("ruleMatchDTOList")
              || f.getName().equalsIgnoreCase("sourceAddressLong")
              || f.getName().equalsIgnoreCase("destinationAddressLong")
              || f.getName().equalsIgnoreCase("ruleMatchIdList")
              || f.getName().equalsIgnoreCase("customerId")
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

  static final Gson gsonBatch = new GsonBuilder().disableHtmlEscaping()
      .setDateFormat("yyyy-MM-dd HH:mm:ss.SSS").setExclusionStrategies(new ExclusionStrategy() {
        @Override
        public boolean shouldSkipField(FieldAttributes f) {
          if (f.getName().equalsIgnoreCase("ruleMatchDTOList")
              || f.getName().equalsIgnoreCase("sourceAddressLong")
              || f.getName().equalsIgnoreCase("destinationAddressLong")
              || f.getName().equalsIgnoreCase("ruleMatchIdList")
              || f.getName().equalsIgnoreCase("customerId")
              || f.getName().equalsIgnoreCase("dataArray")) {
            return true;
          }
          return false;
        }

        @Override
        public boolean shouldSkipClass(Class<?> arg0) {
          return false;
        }
      }).create();

  @Override
  public DataCarrier<JavaRDD<MultiEventCoreDTO>> customTransform(
      DataCarrier<JavaRDD<MultiEventCoreDTO>> dataCarrier) {
    JavaRDD<MultiEventCoreDTO> dtoStream = dataCarrier.getPayload();

    JavaPairRDD<String, String> test =
        dtoStream
            .mapPartitionsToPair(new PairFlatMapFunction<Iterator<MultiEventCoreDTO>, String, String>() {
              private static final long serialVersionUID = 1L;

              @Override
              public Iterator<Tuple2<String, String>> call(Iterator<MultiEventCoreDTO> arg0)
                  throws Exception {
                ArrayList<Tuple2<String, String>> arr = new ArrayList<Tuple2<String, String>>();
                while (arg0.hasNext()) {
                  String jsonInString = "";
                  MultiEventCoreDTO multiEventCoreDTO = arg0.next();
                  multiEventCoreDTO.setMatchedRuleName(multiEventCoreDTO.getRuleMatchDTO()
                      .getRuleName());
                  multiEventCoreDTO.setMatchedRuleId(multiEventCoreDTO.getRuleMatchDTO()
                      .getRuleId());
                  multiEventCoreDTO.setRuleThesholdApplicable(multiEventCoreDTO.getRuleMatchDTO()
                      .getThresholdApplicable());

                  if (!StringUtils.isEmpty(multiEventCoreDTO.getRuleMatchDTO().getSeverity())
                      && !multiEventCoreDTO.getRuleMatchDTO().getSeverity().equalsIgnoreCase("na")
                      && !multiEventCoreDTO.getRuleMatchDTO().getSeverity()
                          .equalsIgnoreCase("null")) {
                    multiEventCoreDTO
                        .setSeverity(multiEventCoreDTO.getRuleMatchDTO().getSeverity());
                  }
                  if (!StringUtils
                      .isEmpty(multiEventCoreDTO.getRuleMatchDTO().getRecommendations())
                      && !multiEventCoreDTO.getRuleMatchDTO().getRecommendations()
                          .equalsIgnoreCase("na")
                      && !multiEventCoreDTO.getRuleMatchDTO().getRecommendations()
                          .equalsIgnoreCase("null")) {
                    multiEventCoreDTO.setRecommendations(multiEventCoreDTO.getRuleMatchDTO()
                        .getRecommendations());
                  }

                  if (multiEventCoreDTO.getRuleMatchDTO().getRuleType()
                      .equalsIgnoreCase(clientProp.getProperty("batchRuleName"))) {
                    jsonInString = gsonBatch.toJson(multiEventCoreDTO);

                    // System.out.println(clientProp.getProperty("batchRuleTopicName") + ": "
                    // + jsonInString);
                    arr.add(new Tuple2<String, String>(
                        clientProp.getProperty("batchRuleTopicName"), jsonInString));
                  } else {
                    jsonInString = gson.toJson(multiEventCoreDTO);
                    // System.out.println(multiEventCoreDTO.getClientTopic() + ": " + jsonInString);
                    arr.add(new Tuple2<String, String>(multiEventCoreDTO.getClientTopic(),
                        jsonInString));
                  }
                }
                return arr.iterator();
              }
            });

    VoidFunction<Tuple2<String, String>> tuple = new VoidFunction<Tuple2<String, String>>() {
      private static final long serialVersionUID = 1L;

      @Override
      public void call(Tuple2<String, String> arg0) throws Exception {
        try {
          KafkaProducerSingleton kfh = KafkaProducerSingleton.getInstance(clientProp);
          if (arg0._1() != null) {
            kfh.send1(arg0._1, arg0._2);
          }
        } catch (Exception e) {
          log.error("Error while generating alert for kafka: " + e.getMessage());
        }
      }
    };
    test.foreach(tuple);
    return null;
  }
}
