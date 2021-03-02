/**
 * 
 */
package net.paladion.rule;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.RuleMatchDTO;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.drools.template.ObjectDataCompiler;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.builder.ReleaseId;
import org.kie.api.builder.Results;

/**
 * @author ankush
 *
 */
@Slf4j
public class AddRule implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy hh:mm");

  /**
   * @param args
   */
  public static void addRule(Properties clientProp) {
    String staticDrlFileStr =
        KieSessionService.readFile(clientProp.getProperty("drlFileTemplatePath"));
    try {
      List<RuleMatchDTO> rulesList = getNewRulesList(clientProp);
      String drlStr = getRulesDRLString(clientProp, rulesList);
      drlStr = staticDrlFileStr + drlStr;

      KieServices kieServices = KieServices.Factory.get();

      ReleaseId releaseId1 = kieServices.newReleaseId("com.paladion.add", "kie-upgrade", "1");
      KieFileSystem kfs1 = kieServices.newKieFileSystem();
      kfs1.generateAndWritePomXML(releaseId1);

      kfs1.write("src/main/resources/addrulematch.drl", drlStr);

      KieBuilder kieBuilder = kieServices.newKieBuilder(kfs1).buildAll();

      String rulesStr = "";
      Results results = kieBuilder.getResults();
      if (results.hasMessages(Message.Level.ERROR)) {
        log.error(results.getMessages().toString());
        throw new IllegalStateException("### errors ###");
      } else {
        FileInputStream rulesStream = null;
        try {
          rulesStream = new FileInputStream(new File(clientProp.getProperty("newdrlRuleCsvPath")));

          rulesStr = IOUtils.toString(rulesStream, "UTF-8");
        } catch (IOException e) {
          log.error("Exception: " + e);
          throw e;
        } finally {
          rulesStream.close();
        }

        if (rulesStr != null && !StringUtils.isEmpty(rulesStr.trim())) {
          FileWriter pw = null;
          FileWriter fw = null;
          PrintWriter pw1 = null;
          try {
            pw = new FileWriter(clientProp.getProperty("activeRulesCsvPath"), true);
            pw.append(rulesStr);

            fw = new FileWriter(clientProp.getProperty("newdrlRuleCsvPath"), false);
            pw1 = new PrintWriter(fw, false);
          } catch (IOException e) {
            log.error("Exception: " + e);
            throw e;
          } finally {
            if (pw != null) {
              pw.close();
            }
            if (fw != null) {
              fw.close();
            }
            if (fw != null) {
              try {
                pw1.flush();
                pw1.close();
              } catch (Exception e) {
                log.error(e.getMessage());
              }
            }
          }
        }
      }

      log.info("Rule Added" + rulesList);
      rulesStr = null;
      rulesList = null;
    } catch (Exception e) {
      log.error("Exception: " + e);
    }
  }

  /**
   * 
   * @param clientProp
   * @return
   * @throws IOException
   * @throws ArrayIndexOutOfBoundsException
   * @throws ParseException
   */
  private static List<RuleMatchDTO> getNewRulesList(Properties clientProp) throws IOException,
      ArrayIndexOutOfBoundsException, ParseException {
    List<RuleMatchDTO> rulesList = new ArrayList<RuleMatchDTO>();

    FileInputStream rulesStream = null;
    try {
      rulesStream = new FileInputStream(new File(clientProp.getProperty("newdrlRuleCsvPath")));

      String rulesStr = IOUtils.toString(rulesStream, "UTF-8");

      if (!StringUtils.isEmpty(rulesStr)) {
        String[] rulesStrArr = rulesStr.split("\n");

        for (String rule : rulesStrArr) {
          if (!StringUtils.isEmpty(rule)) {
            RuleMatchDTO ruleMatchDTO = new RuleMatchDTO();

            String[] ruleArr = rule.split("~#~");
            Integer ruleId = 0;
            if (ruleArr != null) {
              try {
                ruleId = Integer.parseInt(ruleArr[0].trim());
                ruleMatchDTO.setRuleId(ruleId);
                ruleMatchDTO.setRuleType(ruleArr[1].trim());
                ruleMatchDTO.setRuleType(ruleArr[2].trim());
                ruleMatchDTO.setRuleName(ruleArr[3].trim());

                String ruleCondition = "";

                if (!StringUtils.isEmpty(ruleArr[4])) {
                  ruleCondition =
                      ruleArr[4]
                          .replaceAll(" InCSVActiveList", " RuleCustomCondition.InCSVActiveList")
                          .replaceAll(" NotInCSVActiveList",
                              " RuleCustomCondition.NotInCSVActiveList")
                          .replaceAll(" InActiveList\\(",
                              " InActiveConditions.InActiveList\\(\\$m,Connection,")
                          .replaceAll(" NotInActiveList\\(",
                              " InActiveConditions.NotInActiveList\\(\\$m,Connection,");

                }

                ruleMatchDTO.setRuleCondition(ruleCondition.trim());

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
                  ruleMatchDTO.setRulePriority(Integer.parseInt(ruleArr[9]));
                } else {
                  ruleMatchDTO.setRulePriority(0);
                }

                if (ruleArr.length > 10 && !StringUtils.isEmpty(ruleArr[10])) {
                  ruleMatchDTO.setThresholdCondition(ruleArr[9].trim());
                } else {
                  ruleMatchDTO.setThresholdCondition("NA");
                }

                if (ruleArr.length > 11 && !StringUtils.isEmpty(ruleArr[11])) {
                  try {
                    Date date = formatter.parse(ruleArr[11].trim());
                    ruleMatchDTO.setExpiryDate(date);
                  } catch (ParseException e) {
                    log.error("Rule expiry date is not in currect format RuleName " + ruleArr[3]
                        + " Exception is: " + e.getMessage());
                    throw e;
                  }
                } else {
                  ruleMatchDTO.setExpiryDate(null);
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
                  ruleMatchDTO.setFollwedByRuleName(ruleArr[15].trim());
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

                rulesList.add(ruleMatchDTO);
                ruleId++;
              } catch (ArrayIndexOutOfBoundsException e) {
                log.error("Exception: CSV format is not currect for line no. " + ruleId
                    + " error is : " + e);
                throw e;
              }
            }
            ruleArr = null;
          }
          rule = null;
        }
        rulesStrArr = null;
      }
      rulesStr = null;
    } catch (IOException e) {
      log.error("Exception: CSV format is not currect: " + e);
      throw e;
    } finally {
      rulesStream.close();
    }
    return rulesList;
  }

  /**
   * 
   * @param clientProp
   * @param rulesList
   * @return
   */
  public static String getRulesDRLString(Properties clientProp, List<RuleMatchDTO> rulesList) {
    String drlStr = "";
    InputStream templateStream = null;

    if (rulesList != null && !rulesList.isEmpty()) {
      try {
        templateStream = new FileInputStream(clientProp.getProperty("drtTemplatePath"));
        ObjectDataCompiler converter = new ObjectDataCompiler();
        drlStr = converter.compile(rulesList, templateStream);
      } catch (Exception e) {
        log.error("Exception in getRulesDRLString method converter: " + e);
      } finally {
        try {
          templateStream.close();
        } catch (IOException e) {
          log.error(e.getMessage());
        }
      }
    }

    return drlStr;
  }

  /**
   * @param args
   */
  public static void newAddRule(Properties clientProp) {
    String srcPath = clientProp.getProperty("deployedRules");
    File sourceFolder = new File(srcPath);
    String rulesStr = "";
    // Check weather source exists and it is folder.
    if (sourceFolder.exists() && sourceFolder.isDirectory()) {
    	List<File> listOfFiles = new ArrayList<File>();
    	String automaticCsvPath = clientProp.getProperty("deployedRulesAutomaticCSVPath");
        String manualCSVPath = clientProp.getProperty("deployedRulesmanualRuleCSVPath");

        File automaticCsv = new File(automaticCsvPath);
        File manualCSV = new File(manualCSVPath);
        
      // Get list of the files and iterate over them
        if (automaticCsv.exists()) {
        	listOfFiles.add(automaticCsv);
          }

          if (manualCSV.exists()) {
        	  listOfFiles.add(manualCSV);
          }

      if (listOfFiles != null) {
        PrintWriter pw = null;
        FileWriter fw = null;
        try {
          fw = new FileWriter(clientProp.getProperty("activeRulesCsvPath"), false);
          pw = new PrintWriter(fw, false);
        } catch (Exception e) {
          log.error("Exception: " + e);
        } finally {

          if (fw != null) {
            try {
              fw.close();
              pw.flush();
              pw.close();
            } catch (Exception e) {
              log.error(e.getMessage());
            }
          }
        }

        for (File fl : listOfFiles) {
          try {
            FileInputStream rulesStream = null;
            try {
              rulesStream = new FileInputStream(new File(srcPath + fl.getName()));
              rulesStr = IOUtils.toString(rulesStream, "UTF-8");
            } catch (IOException e) {
              log.error("Exception: " + e);
              throw e;
            } finally {
              rulesStream.close();
            }

            if (rulesStr != null && !StringUtils.isEmpty(rulesStr.trim())) {
              BufferedWriter bw = null;
              try {
                bw =
                    new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(
                            clientProp.getProperty("activeRulesCsvPath"), true),
                            StandardCharsets.UTF_8));
                bw.append(rulesStr);
              } catch (IOException e) {
                log.error("Exception: " + e);
                throw e;
              } finally {
                if (pw != null) {
                  pw.close();
                }

                if (bw != null) {
                  try {
                    bw.flush();
                    bw.close();
                  } catch (Exception e) {
                    log.error(e.getMessage());
                  }
                }
              }
            }

          } catch (IOException e) {
            log.error("Error in combined files newAddRule(): " + e);
          }
        }
      }
    } else {
      log.error(sourceFolder + "  Folder does not exists");
    }
  }
}
