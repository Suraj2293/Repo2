package net.paladion.listener;

import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

import com.paladion.racommon.ConnectorCommonConfigurationException;
import com.paladion.racommon.launcher.SparkApplicationLauncher;


@Slf4j
public class MultiEventCoreLauncher extends SparkApplicationLauncher {


  /**
   * 
   * @param args spring configuration file and logger configuimport
   *        com.paladion.connector.arcsight.launcher.ApplicationLauncher; import
   *        com.paladion.connector.arcsight.launcher.ArcsightConnectorLauncher ;ration file.
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
//    LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
//    JoranConfigurator jc = new JoranConfigurator();
//    jc.setContext(ctx);
//    try {
//      ctx.reset();
//      jc.doConfigure(args[1]);
//    } catch (Exception e) {
//      try {
//        throw new ConnectorCommonConfigurationException("Error while configuring logger", e);
//      } catch (ConnectorCommonConfigurationException e1) {
//        log.error(e1.getMessage());
//      }
//    }
    new MultiEventCoreLauncher().launch(args);
  }
}
