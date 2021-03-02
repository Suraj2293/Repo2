package net.paladion.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.paladion.util.DaoUtils;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DelayLoggerDaoImpl {

	/**
	 * 
	 * @param connection
	 * @param ins
	 * @param delayLoggerDTOList
	 */
	public void insertDelayLogger(Connection connection, String ins,
			List<String[]> delayLoggerDTOList) {
		Integer batch = 300000;
		PreparedStatement psDelayLogger = null;
		try {
			connection.setAutoCommit(false);
			psDelayLogger = connection.prepareStatement(ins);
			int commitSize = 0;
			for (String[] delay : delayLoggerDTOList) {
				try {
					String agentAddr = delay[0];
					String agentName = delay[1];
					String cusName = delay[2];
					String dvcHost = delay[3];
					String dvcAddr = delay[4];
					String dvcVendor = delay[5];
					String dvcReceiptTime = delay[6];
					String agentReceiptTime = delay[7];
					String eventTime = delay[8];
					String eventIdStr = delay[9];
					String dm = delay[10];

					Timestamp lRT = null;
					if (dvcReceiptTime != null
							&& !StringUtils.isEmpty(dvcReceiptTime)
							&& dvcReceiptTime != "") {
						lRT = Timestamp.valueOf(dvcReceiptTime);
					}
					Timestamp lART = null;
					if (agentReceiptTime != null
							&& !StringUtils.isEmpty(agentReceiptTime)
							&& agentReceiptTime != "") {
						lART = Timestamp.valueOf(agentReceiptTime);
					}

					Timestamp eT = null;
					if (eventTime != null && !StringUtils.isEmpty(eventTime)
							&& eventTime != "") {
						eT = Timestamp.valueOf(eventTime);
					}
					Long eventId = null;
					if (eventIdStr != null && !StringUtils.isEmpty(eventIdStr)
							&& eventIdStr != "") {
						eventId = Long.valueOf(eventIdStr);
					}

					Integer timeMins = 0;
					if (dm != null && !StringUtils.isEmpty(dm) && dm != "") {
						timeMins = Integer.valueOf(dm);
					}
					// timeMins = (int) (Math.random()*(200 - 15)) + 1;
					psDelayLogger.setString(1, agentAddr);
					psDelayLogger.setString(2, agentName);
					psDelayLogger.setString(3, cusName);
					psDelayLogger.setString(4, dvcHost);
					psDelayLogger.setString(5, dvcAddr);
					psDelayLogger.setString(6, dvcVendor);
					psDelayLogger.setTimestamp(7, lRT);
					psDelayLogger.setTimestamp(8, lART);
					psDelayLogger.setTimestamp(9, eT);
					psDelayLogger.setInt(10, timeMins);
					DaoUtils.LongOrNull(11, psDelayLogger, eventId);
					psDelayLogger.executeUpdate();
					commitSize++;
					if (commitSize % batch == 0) {
						connection.commit();
					}
					agentAddr = null;
					agentName = null;
					cusName = null;
					dvcHost = null;
					dvcAddr = null;
					dvcVendor = null;
					dvcReceiptTime = null;
					agentReceiptTime = null;
					eventTime = null;
					eventIdStr = null;
					dm = null;
				} catch (Exception e) {
					log.error(e.getMessage());
				}
			}
			connection.commit();
		} catch (Exception e) {
			log.error("Error in insert(): " + e.getMessage());
		} finally {
			try {
				if(psDelayLogger != null){
				psDelayLogger.close();
				psDelayLogger = null;
				}
			} catch (Exception e) {
				log.error("Unable to close psDelayLogger: " + e.getMessage());
			}
		}
	}
}
