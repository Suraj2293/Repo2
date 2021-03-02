/**
 * 
 */
package net.paladion.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

//import org.drools.core.util.StringUtils;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.util.DaoUtils;

/**
 * @author ankush
 *
 */
@Slf4j
public class RawDataInsertDaoImpl extends DaoUtils implements Serializable {
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private final Properties clientProp;

	public RawDataInsertDaoImpl(Properties clientProp) {
		this.clientProp = clientProp;
	}

	public void insertThreatRawData(MultiEventCoreDTO multiEventCoreDTO,
			Connection connection, PreparedStatement psThreatRaw) {

		try {
			TimestampOrNull(1, psThreatRaw, multiEventCoreDTO.getEventTime());
			LongOrNull(2, psThreatRaw, multiEventCoreDTO.getThreatId());
			psThreatRaw.setArray(
					3,
					connection.createArrayOf("VARCHAR",
							multiEventCoreDTO.getDataArray()));

			String custName = null;
			int custId = 0;
			if (!StringUtils.isEmpty(multiEventCoreDTO.getCustomerName())) {
				String[] customerName = multiEventCoreDTO.getCustomerName()
						.split("_");
				if (customerName != null && customerName.length > 0 && StringUtils.isNumeric(customerName[0])) {
					custId = Integer.parseInt(customerName[0]);
				}
				if (customerName != null && customerName.length > 1 && !StringUtils.isEmpty(customerName[1])) {
					custName = customerName[1].trim();
				}
			}
			psThreatRaw.setString(4, custName);
			psThreatRaw.setString(5, multiEventCoreDTO.getDeviceVendor());
			psThreatRaw.setString(6, multiEventCoreDTO.getDeviceProduct());
			psThreatRaw.setString(7, multiEventCoreDTO.getDeviceAddress());
			psThreatRaw.setString(8, multiEventCoreDTO.getDeviceHostName());
			psThreatRaw.setInt(9, custId);
			psThreatRaw.setLong(10, multiEventCoreDTO.getEventId());
			psThreatRaw.setTimestamp(11,
					multiEventCoreDTO.getDeviceReceiptTime());
			psThreatRaw.executeUpdate();
		} catch (SQLException e) {
			log.error("Error in insertThreatRawData method: " + e.getMessage());
		}
	}
}
