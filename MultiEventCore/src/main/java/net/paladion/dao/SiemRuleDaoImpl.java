/**
 * 
 */
package net.paladion.dao;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import lombok.extern.slf4j.Slf4j;

/**
 * @author ankush
 *
 */
@Slf4j
public class SiemRuleDaoImpl implements Serializable {
	private static final long serialVersionUID = 1L;
	private final Properties clientProp;

	public SiemRuleDaoImpl(Properties clientProps) {
		super();
		clientProp = clientProps;
	}

	/**
	 *
	 * @param conn
	 * @param phoenixSchema
	 * @param tableConetent
	 * @param instanceName
	 * @param toEmail
	 */
	public void sendToRCEDiNotification(Connection conn, String phoenixSchema,
			String tableContent, String toEmail) {
		String instanceName = null;
		try {
			instanceName = java.net.InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			log.error(e1.getMessage());
		}
		PreparedStatement ps = null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		String emailTime = getAlertTime();
		String str = "upsert into "
				+ phoenixSchema
				+ ".email(rd,to,cc,bcc,eti,ci,cu,eod,st,re,rel,cd,pd) values(NEXT VALUE FOR "
				+ phoenixSchema
				+ ".email_sequence,?,NULL,NULL,8,0,?,?,1,NULL,1,now(),now())";
		Map<String, String> emailOtherDetails = new HashMap<String, String>();
		emailOtherDetails.put("tableContent", tableContent);
		emailOtherDetails.put("instanceName", instanceName);
		emailOtherDetails.put("emailTime", emailTime);

		try {
			ObjectOutputStream ob = new ObjectOutputStream(bos);
			ob.writeObject(emailOtherDetails);
			ob.close();
		} catch (Exception e) {
			log.error("Exception while creating the binary object: "
					+ e.getMessage() + e.getClass());
		}
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(str);
			ps.setString(1, toEmail);
			ps.setString(2, instanceName);
			ps.setBytes(3, bos.toByteArray());
			ps.executeUpdate();
			conn.commit();

		} catch (Exception e) {
			log.error("Exception occured in sendToRCEDINotification(): "
					+ e.getMessage() + e.getClass());
		} finally {
			try {
				if (ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				log.error("Excpetion while closing ps in sendToRCEDINotification(): "
						+ e.getMessage());
			}
		}
	}

	/** * * @return */
	public String getAlertTime() {
		TimeZone timeZone = TimeZone.getTimeZone("UTC");
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"YYYY-MM-dd HH:mm:ss.SSS zzz");
		Calendar cal = Calendar.getInstance(timeZone);
		dateFormat.setTimeZone(cal.getTimeZone());
		String currentTime = dateFormat.format(cal.getTime());
		return currentTime;
	}
}
