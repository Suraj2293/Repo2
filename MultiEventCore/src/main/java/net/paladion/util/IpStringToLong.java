/**
 * 
 */
package net.paladion.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;

/**
 * @author ankush
 *
 */
public class IpStringToLong {
	public static Long stringIpToLong(String ip) {
		InetAddress inetIp = null;
		Long longIp = null;

		if (!StringUtils.isEmpty(ip)) {
			try {

				inetIp = InetAddress.getByName(ip.trim());

			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			longIp = ipToLong(inetIp);
		}
		return longIp;
	}

	public static Long ipToLong(InetAddress ip) {
		byte[] octets = ip.getAddress();
		Long result = 0L;
		for (byte octet : octets) {
			result <<= 8;
			result |= octet & 0xff;
		}
		return result;
	}
}
