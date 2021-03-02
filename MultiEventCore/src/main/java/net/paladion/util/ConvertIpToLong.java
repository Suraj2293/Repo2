/**
 * 
 */
package net.paladion.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;

/**
 * @author ankush
 *
 */
@Slf4j
public class ConvertIpToLong {
  public static Long stringIpToLong(String ip) {
    InetAddress inetIp = null;
    long longIp = 0L;

    if (!StringUtils.isEmpty(ip) && ip.trim() != "" && ip.trim() != "-") {
      try {
        inetIp = InetAddress.getByName(ip.trim());
        longIp = ipToLong(inetIp);
      } catch (UnknownHostException e) {
        // log.error("Error while converting ip to long for ip: " + ip
        // + " and error is: " + e.getMessage());
      }
      inetIp = null;
    }
    return longIp;
  }

  public static Long ipToLong(InetAddress ip) {
    long result = 0L;
    if (ip != null) {
      byte[] octets = ip.getAddress();
      for (byte octet : octets) {
        result <<= 8;
        result |= octet & 0xff;
      }
    }
    return result;
  }
}
