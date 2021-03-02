package net.paladion.model;

import lombok.Getter;
import lombok.Setter;

public class DelayLoggerKeys {
	@Getter
	@Setter
	private String agentAddr;
	@Getter
	@Setter
	private String agentName;
	@Getter
	@Setter
	private String cusName;
	@Getter
	@Setter
	private String dvcHost;
	@Getter
	@Setter
	private String dvcAddr;
	@Getter
	@Setter
	private String dvcVendor;
	@Getter
	@Setter
	private String delayFlag;

	public DelayLoggerKeys(String agentAddr2, String agentName2,
			String cusName2, String dvcHost2, String dvcAddr2,
			String dvcVendor2, String delayFlag2) {
		super();
		agentAddr = agentAddr2;
		agentName = agentName2;
		cusName = cusName2;
		dvcHost = dvcHost2;
		dvcAddr = dvcAddr2;
		dvcVendor = dvcVendor2;
		delayFlag = delayFlag2;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((agentAddr == null) ? 0 : agentAddr.hashCode());
		result = prime * result
				+ ((agentName == null) ? 0 : agentName.hashCode());
		result = prime * result + ((cusName == null) ? 0 : cusName.hashCode());
		result = prime * result
				+ ((delayFlag == null) ? 0 : delayFlag.hashCode());
		result = prime * result + ((dvcAddr == null) ? 0 : dvcAddr.hashCode());
		result = prime * result + ((dvcHost == null) ? 0 : dvcHost.hashCode());
		result = prime * result
				+ ((dvcVendor == null) ? 0 : dvcVendor.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof DelayLoggerKeys))
			return false;
		DelayLoggerKeys other = (DelayLoggerKeys) obj;
		if (agentAddr == null) {
			if (other.agentAddr != null)
				return false;
		} else if (!agentAddr.equals(other.agentAddr))
			return false;
		if (agentName == null) {
			if (other.agentName != null)
				return false;
		} else if (!agentName.equals(other.agentName))
			return false;
		if (cusName == null) {
			if (other.cusName != null)
				return false;
		} else if (!cusName.equals(other.cusName))
			return false;
		if (delayFlag == null) {
			if (other.delayFlag != null)
				return false;
		} else if (!delayFlag.equals(other.delayFlag))
			return false;
		if (dvcAddr == null) {
			if (other.dvcAddr != null)
				return false;
		} else if (!dvcAddr.equals(other.dvcAddr))
			return false;
		if (dvcHost == null) {
			if (other.dvcHost != null)
				return false;
		} else if (!dvcHost.equals(other.dvcHost))
			return false;
		if (dvcVendor == null) {
			if (other.dvcVendor != null)
				return false;
		} else if (!dvcVendor.equals(other.dvcVendor))
			return false;
		return true;
	}
}
