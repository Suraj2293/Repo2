package net.paladion.model;
/**
 * 
 * @author Karthik Madhu
 *
 */
public class OffsetUpdatorKeys {

	private String topic;
	private Integer partition;

	public OffsetUpdatorKeys(String topic1, Integer partition1) {
		super();
		topic = topic1;
		partition = partition1;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((partition == null) ? 0 : partition.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
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
		if (!(obj instanceof OffsetUpdatorKeys))
			return false;
		OffsetUpdatorKeys other = (OffsetUpdatorKeys) obj;
		if (partition == null) {
			if (other.partition != null)
				return false;
		} else if (!partition.equals(other.partition))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}
}
