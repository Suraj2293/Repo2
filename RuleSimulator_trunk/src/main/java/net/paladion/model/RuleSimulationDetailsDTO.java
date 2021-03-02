package net.paladion.model;

import java.sql.Timestamp;

import lombok.Getter;
import lombok.Setter;

public class RuleSimulationDetailsDTO {
	@Getter
	@Setter
	private Integer recId;
	@Getter
	@Setter
	private Integer ruleId;
	@Getter
	@Setter
	private String ruleName;
	@Getter
	@Setter
	private Timestamp ingestionStartTime;
	@Getter
	@Setter
	private Timestamp ingestionEndTime;
	@Getter
	@Setter
	private String customerName;
	@Getter
	@Setter
	private String deviceProduct;
	@Getter
	@Setter
	private String deviceVendor;
	@Getter
	@Setter
	private String status;
	@Getter
	@Setter
	private Timestamp simulationStartTime;
	@Getter
	@Setter
	private Timestamp simulationEndTime;
	@Getter
	@Setter
	private Integer datasetCount;
	@Getter
	@Setter
	private Integer ruleMatchCount;
	@Getter
	@Setter
	private Timestamp createdDate;
}
