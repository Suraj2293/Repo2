package net.paladion.model;

import java.sql.Timestamp;

import lombok.Getter;
import lombok.Setter;

public class ThreatRuleMatchDTO {
	@Getter
	@Setter
	private Long ti;
	@Getter
	@Setter
	private Timestamp et;
	@Getter
	@Setter
	private String dt;
}
