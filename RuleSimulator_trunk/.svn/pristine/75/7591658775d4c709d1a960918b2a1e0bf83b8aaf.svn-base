package net.paladion.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import net.paladion.model.RuleSimulationDetailsDTO;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleSimulationDaoImpl implements Serializable {
	private static final long serialVersionUID = 1L;
	private final Properties clientProp;

	public RuleSimulationDaoImpl(Properties clientProps) {
		super();
		clientProp = clientProps;
	}

	public RuleSimulationDetailsDTO getEventTime() {
		Connection connection = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		RuleSimulationDetailsDTO ruleSimulationDetailsDTO = new RuleSimulationDetailsDTO();
		String query = "";
		query = "select INGESTION_START_TIME, INGESTION_END_TIME from "
				+ clientProp.getProperty("phoenix.schemaname")
				+ ".rule_simulation_details where flag is null";
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			connection = DriverManager.getConnection(clientProp
					.getProperty("phoenix.database.url"));

			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();

			if (rs != null) {
				while (rs.next()) {
					ruleSimulationDetailsDTO.setIngestionStartTime(rs
							.getTimestamp("INGESTION_START_TIME"));
					ruleSimulationDetailsDTO.setIngestionEndTime(rs
							.getTimestamp("INGESTION_END_TIME"));
				}
			}
			query = null;
		} catch (Exception e) {
			log.error("Exception occured in getEventTime() of class RuleSimulationDaoImpl. Due to: "
					+ e.getMessage());
		} finally {
			try {
				if (rs != null) {
					rs.close();
					rs = null;
				}
			} catch (SQLException e) {
				log.error("Exception occured while closing rs in method getEventTime() in class RuleSimulationDaoImpl: "
						+ e.getMessage());
			}
			try {
				if (ps != null) {
					ps.close();
					ps = null;
				}
			} catch (SQLException e) {
				log.error("Exception occured while closing ps in method getEventTime() in class RuleSimulationDaoImpl: "
						+ e.getMessage());
			}
			try {
				if (connection != null) {
					connection.close();
					connection = null;
				}
			} catch (SQLException e) {
				log.error("Exception occured while closing connection in method getEventTime() in class RuleSimulationDaoImpl: "
						+ e.getMessage());
			}
		}
		return ruleSimulationDetailsDTO;

	}
}
