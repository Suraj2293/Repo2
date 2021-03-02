/**
 * 
 */
package net.paladion.steps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import net.paladion.dao.RawDataInsertDaoImpl;
import net.paladion.model.MultiEventCoreDTO;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.paladion.racommon.data.BaseDataCarrier;
import com.paladion.racommon.data.DataCarrier;

/**
 * @author ankush
 *
 */
@Slf4j
public class RawDataInsertStep
		extends
		SparkStep<DataCarrier<JavaRDD<MultiEventCoreDTO>>, DataCarrier<JavaRDD<MultiEventCoreDTO>>> {

	private static final long serialVersionUID = 1L;

	private final Properties clientProp;
	private RawDataInsertDaoImpl rawDataInsertDaoImpl;

	public RawDataInsertStep(Properties clientProp1) {
		super();
		clientProp = clientProp1;
		this.rawDataInsertDaoImpl = new RawDataInsertDaoImpl(clientProp);
	}

	@Override
	public DataCarrier<JavaRDD<MultiEventCoreDTO>> customTransform(
			DataCarrier<JavaRDD<MultiEventCoreDTO>> dataCarrier) {
		JavaRDD<MultiEventCoreDTO> dtoStream = dataCarrier.getPayload();

		JavaRDD<MultiEventCoreDTO> dtoStreamNew = dtoStream
				.mapPartitions(new FlatMapFunction<Iterator<MultiEventCoreDTO>, MultiEventCoreDTO>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<MultiEventCoreDTO> call(
							Iterator<MultiEventCoreDTO> multiEventCoreDTOs)
							throws Exception {
						List<MultiEventCoreDTO> multiEventCoreDTOList = new ArrayList<MultiEventCoreDTO>();
						Connection connection = null;
						PreparedStatement psThreatRaw = null;
						try {
							Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
							connection = DriverManager.getConnection(clientProp
									.getProperty("phoenix.database.url"));
							connection.setAutoCommit(false);
							psThreatRaw = connection.prepareStatement("upsert into "
									+ clientProp
											.getProperty("phoenix.schemaname")
									+ "."
									+ clientProp
											.getProperty("threatRaw.tableName")
									+ "(eT,tI,dT,lo,bf,bc,al,ay,cu,bh,bd) values(?,?,?,?,?,?,?,?,?,?,?)");

							while (multiEventCoreDTOs.hasNext()) {
								MultiEventCoreDTO multiEventCoreDTO = multiEventCoreDTOs
										.next();
								rawDataInsertDaoImpl.insertThreatRawData(
										multiEventCoreDTO, connection,
										psThreatRaw);
								multiEventCoreDTOList.add(multiEventCoreDTO);
							}
							connection.commit();
						} catch (SQLException | ClassNotFoundException e) {
							log.error("Error in insertThreatRawData method: "
									+ e.getMessage());
						} finally {
							if (connection != null) {
								try {
									connection.close();
									connection = null;
								} catch (Exception e) {
									log.error(e.getMessage());
								}
							}
							if (psThreatRaw != null) {
								try {
									psThreatRaw.close();
									psThreatRaw = null;
								} catch (Exception e) {
									log.error(e.getMessage());
								}
							}
						}
						return multiEventCoreDTOList.iterator();
					}
				});

		// dtoStreamNew.persist(StorageLevel.MEMORY_ONLY_SER());
		// dtoStreamNew.print(1);
		DataCarrier<JavaRDD<MultiEventCoreDTO>> newDataCarrier = new BaseDataCarrier<JavaRDD<MultiEventCoreDTO>>(
				dtoStreamNew, null);
		return newDataCarrier;
	}
}
