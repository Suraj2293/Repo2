/**
 * 
 */
package net.paladion.dao;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import lombok.extern.slf4j.Slf4j;
import net.paladion.model.MultiEventCoreDTO;
import net.paladion.util.DaoUtils;

/**
 * @author ankush
 *
 */
@Slf4j
public class GeoZoneTaggingDaoImpl extends DaoUtils implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * 
   * @param multiEventCoreDTO
   * @param psmtZone
   */
  public void addZone(MultiEventCoreDTO multiEventCoreDTO, PreparedStatement psmtZone) {
    ResultSet rsZone = null;

    try {
      String customerId = multiEventCoreDTO.getCustomerId();
      customerId = customerId.substring(0, customerId.length() - 1);

      psmtZone.setLong(1, multiEventCoreDTO.getSourceAddressLong());
      psmtZone.setString(2, customerId + "%");
      psmtZone.setLong(3, multiEventCoreDTO.getDestinationAddressLong());
      psmtZone.setString(4, customerId + "%");

      rsZone = psmtZone.executeQuery();

      if (rsZone != null) {
        while (rsZone.next()) {
          if (rsZone.getString("a.at") != null && rsZone.getString("a.at").equalsIgnoreCase("src")
              && multiEventCoreDTO.getSourceAddressLong() != 0) {
            multiEventCoreDTO.setSourceZone(rsZone.getString("zo"));
          }
          if (rsZone.getString("a.at") != null && rsZone.getString("a.at").equalsIgnoreCase("dest")
              && multiEventCoreDTO.getDestinationAddressLong() != 0) {
            multiEventCoreDTO.setDestinationZone(rsZone.getString("zo"));
          }
        }
      }
      customerId = null;
    } catch (Exception e) {
      log.error("Error in GeoZoneTaggingDaoImpl, addZone method: " + e.getMessage() + " IP is: "
          + multiEventCoreDTO.getSourceAddress() + " and customer id is: "
          + multiEventCoreDTO.getCustomerId());
    } finally {
      if (rsZone != null) {
        try {
          rsZone.close();
          rsZone = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
    }
  }

  /**
   * 
   * @param multiEventCoreDTO
   * @param psmtGeo
   */
  public void addGeo(MultiEventCoreDTO multiEventCoreDTO, PreparedStatement psmtGeo) {
    ResultSet rsGeo = null;
    try {
      psmtGeo.setLong(1, multiEventCoreDTO.getSourceAddressLong());
      psmtGeo.setLong(2, multiEventCoreDTO.getDestinationAddressLong());
      rsGeo = psmtGeo.executeQuery();

      if (rsGeo != null) {
        while (rsGeo.next()) {
          if (rsGeo.getString("a.at") != null && rsGeo.getString("a.at").equalsIgnoreCase("src")
              && multiEventCoreDTO.getSourceAddressLong() != 0) {
            multiEventCoreDTO.setSourceGeoCountryName(rsGeo.getString("con"));
            multiEventCoreDTO.setSourceGeoCityName(rsGeo.getString("cin"));
          }
          if (rsGeo.getString("a.at") != null && rsGeo.getString("a.at").equalsIgnoreCase("dest")
              && multiEventCoreDTO.getDestinationAddressLong() != 0) {
            multiEventCoreDTO.setDestinationGeoCountryName(rsGeo.getString("con"));
            multiEventCoreDTO.setDestinationGeoCityName(rsGeo.getString("cin"));
          }
        }
      }
    } catch (Exception e) {
      log.error("Error in GeoZoneTaggingDaoImpl, addGeo method: " + e.getMessage() + " IP is: "
          + multiEventCoreDTO.getSourceAddress());
    } finally {
      if (rsGeo != null) {
        try {
          rsGeo.close();
          rsGeo = null;
        } catch (Exception e) {
          log.error(e.getMessage());
        }
      }
    }
  }
}
