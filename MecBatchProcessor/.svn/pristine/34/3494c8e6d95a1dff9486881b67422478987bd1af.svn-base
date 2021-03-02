/**
 * 
 */
package net.paladion.util;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

/**
 * @author ankush
 *
 */
public class DaoUtils implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public static void FloatOrNull(int index, PreparedStatement preparedStmt, Float value)
      throws SQLException {
    if (value == null) {
      preparedStmt.setNull(index, Types.FLOAT);
    } else {
      preparedStmt.setFloat(index, value);
    }
  }

  public static void IntOrNull(int index, PreparedStatement preparedStmt, Integer value)
      throws SQLException {
    if (value == null) {
      preparedStmt.setNull(index, Types.INTEGER);
    } else {
      preparedStmt.setInt(index, value);
    }
  }

  public static void LongOrNull(int index, PreparedStatement preparedStmt, Long value)
      throws SQLException {
    if (value == null) {
      preparedStmt.setNull(index, Types.BIGINT);
    } else {
      preparedStmt.setLong(index, value);
    }
  }

  public static void TimestampOrNull(int index, PreparedStatement preparedStmt, Date value)
      throws SQLException {
    if (value == null) {
      preparedStmt.setNull(index, Types.TIMESTAMP);
    } else {
      preparedStmt.setTimestamp(index, new Timestamp(value.getTime()));
    }
  }

  public static void DoubleOrNull(int index, PreparedStatement preparedStmt, Double value)
      throws SQLException {
    if (value == null || value.isNaN()) {
      preparedStmt.setNull(index, Types.DOUBLE);
    } else {
      preparedStmt.setDouble(index, value);
    }
  }
}
