/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.oracle;

import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_COLUMN_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_COLUMN_TYPE;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_SCHEMA_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_TABLE_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.cdk.db.jdbc.DateTimeConverter;
import io.airbyte.cdk.db.jdbc.JdbcSourceOperations;
import io.airbyte.protocol.models.JsonSchemaType;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.util.Arrays;
import oracle.jdbc.OracleType;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleSourceOperations extends JdbcSourceOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleSourceOperations.class);

  /**
   * Oracle's DATE type can actually have time values, so we return a full timestamp.
   */
  @Override
  protected void putDate(ObjectNode node, String columnName, ResultSet resultSet, int index) throws SQLException {
    node.put(columnName, DateTimeConverter.convertToTimestamp(resultSet.getTimestamp(index)));
  }

  protected JDBCType safeGetJdbcType(final int columnTypeInt) {
    try {
      if (Arrays.asList(
          OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getVendorTypeNumber(),
          OracleType.TIMESTAMP_WITH_TIME_ZONE.getVendorTypeNumber()
      ).contains(columnTypeInt)) {
        return JDBCType.TIMESTAMP_WITH_TIMEZONE;
      }

      if (Arrays.asList(
          OracleType.BINARY_FLOAT.getVendorTypeNumber(),
          OracleType.BINARY_DOUBLE.getVendorTypeNumber()
      ).contains(columnTypeInt)) {
        return JDBCType.DOUBLE;
      }
      return JDBCType.valueOf(columnTypeInt);
    } catch (final Exception e) {
      return JDBCType.VARCHAR;
    }
  }

  @Override
  public JDBCType getDatabaseFieldType(final JsonNode field) {
    try {
      if (Arrays.asList(
          OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getVendorTypeNumber(),
          OracleType.TIMESTAMP_WITH_TIME_ZONE.getVendorTypeNumber()
      ).contains(field.get(INTERNAL_COLUMN_TYPE).asInt())) {
        return JDBCType.TIMESTAMP_WITH_TIMEZONE;
      }

      if (Arrays.asList(
          OracleType.BINARY_FLOAT.getVendorTypeNumber(),
          OracleType.BINARY_DOUBLE.getVendorTypeNumber()
      ).contains(field.get(INTERNAL_COLUMN_TYPE).asInt())) {
        return JDBCType.DOUBLE;
      }

      return JDBCType.valueOf(field.get(INTERNAL_COLUMN_TYPE).asInt());
    } catch (final IllegalArgumentException ex) {
      LOGGER.warn(String.format(
          "Could not convert column: %s from table: %s.%s with type: %s. Casting to VARCHAR.",
          field.get(INTERNAL_COLUMN_NAME),
          field.get(INTERNAL_SCHEMA_NAME),
          field.get(INTERNAL_TABLE_NAME),
          field.get(INTERNAL_COLUMN_TYPE)));
      return JDBCType.VARCHAR;
    }
  }

  @Override
  public JsonSchemaType getAirbyteType(final JDBCType jdbcType) {
    return switch (jdbcType) {
      case BIT, BOOLEAN -> JsonSchemaType.BOOLEAN;
      case TINYINT, SMALLINT -> JsonSchemaType.INTEGER;
      case INTEGER -> JsonSchemaType.INTEGER;
      case BIGINT -> JsonSchemaType.INTEGER;
      case FLOAT, DOUBLE -> JsonSchemaType.NUMBER;
      case REAL -> JsonSchemaType.NUMBER;
      case NUMERIC, DECIMAL -> JsonSchemaType.NUMBER;
      case CHAR, NCHAR, NVARCHAR, VARCHAR, LONGVARCHAR -> JsonSchemaType.STRING;
      case DATE -> JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE;
      case TIME -> JsonSchemaType.STRING_TIMESTAMP_WITH_TIMEZONE;
      case TIMESTAMP_WITH_TIMEZONE -> JsonSchemaType.STRING_TIMESTAMP_WITH_TIMEZONE;
      case TIMESTAMP -> JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE;
      case BLOB, BINARY, VARBINARY, LONGVARBINARY -> JsonSchemaType.STRING_BASE_64;
      case ARRAY -> JsonSchemaType.ARRAY;
      // since column types aren't necessarily meaningful to Airbyte, liberally convert all unrecgonised
      // types to String
      default -> JsonSchemaType.STRING;
    };
  }

}
