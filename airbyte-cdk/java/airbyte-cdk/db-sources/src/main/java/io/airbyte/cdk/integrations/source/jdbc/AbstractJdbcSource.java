/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.integrations.source.jdbc;

import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_COLUMN_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_COLUMN_SIZE;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_COLUMN_TYPE;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_COLUMN_TYPE_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_DECIMAL_DIGITS;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_IS_NULLABLE;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_SCHEMA_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.INTERNAL_TABLE_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.JDBC_COLUMN_COLUMN_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.JDBC_COLUMN_DATABASE_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.JDBC_COLUMN_DATA_TYPE;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.JDBC_COLUMN_SCHEMA_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.JDBC_COLUMN_SIZE;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.JDBC_COLUMN_TABLE_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.JDBC_COLUMN_TYPE_NAME;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.JDBC_DECIMAL_DIGITS;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.JDBC_IS_NULLABLE;
import static io.airbyte.cdk.db.jdbc.JdbcConstants.KEY_SEQ;
import static io.airbyte.cdk.integrations.source.relationaldb.RelationalDbQueryUtils.enquoteIdentifier;
import static io.airbyte.cdk.integrations.source.relationaldb.RelationalDbQueryUtils.enquoteIdentifierList;
import static io.airbyte.cdk.integrations.source.relationaldb.RelationalDbQueryUtils.getFullyQualifiedTableNameWithQuoting;
import static io.airbyte.cdk.integrations.source.relationaldb.RelationalDbQueryUtils.queryTable;
import static java.sql.JDBCType.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import datadog.trace.api.Trace;
import io.airbyte.cdk.db.JdbcCompatibleSourceOperations;
import io.airbyte.cdk.db.SqlDatabase;
import io.airbyte.cdk.db.factory.DataSourceFactory;
import io.airbyte.cdk.db.jdbc.JdbcDatabase;
import io.airbyte.cdk.db.jdbc.JdbcUtils;
import io.airbyte.cdk.db.jdbc.StreamingJdbcDatabase;
import io.airbyte.cdk.db.jdbc.streaming.JdbcStreamingQueryConfig;
import io.airbyte.cdk.integrations.base.Source;
import io.airbyte.cdk.integrations.source.jdbc.dto.JdbcPrivilegeDto;
import io.airbyte.cdk.integrations.source.relationaldb.AbstractDbSource;
import io.airbyte.cdk.integrations.source.relationaldb.CursorInfo;
import io.airbyte.cdk.integrations.source.relationaldb.DbSourceDiscoverUtil;
import io.airbyte.cdk.integrations.source.relationaldb.TableInfo;
import io.airbyte.cdk.integrations.source.relationaldb.state.StateManager;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.stream.AirbyteStreamUtils;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.protocol.models.CommonField;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage.Type;
import io.airbyte.protocol.models.v0.AirbyteStream;
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.v0.SyncMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains helper functions and boilerplate for implementing a source connector for a
 * relational DB source which can be accessed via JDBC driver. If you are implementing a connector
 * for a relational DB which has a JDBC driver, make an effort to use this class.
 */
public abstract class AbstractJdbcSource<Datatype> extends AbstractDbSource<Datatype, JdbcDatabase> implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJdbcSource.class);

  protected final Supplier<JdbcStreamingQueryConfig> streamingQueryConfigProvider;
  protected final JdbcCompatibleSourceOperations<Datatype> sourceOperations;

  protected String quoteString;
  protected Collection<DataSource> dataSources = new ArrayList<>();

  public AbstractJdbcSource(final String driverClass,
                            final Supplier<JdbcStreamingQueryConfig> streamingQueryConfigProvider,
                            final JdbcCompatibleSourceOperations<Datatype> sourceOperations) {
    super(driverClass);
    this.streamingQueryConfigProvider = streamingQueryConfigProvider;
    this.sourceOperations = sourceOperations;
  }

  @Override
  protected AutoCloseableIterator<JsonNode> queryTableFullRefresh(final JdbcDatabase database,
      final List<String> columnNames,
      final String schemaName,
      final String tableName,
      final SyncMode syncMode,
      final Optional<String> cursorField,
      final String whereClause,
      final String customSQL) {
    LOGGER.info("Queueing query for table: {}", tableName);
    // This corresponds to the initial sync for in INCREMENTAL_MODE, where the ordering of the records
    // matters
    // as intermediate state messages are emitted (if the connector emits intermediate state).
    if (syncMode.equals(SyncMode.INCREMENTAL) && getStateEmissionFrequency() > 0) {
      final String quotedCursorField = enquoteIdentifier(cursorField.get(), getQuoteString());
      String query = "";
      if (!whereClause.equals("")) {
        query = String.format("SELECT %s FROM %s where %s ORDER BY %s ASC",
            enquoteIdentifierList(columnNames, getQuoteString()),
            getFullyQualifiedTableNameWithQuoting(schemaName, tableName, getQuoteString()),
            whereClause,
            quotedCursorField);
      } else if (customSQL != null && !customSQL.equals("")) {
        query = String.format("SELECT * FROM (%s) sc ORDER BY %s ASC", customSQL, quotedCursorField);
      } else {
        query = String.format("SELECT %s FROM %s ORDER BY %s ASC",
            enquoteIdentifierList(columnNames, getQuoteString()),
            getFullyQualifiedTableNameWithQuoting(schemaName, tableName, getQuoteString()),
            quotedCursorField);
      }

      return queryTable(database, query, tableName, schemaName);
    } else {
      // If we are in FULL_REFRESH mode, state messages are never emitted, so we don't care about ordering
      // of the records.
      String query = "";
      if (!whereClause.equals("")) {
        query = String.format("SELECT %s FROM %s where %s",
            enquoteIdentifierList(columnNames, getQuoteString()),
            getFullyQualifiedTableNameWithQuoting(schemaName, tableName, getQuoteString()),
            whereClause
        );
      } else if (customSQL != null && !customSQL.equals("")) {
        query = customSQL;
      } else {
        query = String.format("SELECT %s FROM %s",
            enquoteIdentifierList(columnNames, getQuoteString()),
            getFullyQualifiedTableNameWithQuoting(schemaName, tableName, getQuoteString())
        );
      }
      return queryTable(database, query, tableName, schemaName);
    }
  }

  /**
   * Configures a list of operations that can be used to check the connection to the source.
   *
   * @return list of consumers that run queries for the check command.
   */
  @Trace(operationName = CHECK_TRACE_OPERATION_NAME)
  protected List<CheckedConsumer<JdbcDatabase, Exception>> getCheckOperations(final JsonNode config) throws Exception {
    return ImmutableList.of(database -> {
      LOGGER.info("Attempting to get metadata from the database to see if we can connect.");
      database.bufferedResultSetQuery(connection -> connection.getMetaData().getCatalogs(), sourceOperations::rowToJson);
    });
  }

  /**
   * Aggregate list of @param entries of StreamName and PrimaryKey and
   *
   * @return a map by StreamName to associated list of primary keys
   */
  @VisibleForTesting
  public static Map<String, List<String>> aggregatePrimateKeys(final List<PrimaryKeyAttributesFromDb> entries) {
    final Map<String, List<String>> result = new HashMap<>();
    entries.stream().sorted(Comparator.comparingInt(PrimaryKeyAttributesFromDb::keySequence)).forEach(entry -> {
      if (!result.containsKey(entry.streamName())) {
        result.put(entry.streamName(), new ArrayList<>());
      }
      result.get(entry.streamName()).add(entry.primaryKey());
    });
    return result;
  }

  private String getCatalog(final SqlDatabase database) {
    return (database.getSourceConfig().has(JdbcUtils.DATABASE_KEY) ? database.getSourceConfig().get(JdbcUtils.DATABASE_KEY).asText() : null);
  }

  @Override
  protected List<TableInfo<CommonField<Datatype>>> discoverInternal(final JdbcDatabase database, final String schema) throws Exception {
    final Set<String> internalSchemas = new HashSet<>(getExcludedInternalNameSpaces());
    LOGGER.info("Internal schemas to exclude: {}", internalSchemas);
    final Set<JdbcPrivilegeDto> tablesWithSelectGrantPrivilege = getPrivilegesTableForCurrentUser(database, schema);
    return database.bufferedResultSetQuery(
        // retrieve column metadata from the database
        connection -> connection.getMetaData().getColumns(getCatalog(database), schema, null, null),
        // store essential column metadata to a Json object from the result set about each column
        this::getColumnMetadata)
        .stream()
        .filter(excludeNotAccessibleTables(internalSchemas, tablesWithSelectGrantPrivilege))
        // group by schema and table name to handle the case where a table with the same name exists in
        // multiple schemas.
        .collect(Collectors.groupingBy(t -> ImmutablePair.of(t.get(INTERNAL_SCHEMA_NAME).asText(), t.get(INTERNAL_TABLE_NAME).asText())))
        .values()
        .stream()
        .map(fields -> TableInfo.<CommonField<Datatype>>builder()
            .nameSpace(fields.get(0).get(INTERNAL_SCHEMA_NAME).asText())
            .name(fields.get(0).get(INTERNAL_TABLE_NAME).asText())
            .fields(fields.stream()
                // read the column metadata Json object, and determine its type
                .map(f -> {
                  final Datatype datatype = sourceOperations.getDatabaseFieldType(f);
                  final JsonSchemaType jsonType = getAirbyteType(datatype);
                  LOGGER.debug("Table {} column {} (type {}[{}], nullable {}) -> {}",
                      fields.get(0).get(INTERNAL_TABLE_NAME).asText(),
                      f.get(INTERNAL_COLUMN_NAME).asText(),
                      f.get(INTERNAL_COLUMN_TYPE_NAME).asText(),
                      f.get(INTERNAL_COLUMN_SIZE).asInt(),
                      f.get(INTERNAL_IS_NULLABLE).asBoolean(),
                      jsonType);
                  return new CommonField<Datatype>(f.get(INTERNAL_COLUMN_NAME).asText(), datatype) {};
                })
                .collect(Collectors.toList()))
            .cursorFields(extractCursorFields(fields))
            .build())
        .collect(Collectors.toList());
  }


  private static final String PROPERTIES = "properties";

  @Override
  protected Datatype getCursorTypeDerivedColumn(final ConfiguredAirbyteStream stream, final String cursorField)  {
    if (stream.getStream().getJsonSchema().get(PROPERTIES) == null) {
      throw new IllegalStateException(String.format("No properties found in stream: %s.", stream.getStream().getName()));
    }
    if (stream.getStream().getJsonSchema().get(PROPERTIES).get(cursorField) == null) {
      throw new IllegalStateException(
              String.format("Could not find cursor field: %s in schema for stream: %s.", cursorField, stream.getStream().getName()));
    } else {
      String propertyType = stream.getStream().getJsonSchema().get(PROPERTIES).get(cursorField).get("type").asText();
      switch (propertyType) {
        case "boolean" -> {
          return (Datatype) BOOLEAN;
        }
        case "integer" -> {
          return (Datatype) BIGINT;
        }
        case "number" -> {
          return (Datatype) DECIMAL;
        }
        case "string" -> {
          String airbyteType;
          String format;
          try {
            airbyteType = stream.getStream().getJsonSchema().get(PROPERTIES).get(cursorField).get("airbyte_type").asText();
            format = stream.getStream().getJsonSchema().get(PROPERTIES).get(cursorField).get("format").asText();
          } catch (NullPointerException e) {
            return (Datatype) VARCHAR;
          }
          switch (format) {
            case "date" -> {
              return (Datatype) DATE;
            }
            case "time" -> {
              if (airbyteType.equals("time_without_timezone")) {
                return (Datatype) TIME;
              } else if (airbyteType.equals("time_with_timezone")) {
                return (Datatype) TIME_WITH_TIMEZONE;
              }
            }
            case "date-time" -> {
              if (airbyteType.equals("timestamp_without_timezone")) {
                return (Datatype) TIMESTAMP;
              } else if (airbyteType.equals("timestamp_with_timezone")) {
                return (Datatype) TIMESTAMP_WITH_TIMEZONE;
              }
            }
            default -> {
              return (Datatype) VARCHAR;
            }
          }
        }
      }
    }
    return (Datatype) VARCHAR;
  }

  private List<String> extractCursorFields(final List<JsonNode> fields) {
    return fields.stream()
        .filter(field -> isCursorType(sourceOperations.getDatabaseFieldType(field)))
        .map(field -> field.get(INTERNAL_COLUMN_NAME).asText())
        .collect(Collectors.toList());
  }

  protected Predicate<JsonNode> excludeNotAccessibleTables(final Set<String> internalSchemas,
                                                           final Set<JdbcPrivilegeDto> tablesWithSelectGrantPrivilege) {
    return jsonNode -> {
      if (tablesWithSelectGrantPrivilege.isEmpty()) {
        return isNotInternalSchema(jsonNode, internalSchemas);
      }
      return tablesWithSelectGrantPrivilege.stream()
          .anyMatch(e -> e.getSchemaName().equals(jsonNode.get(INTERNAL_SCHEMA_NAME).asText()))
          && tablesWithSelectGrantPrivilege.stream()
              .anyMatch(e -> e.getTableName().equals(jsonNode.get(INTERNAL_TABLE_NAME).asText()))
          && !internalSchemas.contains(jsonNode.get(INTERNAL_SCHEMA_NAME).asText());
    };
  }

  // needs to override isNotInternalSchema for connectors that override
  // getPrivilegesTableForCurrentUser()
  protected boolean isNotInternalSchema(final JsonNode jsonNode, final Set<String> internalSchemas) {
    return !internalSchemas.contains(jsonNode.get(INTERNAL_SCHEMA_NAME).asText());
  }

  /**
   * @param resultSet Description of a column available in the table catalog.
   * @return Essential information about a column to determine which table it belongs to and its type.
   */
  private JsonNode getColumnMetadata(final ResultSet resultSet) throws SQLException {
    final var fieldMap = ImmutableMap.<String, Object>builder()
        // we always want a namespace, if we cannot get a schema, use db name.
        .put(INTERNAL_SCHEMA_NAME,
            resultSet.getObject(JDBC_COLUMN_SCHEMA_NAME) != null ? resultSet.getString(JDBC_COLUMN_SCHEMA_NAME)
                : resultSet.getObject(JDBC_COLUMN_DATABASE_NAME))
        .put(INTERNAL_TABLE_NAME, resultSet.getString(JDBC_COLUMN_TABLE_NAME))
        .put(INTERNAL_COLUMN_NAME, resultSet.getString(JDBC_COLUMN_COLUMN_NAME))
        .put(INTERNAL_COLUMN_TYPE, resultSet.getString(JDBC_COLUMN_DATA_TYPE))
        .put(INTERNAL_COLUMN_TYPE_NAME, resultSet.getString(JDBC_COLUMN_TYPE_NAME))
        .put(INTERNAL_COLUMN_SIZE, resultSet.getInt(JDBC_COLUMN_SIZE))
        .put(INTERNAL_IS_NULLABLE, resultSet.getString(JDBC_IS_NULLABLE));
    if (resultSet.getString(JDBC_DECIMAL_DIGITS) != null) {
      fieldMap.put(INTERNAL_DECIMAL_DIGITS, resultSet.getString(JDBC_DECIMAL_DIGITS));
    }
    return Jsons.jsonNode(fieldMap.build());
  }

  @Override
  public List<TableInfo<CommonField<Datatype>>> discoverInternal(final JdbcDatabase database)
      throws Exception {
    return discoverInternal(database, null);
  }

  @Override
  public JsonSchemaType getAirbyteType(final Datatype columnType) {
    return sourceOperations.getAirbyteType(columnType);
  }

  @VisibleForTesting
  public record PrimaryKeyAttributesFromDb(String streamName,
                                           String primaryKey,
                                           int keySequence) {

  }

  @Override
  protected Map<String, List<String>> discoverPrimaryKeys(final JdbcDatabase database,
                                                          final List<TableInfo<CommonField<Datatype>>> tableInfos) {
    LOGGER.info("Discover primary keys for tables: " + tableInfos.stream().map(TableInfo::getName).collect(
        Collectors.toSet()));
    try {
      // Get all primary keys without specifying a table name
      final Map<String, List<String>> tablePrimaryKeys = aggregatePrimateKeys(database.bufferedResultSetQuery(
          connection -> connection.getMetaData().getPrimaryKeys(getCatalog(database), null, null),
          r -> {
            final String schemaName =
                r.getObject(JDBC_COLUMN_SCHEMA_NAME) != null ? r.getString(JDBC_COLUMN_SCHEMA_NAME) : r.getString(JDBC_COLUMN_DATABASE_NAME);
            final String streamName = JdbcUtils.getFullyQualifiedTableName(schemaName, r.getString(JDBC_COLUMN_TABLE_NAME));
            final String primaryKey = r.getString(JDBC_COLUMN_COLUMN_NAME);
            final int keySeq = r.getInt(KEY_SEQ);
            return new PrimaryKeyAttributesFromDb(streamName, primaryKey, keySeq);
          }));
      if (!tablePrimaryKeys.isEmpty()) {
        return tablePrimaryKeys;
      }
    } catch (final SQLException e) {
      LOGGER.debug(String.format("Could not retrieve primary keys without a table name (%s), retrying", e));
    }
    // Get primary keys one table at a time
    return tableInfos.stream()
        .collect(Collectors.toMap(
            tableInfo -> JdbcUtils.getFullyQualifiedTableName(tableInfo.getNameSpace(), tableInfo.getName()),
            tableInfo -> {
              final String streamName = JdbcUtils.getFullyQualifiedTableName(tableInfo.getNameSpace(), tableInfo.getName());
              try {
                final Map<String, List<String>> primaryKeys = aggregatePrimateKeys(database.bufferedResultSetQuery(
                    connection -> connection.getMetaData().getPrimaryKeys(getCatalog(database), tableInfo.getNameSpace(), tableInfo.getName()),
                    r -> new PrimaryKeyAttributesFromDb(streamName, r.getString(JDBC_COLUMN_COLUMN_NAME), r.getInt(KEY_SEQ))));
                return primaryKeys.getOrDefault(streamName, Collections.emptyList());
              } catch (final SQLException e) {
                LOGGER.error(String.format("Could not retrieve primary keys for %s: %s", streamName, e));
                return Collections.emptyList();
              }
            }));
  }

  @Override
  protected String getQuoteString() {
    return quoteString;
  }

  @Override
  public boolean isCursorType(final Datatype type) {
    return sourceOperations.isCursorType(type);
  }

  @Override
  public AutoCloseableIterator<JsonNode> queryTableIncremental(final JdbcDatabase database,
      final List<String> columnNames,
      final String schemaName,
      final String tableName,
      final CursorInfo cursorInfo,
      final Datatype cursorFieldType,
      final String whereClause,
      final String customSQL,
      final Boolean isDerivedColumn) {
    LOGGER.info("Queueing query for table: {}", tableName);
    final io.airbyte.protocol.models.AirbyteStreamNameNamespacePair airbyteStream =
        AirbyteStreamUtils.convertFromNameAndNamespace(tableName, schemaName);
    return AutoCloseableIterators.lazyIterator(() -> {
      try {
        final Stream<JsonNode> stream = database.unsafeQuery(
            connection -> {
              LOGGER.info("Preparing query for table: {}", tableName);
              final String fullTableName = getFullyQualifiedTableNameWithQuoting(schemaName, tableName, getQuoteString());
              final String quotedCursorField = enquoteIdentifier(cursorInfo.getCursorField(), getQuoteString());

              LOGGER.info("customSQL {}", customSQL);

              final String operator;
              if (cursorInfo.getCursorRecordCount() <= 0L) {
                operator = ">";
              } else {
                final long actualRecordCount = getActualCursorRecordCount(
                    connection, fullTableName, quotedCursorField, cursorFieldType, cursorInfo.getCursor(),
                     customSQL, isDerivedColumn);
                LOGGER.info("Table {} cursor count: expected {}, actual {}", tableName,
                    cursorInfo.getCursorRecordCount(), actualRecordCount);
                if (actualRecordCount == cursorInfo.getCursorRecordCount()) {
                  operator = ">";
                } else {
                  operator = ">=";
                }
              }

              final String wrappedColumnNames = getWrappedColumnNames(database, connection, columnNames, schemaName, tableName);
              StringBuilder sql = new StringBuilder();
              if (customSQL!= null && !customSQL.equals("")) {
                sql = new StringBuilder(String.format("SELECT * FROM (%s) sc WHERE %s %s ?", customSQL, quotedCursorField, operator));
              } else {
                sql = new StringBuilder(String.format("SELECT %s FROM %s WHERE %s %s ?",
                    wrappedColumnNames,
                    fullTableName,
                    quotedCursorField,
                    operator));
                // if the connector emits intermediate states, the incremental query must be sorted by the cursor
                // field
                if (!whereClause.equals("")) {
                  sql.append(String.format(" AND %s", whereClause));
                }
              }
              if (getStateEmissionFrequency() > 0) {
                sql.append(String.format(" ORDER BY %s ASC", quotedCursorField));
              }

              final PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
              LOGGER.info("Executing query for table {}: {}", tableName, sql);
              sourceOperations.setCursorField(preparedStatement, 1, cursorFieldType, cursorInfo.getCursor());
              return preparedStatement;
            },
            sourceOperations::rowToJson);
        return AutoCloseableIterators.fromStream(stream, airbyteStream);
      } catch (final SQLException e) {
        throw new RuntimeException(e);
      }
    }, airbyteStream);
  }

  /**
   * Some databases need special column names in the query.
   */
  protected String getWrappedColumnNames(final JdbcDatabase database,
                                         final Connection connection,
                                         final List<String> columnNames,
                                         final String schemaName,
                                         final String tableName)
      throws SQLException {
    return enquoteIdentifierList(columnNames, getQuoteString());
  }

  protected String getCountColumnName() {
    return "record_count";
  }

  protected long getActualCursorRecordCount(final Connection connection,
                                            final String fullTableName,
                                            final String quotedCursorField,
                                            final Datatype cursorFieldType,
                                            final String cursor,final String customSQL,
                                            final Boolean isDerivedColumn)
      throws SQLException {
    final String columnName = getCountColumnName();
    final PreparedStatement cursorRecordStatement;
    if (cursor == null) {
      final String cursorRecordQuery = String.format("SELECT COUNT(*) AS %s FROM %s WHERE %s IS NULL",
          columnName,
          fullTableName,
          quotedCursorField);
      cursorRecordStatement = connection.prepareStatement(cursorRecordQuery);
    } else if (isDerivedColumn || !customSQL.isEmpty()) {
      LOGGER.info("columnName: {}, customSQL: {}, quotedCursorField: {}", columnName, customSQL, quotedCursorField);
      final String cursorRecordQuery = String.format("SELECT COUNT(*) AS %s FROM (%s) sc WHERE %s = ?",
          columnName,
          customSQL,
          quotedCursorField);
      cursorRecordStatement = connection.prepareStatement(cursorRecordQuery);
      sourceOperations.setCursorField(cursorRecordStatement, 1, cursorFieldType, cursor);
    } else {
      final String cursorRecordQuery = String.format("SELECT COUNT(*) AS %s FROM %s WHERE %s = ?",
          columnName,
          fullTableName,
          quotedCursorField);
      cursorRecordStatement = connection.prepareStatement(cursorRecordQuery);;
      sourceOperations.setCursorField(cursorRecordStatement, 1, cursorFieldType, cursor);
    }
    final ResultSet resultSet = cursorRecordStatement.executeQuery();
    if (resultSet.next()) {
      return resultSet.getLong(columnName);
    } else {
      return 0L;
    }
  }

  @Override
  public JdbcDatabase createDatabase(final JsonNode sourceConfig) throws SQLException {
    return createDatabase(sourceConfig, JdbcDataSourceUtils.DEFAULT_JDBC_PARAMETERS_DELIMITER);
  }

  public JdbcDatabase createDatabase(final JsonNode sourceConfig, String delimiter) throws SQLException {
    final JsonNode jdbcConfig = toDatabaseConfig(sourceConfig);
    Map<String, String> connectionProperties = JdbcDataSourceUtils.getConnectionProperties(sourceConfig, delimiter);
    // Create the data source
    final DataSource dataSource = DataSourceFactory.create(
        jdbcConfig.has(JdbcUtils.USERNAME_KEY) ? jdbcConfig.get(JdbcUtils.USERNAME_KEY).asText() : null,
        jdbcConfig.has(JdbcUtils.PASSWORD_KEY) ? jdbcConfig.get(JdbcUtils.PASSWORD_KEY).asText() : null,
        driverClassName,
        jdbcConfig.get(JdbcUtils.JDBC_URL_KEY).asText(),
        connectionProperties,
        getConnectionTimeout(connectionProperties));
    // Record the data source so that it can be closed.
    dataSources.add(dataSource);

    final JdbcDatabase database = new StreamingJdbcDatabase(
        dataSource,
        sourceOperations,
        streamingQueryConfigProvider);

    quoteString = (quoteString == null ? database.getMetaData().getIdentifierQuoteString() : quoteString);
    database.setSourceConfig(sourceConfig);
    database.setDatabaseConfig(jdbcConfig);
    return database;
  }

  /**
   * {@inheritDoc}
   *
   * @param database database instance
   * @param catalog schema of the incoming messages.
   * @throws SQLException
   */
  @Override
  protected void logPreSyncDebugData(final JdbcDatabase database, final ConfiguredAirbyteCatalog catalog)
      throws SQLException {
    LOGGER.info("Data source product recognized as {}:{}",
        database.getMetaData().getDatabaseProductName(),
        database.getMetaData().getDatabaseProductVersion());
  }

  @Override
  public void close() {
    dataSources.forEach(d -> {
      try {
        DataSourceFactory.close(d);
      } catch (final Exception e) {
        LOGGER.warn("Unable to close data source.", e);
      }
    });
    dataSources.clear();
  }

  protected List<ConfiguredAirbyteStream> identifyStreamsToSnapshot(final ConfiguredAirbyteCatalog catalog, final StateManager stateManager) {
    final Set<AirbyteStreamNameNamespacePair> alreadySyncedStreams = stateManager.getCdcStateManager().getInitialStreamsSynced();
    if (alreadySyncedStreams.isEmpty() && (stateManager.getCdcStateManager().getCdcState() == null
        || stateManager.getCdcStateManager().getCdcState().getState() == null)) {
      return Collections.emptyList();
    }

    final Set<AirbyteStreamNameNamespacePair> allStreams = AirbyteStreamNameNamespacePair.fromConfiguredCatalog(catalog);

    final Set<AirbyteStreamNameNamespacePair> newlyAddedStreams = new HashSet<>(Sets.difference(allStreams, alreadySyncedStreams));

    return catalog.getStreams().stream()
        .filter(c -> c.getSyncMode() == SyncMode.INCREMENTAL)
        .filter(stream -> newlyAddedStreams.contains(AirbyteStreamNameNamespacePair.fromAirbyteStream(stream.getStream())))
        .map(Jsons::clone)
        .collect(Collectors.toList());
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> read(final JsonNode config,
      final ConfiguredAirbyteCatalog catalog,
      final JsonNode state)
      throws Exception {
    LOGGER.info(catalog.toString());
    if (!isDiscoverForCustomSQL(catalog)) {
      return super.read(config, catalog, state);
    }

    final List<AutoCloseableIterator<AirbyteMessage>> iteratorList = new ArrayList<>();
    try {
      final JdbcDatabase database = createDatabase(config);

      List<TableInfo<CommonField<Datatype>>> tableInfos = catalog.getStreams().stream()
          .map(configuredAirbyteStream -> {
            AirbyteStream stream = configuredAirbyteStream.getStream();
            String customSQL = (String) stream.getAdditionalProperties().get("custom_sql");
            if (customSQL.isEmpty()) {
              throw new RuntimeException("Custom SQL is required to discover the schema");
            }
            try {
              return database.unsafeQuery(
                  connection -> connection.prepareStatement(customSQL),
                  resultSet -> this.getColumnMetadataForCustomSQL(resultSet, stream.getNamespace(),
                      stream.getName())
              ).findFirst();
            } catch (SQLException e) {
              throw new RuntimeException("Error executing custom SQL: " + customSQL, e);
            }
          })
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toList());

      return AutoCloseableIterators.fromStream(
          Stream.of(new AirbyteMessage().withType(Type.CATALOG)
              .withCatalog(DbSourceDiscoverUtil.convertTableInfosToAirbyteCatalog(
                  tableInfos, new HashMap<>(), this::getAirbyteType))),
          AirbyteStreamUtils.convertFromNameAndNamespace("tableName", "schemaName")
      );
    } finally {
      close();
    }
  }

  private boolean isDiscoverForCustomSQL(ConfiguredAirbyteCatalog catalog) {
    return catalog.getStreams()
        .stream()
        .map(ConfiguredAirbyteStream::getStream)
        .anyMatch(
            airbyteStream -> airbyteStream.getAdditionalProperties().containsKey("is_discover")
                && (boolean) airbyteStream.getAdditionalProperties().get("is_discover"));
  }


  private TableInfo<CommonField<Datatype>> getColumnMetadataForCustomSQL(ResultSet resultSet,
      String namespace, String name)
      throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();

    List<CommonField<Datatype>> fields = IntStream.rangeClosed(1, metaData.getColumnCount())
        .mapToObj(i -> {
          try {
            String columnName = metaData.getColumnName(i);
            Datatype datatype = sourceOperations.getDatabaseFieldType(
                Jsons.jsonNode(ImmutableMap.<String, Object>builder()
                    // we always want a namespace, if we cannot get a schema, use db name.
                    .put(INTERNAL_SCHEMA_NAME, namespace)
                    .put(INTERNAL_TABLE_NAME, name)
                    .put(INTERNAL_COLUMN_NAME, metaData.getColumnName(i))
                    .put(INTERNAL_COLUMN_TYPE, metaData.getColumnType(i))
                    .put(INTERNAL_COLUMN_TYPE_NAME, metaData.getColumnTypeName(i))
                    .put(INTERNAL_COLUMN_SIZE, metaData.getColumnDisplaySize(i))
                    .put(INTERNAL_DECIMAL_DIGITS, metaData.getScale(i))
                    .put(INTERNAL_IS_NULLABLE, metaData.isNullable(i)).build())
            );
            return new CommonField<>(columnName, datatype);
          } catch (SQLException e) {
            throw new RuntimeException("Error while processing column metadata", e);
          }
        })
        .collect(Collectors.toList());
    return TableInfo.<CommonField<Datatype>>builder()
        .nameSpace(namespace)
        .name(name)
        .fields(fields)
        .primaryKeys(List.of())
        .cursorFields(List.of())
        .build();
  }

}
