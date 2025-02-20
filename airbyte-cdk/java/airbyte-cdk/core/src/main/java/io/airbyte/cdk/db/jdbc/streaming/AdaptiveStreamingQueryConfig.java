/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.db.jdbc.streaming;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdaptiveStreamingQueryConfig implements JdbcStreamingQueryConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveStreamingQueryConfig.class);
  private final FetchSizeEstimator fetchSizeEstimator;
  private int currentFetchSize;

  public AdaptiveStreamingQueryConfig() {
    this.fetchSizeEstimator = TwoStageSizeEstimator.getInstance();
    this.currentFetchSize = FetchSizeConstants.INITIAL_SAMPLE_SIZE;
  }

  @Override
  public void initialize(final Connection connection, final Statement preparedStatement) throws SQLException {
    connection.setAutoCommit(false);
    preparedStatement.setFetchSize(FetchSizeConstants.INITIAL_SAMPLE_SIZE);
    currentFetchSize = FetchSizeConstants.INITIAL_SAMPLE_SIZE;
    LOGGER.info("Set initial fetch size: {} rows", preparedStatement.getFetchSize());
  }

  @Override
  public void accept(final ResultSet resultSet, final Object rowData) throws SQLException {
    fetchSizeEstimator.accept(rowData);
    final Optional<Integer> newFetchSize = fetchSizeEstimator.getFetchSize();

    int CUSTOM_FETCH_SIZE = -1;
    if(System.getenv("CUSTOM_FETCH_SIZE") != null){
      CUSTOM_FETCH_SIZE = Integer.parseInt(System.getenv("CUSTOM_FETCH_SIZE"));
    }

    if (CUSTOM_FETCH_SIZE > 0 && currentFetchSize != CUSTOM_FETCH_SIZE){
      LOGGER.info("custom fetch size: {} rows", CUSTOM_FETCH_SIZE);
      resultSet.setFetchSize(CUSTOM_FETCH_SIZE);
      currentFetchSize = CUSTOM_FETCH_SIZE;
      return;
    }

    if (newFetchSize.isPresent() && currentFetchSize != newFetchSize.get() && CUSTOM_FETCH_SIZE < 0) {
      LOGGER.info("Set new fetch size: {} rows", newFetchSize.get());
      resultSet.setFetchSize(newFetchSize.get());
      currentFetchSize = newFetchSize.get();
    }
  }
}
