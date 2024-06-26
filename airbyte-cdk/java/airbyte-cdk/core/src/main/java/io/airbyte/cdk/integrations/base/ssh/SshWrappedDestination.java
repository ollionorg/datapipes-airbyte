/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.integrations.base.ssh;

import static io.airbyte.cdk.integrations.base.ssh.SshTunnel.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.cdk.integrations.base.AirbyteMessageConsumer;
import io.airbyte.cdk.integrations.base.AirbyteTraceMessageUtility;
import io.airbyte.cdk.integrations.base.Destination;
import io.airbyte.cdk.integrations.base.SerializedAirbyteMessageConsumer;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorates a Destination with an SSH Tunnel using the standard configuration that Airbyte uses for
 * configuring SSH.
 */
public class SshWrappedDestination implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(SshWrappedDestination.class);

  private final Destination delegate;
  private final List<String> hostKey;
  private final List<String> portKey;
  private final String endPointKey;

  public SshWrappedDestination(final Destination delegate,
                               final List<String> hostKey,
                               final List<String> portKey) {
    this.delegate = delegate;
    this.hostKey = hostKey;
    this.portKey = portKey;
    this.endPointKey = null;
  }

  public SshWrappedDestination(final Destination delegate,
                               final String endPointKey) {
    this.delegate = delegate;
    this.endPointKey = endPointKey;
    this.portKey = null;
    this.hostKey = null;
  }

  @Override
  public ConnectorSpecification spec() throws Exception {
    // inject the standard ssh configuration into the spec.
    final ConnectorSpecification originalSpec = delegate.spec();
    final ObjectNode propNode = (ObjectNode) originalSpec.getConnectionSpecification().get("properties");
    propNode.set("tunnel_method", Jsons.deserialize(MoreResources.readResource("ssh-tunnel-spec.json")));
    return originalSpec;
  }

  @Override
  public AirbyteConnectionStatus check(final JsonNode config) throws Exception {
    try {
      return (endPointKey != null) ? sshWrap(config, endPointKey, delegate::check)
          : sshWrap(config, hostKey, portKey, delegate::check);
    } catch (final RuntimeException e) {
      final String sshErrorMessage = "Could not connect with provided SSH configuration. Error: " + e.getMessage();
      AirbyteTraceMessageUtility.emitConfigErrorTrace(e, sshErrorMessage);
      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage(sshErrorMessage);
    }
  }

  @Override
  public AirbyteMessageConsumer getConsumer(final JsonNode config,
                                            final ConfiguredAirbyteCatalog catalog,
                                            final Consumer<AirbyteMessage> outputRecordCollector)
      throws Exception {
    final SshTunnel tunnel = getTunnelInstance(config);

    final AirbyteMessageConsumer delegateConsumer;
    try {
      delegateConsumer = delegate.getConsumer(tunnel.getConfigInTunnel(), catalog, outputRecordCollector);
    } catch (final Exception e) {
      LOGGER.error("Exception occurred while getting the delegate consumer, closing SSH tunnel", e);
      tunnel.close();
      throw e;
    }
    return AirbyteMessageConsumer.appendOnClose(delegateConsumer, tunnel::close);
  }

  @Override
  public SerializedAirbyteMessageConsumer getSerializedMessageConsumer(final JsonNode config,
                                                                       final ConfiguredAirbyteCatalog catalog,
                                                                       final Consumer<AirbyteMessage> outputRecordCollector)
      throws Exception {
    final JsonNode clone = Jsons.clone(config);
    Optional<JsonNode> connectionOptionsConfig = Jsons.getOptional(clone, CONNECTION_OPTIONS_KEY);
    if (connectionOptionsConfig.isEmpty()) {
      LOGGER.info("No SSH connection options found, using defaults");
      if (clone instanceof ObjectNode) { // Defensive check, it will always be object node
        ObjectNode connectionOptions = ((ObjectNode) clone).putObject(CONNECTION_OPTIONS_KEY);
        connectionOptions.put(SESSION_HEARTBEAT_INTERVAL_KEY, SESSION_HEARTBEAT_INTERVAL_DEFAULT_IN_MILLIS);
        connectionOptions.put(GLOBAL_HEARTBEAT_INTERVAL_KEY, GLOBAL_HEARTBEAT_INTERVAL_DEFAULT_IN_MILLIS);
      }
    }
    final SshTunnel tunnel = getTunnelInstance(clone);
    final SerializedAirbyteMessageConsumer delegateConsumer;
    try {
      delegateConsumer = delegate.getSerializedMessageConsumer(tunnel.getConfigInTunnel(), catalog, outputRecordCollector);
    } catch (final Exception e) {
      LOGGER.error("Exception occurred while getting the delegate consumer, closing SSH tunnel", e);
      tunnel.close();
      throw e;
    }
    return SerializedAirbyteMessageConsumer.appendOnClose(delegateConsumer, tunnel::close);
  }

  protected SshTunnel getTunnelInstance(final JsonNode config) throws Exception {
    return (endPointKey != null)
        ? getInstance(config, endPointKey)
        : getInstance(config, hostKey, portKey);
  }

  @Override
  public boolean isV2Destination() {
    return delegate.isV2Destination();
  }

}
