package com.streamlens.service.connection;

import com.azure.core.credential.TokenCredential;
import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.model.connection.ConnectionProfile.ConnectionType;
import com.streamlens.model.connection.EventHubsConnectionConfig;
import com.streamlens.model.connection.KafkaConnectionConfig;
import com.streamlens.service.security.AzureAuthService;
import com.streamlens.service.storage.ConnectionRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Service for managing connection profiles and testing connections.
 */
@Service
public class ConnectionService {

    private static final Logger log = LoggerFactory.getLogger(ConnectionService.class);
    private static final int CONNECTION_TIMEOUT_SECONDS = 10;

    private final ConnectionRepository connectionRepository;
    private final AzureAuthService azureAuthService;
    private final Map<String, Object> activeConnections = new HashMap<>();

    public ConnectionService(ConnectionRepository connectionRepository, AzureAuthService azureAuthService) {
        this.connectionRepository = connectionRepository;
        this.azureAuthService = azureAuthService;
    }

    /**
     * Creates a new connection profile.
     */
    public ConnectionProfile createConnection(String name, ConnectionType type, Object config) {
        if (connectionRepository.existsByName(name)) {
            throw new IllegalArgumentException("Connection with name '" + name + "' already exists");
        }

        ConnectionProfile profile = new ConnectionProfile(name, type);
        profile.setConfig(config);
        connectionRepository.save(profile);
        return profile;
    }

    /**
     * Updates an existing connection profile.
     */
    public void updateConnection(ConnectionProfile profile) {
        connectionRepository.update(profile);
    }

    /**
     * Deletes a connection profile.
     */
    public void deleteConnection(String id) {
        disconnect(id);
        connectionRepository.delete(id);
    }

    /**
     * Gets all connection profiles.
     */
    public List<ConnectionProfile> getAllConnections() {
        return connectionRepository.findAll();
    }

    /**
     * Gets a connection profile by ID.
     */
    public Optional<ConnectionProfile> getConnection(String id) {
        return connectionRepository.findById(id);
    }

    /**
     * Tests a Kafka connection and returns topic list on success.
     */
    public ConnectionTestResult testKafkaConnection(KafkaConnectionConfig config) {
        log.info("Testing Kafka connection to: {}", config.getBootstrapServers());

        Map<String, Object> props = config.toKafkaProperties();
        props.put("request.timeout.ms", CONNECTION_TIMEOUT_SECONDS * 1000);
        props.put("default.api.timeout.ms", CONNECTION_TIMEOUT_SECONDS * 1000);

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topics = topicsResult.names().get(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.info("Kafka connection successful, found {} topics", topics.size());
            return new ConnectionTestResult(true, "Connected successfully", new ArrayList<>(topics));
        } catch (Exception e) {
            String message = extractErrorMessage(e);
            log.error("Kafka connection test failed: {}", message);
            return new ConnectionTestResult(false, message, Collections.emptyList());
        }
    }

    /**
     * Tests an Azure Event Hubs connection.
     */
    public ConnectionTestResult testEventHubsConnection(EventHubsConnectionConfig config) {
        log.info("Testing Event Hubs connection to: {}", config.getFullyQualifiedNamespace());

        try {
            // Create EventHubConsumerAsyncClient to test connection
            var clientBuilder = new com.azure.messaging.eventhubs.EventHubClientBuilder();

            switch (config.getAuthenticationType()) {
                case CONNECTION_STRING:
                    clientBuilder.connectionString(
                            config.getConnectionString(),
                            config.getEventHubName()
                    );
                    break;

                case MANAGED_IDENTITY:
                    TokenCredential managedIdCred = azureAuthService.getManagedIdentityCredential();
                    clientBuilder.credential(
                            config.getFullyQualifiedNamespace(),
                            config.getEventHubName(),
                            managedIdCred
                    );
                    break;

                case SERVICE_PRINCIPAL:
                    TokenCredential spCred = azureAuthService.getServicePrincipalCredential(
                            config.getTenantId(),
                            config.getClientId(),
                            config.getClientSecret()
                    );
                    clientBuilder.credential(
                            config.getFullyQualifiedNamespace(),
                            config.getEventHubName(),
                            spCred
                    );
                    break;

                case INTERACTIVE_BROWSER:
                    // Use a temporary ID for testing (will open browser for login)
                    String testConnectionId = "test-" + UUID.randomUUID().toString().substring(0, 8);
                    TokenCredential browserCred = azureAuthService.getInteractiveBrowserCredential(
                            testConnectionId,
                            config.getTenantId()
                    );
                    clientBuilder.credential(
                            config.getFullyQualifiedNamespace(),
                            config.getEventHubName(),
                            browserCred
                    );
                    log.info("Testing with interactive browser authentication (will open browser)");
                    break;
            }

            // Test by getting partition IDs using producer client
            try (var client = clientBuilder.buildProducerClient()) {
                var properties = client.getEventHubProperties();
                if (properties != null) {
                    List<String> partitions = new ArrayList<>();
                    for (String partitionId : properties.getPartitionIds()) {
                        partitions.add("Partition " + partitionId);
                    }
                    log.info("Event Hubs connection successful, found {} partitions", partitions.size());
                    return new ConnectionTestResult(true, "Connected successfully", partitions);
                }
            }

            return new ConnectionTestResult(false, "Failed to get Event Hub properties", Collections.emptyList());
        } catch (Exception e) {
            String message = extractErrorMessage(e);
            log.error("Event Hubs connection test failed: {}", message);
            return new ConnectionTestResult(false, message, Collections.emptyList());
        }
    }

    /**
     * Establishes an active connection (for streaming).
     */
    public void connect(String connectionId) {
        // TODO: Implement active connection management
        log.info("Connecting: {}", connectionId);
    }

    /**
     * Disconnects an active connection.
     */
    public void disconnect(String connectionId) {
        Object connection = activeConnections.remove(connectionId);
        if (connection != null) {
            // TODO: Close connection resources
            log.info("Disconnected: {}", connectionId);
        }
    }

    /**
     * Checks if a connection is active.
     */
    public boolean isConnected(String connectionId) {
        return activeConnections.containsKey(connectionId);
    }

    private String extractErrorMessage(Exception e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        if (message == null || message.isEmpty()) {
            message = cause.getClass().getSimpleName();
        }
        return message;
    }

    /**
     * Result of a connection test.
     */
    public static class ConnectionTestResult {
        private final boolean success;
        private final String message;
        private final List<String> details;

        public ConnectionTestResult(boolean success, String message, List<String> details) {
            this.success = success;
            this.message = message;
            this.details = details;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public List<String> getDetails() {
            return details;
        }
    }
}
