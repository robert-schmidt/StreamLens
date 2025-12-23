package com.streamlens.service.browser;

import com.azure.core.credential.TokenCredential;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.model.connection.EventHubsConnectionConfig;
import com.streamlens.model.connection.KafkaConnectionConfig;
import com.streamlens.service.security.AzureAuthService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Service for discovering topics and partitions from Kafka and Event Hubs.
 */
@Service
public class TopicBrowserService {

    private static final Logger log = LoggerFactory.getLogger(TopicBrowserService.class);
    private static final int TIMEOUT_SECONDS = 30;

    private final AzureAuthService azureAuthService;

    public TopicBrowserService(AzureAuthService azureAuthService) {
        this.azureAuthService = azureAuthService;
    }

    /**
     * Gets all topics with their partition counts for a connection.
     */
    public Map<String, Integer> getTopicsWithPartitions(ConnectionProfile profile) {
        if (profile.getType() == ConnectionProfile.ConnectionType.KAFKA) {
            return getKafkaTopics(profile.getKafkaConfig());
        } else {
            return getEventHubsTopics(profile.getId(), profile.getEventHubsConfig());
        }
    }

    /**
     * Gets Kafka topics with partition counts.
     */
    private Map<String, Integer> getKafkaTopics(KafkaConnectionConfig config) {
        Map<String, Integer> result = new LinkedHashMap<>();

        Map<String, Object> props = config.toKafkaProperties();
        props.put("request.timeout.ms", TIMEOUT_SECONDS * 1000);
        props.put("default.api.timeout.ms", TIMEOUT_SECONDS * 1000);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Get topic names
            ListTopicsResult listResult = adminClient.listTopics();
            Set<String> topicNames = listResult.names().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (topicNames.isEmpty()) {
                return result;
            }

            // Get topic descriptions for partition counts
            DescribeTopicsResult describeResult = adminClient.describeTopics(topicNames);
            Map<String, TopicDescription> descriptions = describeResult.allTopicNames()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // Sort topics alphabetically
            List<String> sortedTopics = new ArrayList<>(topicNames);
            Collections.sort(sortedTopics);

            for (String topic : sortedTopics) {
                TopicDescription desc = descriptions.get(topic);
                if (desc != null) {
                    result.put(topic, desc.partitions().size());
                }
            }

            log.info("Found {} Kafka topics", result.size());
        } catch (Exception e) {
            log.error("Failed to get Kafka topics", e);
            throw new RuntimeException("Failed to get Kafka topics: " + e.getMessage(), e);
        }

        return result;
    }

    /**
     * Gets Event Hubs topics (just the configured Event Hub) with partition counts.
     */
    private Map<String, Integer> getEventHubsTopics(String connectionId, EventHubsConnectionConfig config) {
        Map<String, Integer> result = new LinkedHashMap<>();

        try {
            EventHubClientBuilder builder = new EventHubClientBuilder();

            switch (config.getAuthenticationType()) {
                case CONNECTION_STRING:
                    builder.connectionString(config.getConnectionString(), config.getEventHubName());
                    break;
                case MANAGED_IDENTITY:
                    TokenCredential managedIdCred = azureAuthService.getManagedIdentityCredential();
                    builder.credential(
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
                    builder.credential(
                            config.getFullyQualifiedNamespace(),
                            config.getEventHubName(),
                            spCred
                    );
                    break;
                case INTERACTIVE_BROWSER:
                    TokenCredential browserCred = azureAuthService.getInteractiveBrowserCredential(
                            connectionId,
                            config.getTenantId()
                    );
                    builder.credential(
                            config.getFullyQualifiedNamespace(),
                            config.getEventHubName(),
                            browserCred
                    );
                    log.info("Using interactive browser authentication for connection: {}", connectionId);
                    break;
            }

            try (EventHubProducerClient client = builder.buildProducerClient()) {
                var properties = client.getEventHubProperties();
                String hubName = properties.getName();
                int partitionCount = properties.getPartitionIds().stream().toList().size();
                result.put(hubName, partitionCount);
                log.info("Found Event Hub '{}' with {} partitions", hubName, partitionCount);
            }
        } catch (Exception e) {
            log.error("Failed to get Event Hubs topics", e);
            throw new RuntimeException("Failed to get Event Hubs info: " + e.getMessage(), e);
        }

        return result;
    }

    /**
     * Result class for topic information.
     */
    public static class TopicInfo {
        private final String name;
        private final int partitionCount;

        public TopicInfo(String name, int partitionCount) {
            this.name = name;
            this.partitionCount = partitionCount;
        }

        public String getName() {
            return name;
        }

        public int getPartitionCount() {
            return partitionCount;
        }
    }
}
