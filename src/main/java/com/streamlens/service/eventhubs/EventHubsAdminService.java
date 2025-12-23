package com.streamlens.service.eventhubs;

import com.azure.core.credential.TokenCredential;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.EventHubProperties;
import com.azure.messaging.eventhubs.PartitionProperties;
import com.streamlens.model.connection.EventHubsConnectionConfig;
import com.streamlens.service.security.AzureAuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Service for Azure Event Hubs administrative operations.
 * Provides Event Hub metadata, partition info, and properties.
 *
 * Note: Consumer group management in Azure Event Hubs requires Azure Resource Manager
 * access (subscription-level permissions). Consumer groups should be created via
 * Azure Portal, CLI, or ARM templates.
 */
@Service
public class EventHubsAdminService {

    private static final Logger log = LoggerFactory.getLogger(EventHubsAdminService.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final AzureAuthService azureAuthService;

    public EventHubsAdminService(AzureAuthService azureAuthService) {
        this.azureAuthService = azureAuthService;
    }

    /**
     * Gets Event Hub properties including partition IDs.
     */
    public EventHubInfo getEventHubInfo(EventHubsConnectionConfig config) {
        try (EventHubProducerClient client = createProducerClient(config)) {
            EventHubProperties props = client.getEventHubProperties();

            EventHubInfo info = new EventHubInfo();
            info.setName(props.getName());
            info.setCreatedAt(props.getCreatedAt());
            List<String> partitionIds = new ArrayList<>();
            props.getPartitionIds().forEach(partitionIds::add);
            info.setPartitionIds(partitionIds);
            info.setConsumerGroup(config.getConsumerGroup());

            return info;
        } catch (Exception e) {
            log.error("Failed to get Event Hub info", e);
            throw new RuntimeException("Failed to get Event Hub info: " + e.getMessage(), e);
        }
    }

    /**
     * Gets detailed partition information.
     */
    public List<PartitionInfo> getPartitionInfo(EventHubsConnectionConfig config) {
        List<PartitionInfo> partitions = new ArrayList<>();

        try (EventHubConsumerAsyncClient client = createConsumerClient(config)) {
            EventHubProperties props = client.getEventHubProperties().block(TIMEOUT);

            if (props != null) {
                for (String partitionId : props.getPartitionIds()) {
                    PartitionProperties partProps = client.getPartitionProperties(partitionId).block(TIMEOUT);

                    if (partProps != null) {
                        PartitionInfo pInfo = new PartitionInfo();
                        pInfo.setPartitionId(partitionId);
                        pInfo.setBeginningSequenceNumber(partProps.getBeginningSequenceNumber());
                        pInfo.setLastEnqueuedSequenceNumber(partProps.getLastEnqueuedSequenceNumber());
                        pInfo.setLastEnqueuedOffset(partProps.getLastEnqueuedOffset());
                        pInfo.setLastEnqueuedTime(partProps.getLastEnqueuedTime());
                        pInfo.setEmpty(partProps.isEmpty());
                        partitions.add(pInfo);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to get partition info", e);
            throw new RuntimeException("Failed to get partition info: " + e.getMessage(), e);
        }

        return partitions;
    }

    /**
     * Gets the message count for a partition (approximation based on sequence numbers).
     */
    public long getApproximateMessageCount(EventHubsConnectionConfig config, String partitionId) {
        try (EventHubConsumerAsyncClient client = createConsumerClient(config)) {
            PartitionProperties props = client.getPartitionProperties(partitionId).block(TIMEOUT);

            if (props != null && !props.isEmpty()) {
                return props.getLastEnqueuedSequenceNumber() - props.getBeginningSequenceNumber() + 1;
            }
            return 0;
        } catch (Exception e) {
            log.error("Failed to get message count for partition {}", partitionId, e);
            return -1;
        }
    }

    private EventHubProducerClient createProducerClient(EventHubsConnectionConfig config) {
        EventHubClientBuilder builder = new EventHubClientBuilder();
        configureBuilder(builder, config);
        return builder.buildProducerClient();
    }

    private EventHubConsumerAsyncClient createConsumerClient(EventHubsConnectionConfig config) {
        EventHubClientBuilder builder = new EventHubClientBuilder()
                .consumerGroup(config.getConsumerGroup());
        configureBuilder(builder, config);
        return builder.buildAsyncConsumerClient();
    }

    private void configureBuilder(EventHubClientBuilder builder, EventHubsConnectionConfig config) {
        switch (config.getAuthenticationType()) {
            case CONNECTION_STRING:
                builder.connectionString(config.getConnectionString(), config.getEventHubName());
                break;

            case MANAGED_IDENTITY:
                TokenCredential managedIdCred = azureAuthService.getManagedIdentityCredential();
                builder.credential(config.getFullyQualifiedNamespace(), config.getEventHubName(), managedIdCred);
                break;

            case SERVICE_PRINCIPAL:
                TokenCredential spCred = azureAuthService.getServicePrincipalCredential(
                        config.getTenantId(),
                        config.getClientId(),
                        config.getClientSecret()
                );
                builder.credential(config.getFullyQualifiedNamespace(), config.getEventHubName(), spCred);
                break;

            case INTERACTIVE_BROWSER:
                TokenCredential browserCred = azureAuthService.getInteractiveBrowserCredential(
                        null, // No specific connection ID for admin
                        config.getTenantId()
                );
                builder.credential(config.getFullyQualifiedNamespace(), config.getEventHubName(), browserCred);
                break;
        }
    }

    // ============== Data Classes ==============

    public static class EventHubInfo {
        private String name;
        private Instant createdAt;
        private List<String> partitionIds;
        private String consumerGroup;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Instant getCreatedAt() { return createdAt; }
        public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
        public List<String> getPartitionIds() { return partitionIds; }
        public void setPartitionIds(List<String> partitionIds) { this.partitionIds = partitionIds; }
        public String getConsumerGroup() { return consumerGroup; }
        public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
    }

    public static class PartitionInfo {
        private String partitionId;
        private long beginningSequenceNumber;
        private long lastEnqueuedSequenceNumber;
        private String lastEnqueuedOffset;
        private Instant lastEnqueuedTime;
        private boolean empty;

        public String getPartitionId() { return partitionId; }
        public void setPartitionId(String partitionId) { this.partitionId = partitionId; }
        public long getBeginningSequenceNumber() { return beginningSequenceNumber; }
        public void setBeginningSequenceNumber(long beginningSequenceNumber) { this.beginningSequenceNumber = beginningSequenceNumber; }
        public long getLastEnqueuedSequenceNumber() { return lastEnqueuedSequenceNumber; }
        public void setLastEnqueuedSequenceNumber(long lastEnqueuedSequenceNumber) { this.lastEnqueuedSequenceNumber = lastEnqueuedSequenceNumber; }
        public String getLastEnqueuedOffset() { return lastEnqueuedOffset; }
        public void setLastEnqueuedOffset(String lastEnqueuedOffset) { this.lastEnqueuedOffset = lastEnqueuedOffset; }
        public Instant getLastEnqueuedTime() { return lastEnqueuedTime; }
        public void setLastEnqueuedTime(Instant lastEnqueuedTime) { this.lastEnqueuedTime = lastEnqueuedTime; }
        public boolean isEmpty() { return empty; }
        public void setEmpty(boolean empty) { this.empty = empty; }

        public long getMessageCount() {
            if (empty) return 0;
            return lastEnqueuedSequenceNumber - beginningSequenceNumber + 1;
        }
    }
}
