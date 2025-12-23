package com.streamlens.service.streaming;

import com.azure.core.credential.TokenCredential;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.streamlens.model.connection.EventHubsConnectionConfig;
import com.streamlens.model.message.MessageHeader;
import com.streamlens.model.message.StreamMessage;
import com.streamlens.service.security.AzureAuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Service for streaming messages from Azure Event Hubs.
 */
@Service
public class EventHubsStreamingService {

    private static final Logger log = LoggerFactory.getLogger(EventHubsStreamingService.class);

    private final AzureAuthService azureAuthService;
    private final Map<String, StreamingSession> activeSessions = new ConcurrentHashMap<>();

    public EventHubsStreamingService(AzureAuthService azureAuthService) {
        this.azureAuthService = azureAuthService;
    }

    /**
     * Starts streaming from an Event Hub.
     */
    public String startStreaming(
            String connectionId,
            EventHubsConnectionConfig config,
            String partitionId,
            StartPosition startPosition,
            Consumer<StreamMessage> messageHandler) {

        String sessionId = UUID.randomUUID().toString();
        log.info("Starting Event Hubs streaming session {} for partition {}",
                sessionId, partitionId != null ? partitionId : "all");

        StreamingSession session = new StreamingSession(
                sessionId, connectionId, config, partitionId, startPosition, messageHandler
        );

        activeSessions.put(sessionId, session);
        session.start();

        return sessionId;
    }

    /**
     * Pauses a streaming session.
     */
    public void pauseStreaming(String sessionId) {
        StreamingSession session = activeSessions.get(sessionId);
        if (session != null) {
            session.pause();
            log.info("Paused streaming session: {}", sessionId);
        }
    }

    /**
     * Resumes a paused streaming session.
     */
    public void resumeStreaming(String sessionId) {
        StreamingSession session = activeSessions.get(sessionId);
        if (session != null) {
            session.resume();
            log.info("Resumed streaming session: {}", sessionId);
        }
    }

    /**
     * Stops a streaming session.
     */
    public void stopStreaming(String sessionId) {
        StreamingSession session = activeSessions.remove(sessionId);
        if (session != null) {
            session.stop();
            log.info("Stopped streaming session: {}", sessionId);
        }
    }

    /**
     * Stops all streaming sessions for a connection.
     */
    public void stopAllForConnection(String connectionId) {
        activeSessions.entrySet().removeIf(entry -> {
            if (entry.getValue().connectionId.equals(connectionId)) {
                entry.getValue().stop();
                return true;
            }
            return false;
        });
    }

    /**
     * Start position for streaming.
     */
    public enum StartPosition {
        EARLIEST,
        LATEST,
        FROM_SEQUENCE_NUMBER,
        FROM_ENQUEUED_TIME
    }

    /**
     * Internal streaming session.
     */
    private class StreamingSession {
        private final String sessionId;
        private final String connectionId;
        private final EventHubsConnectionConfig config;
        private final String partitionId;
        private final StartPosition startPosition;
        private final Consumer<StreamMessage> messageHandler;

        private EventHubConsumerAsyncClient client;
        private final List<Disposable> subscriptions = new ArrayList<>();
        private volatile boolean paused = false;

        StreamingSession(
                String sessionId,
                String connectionId,
                EventHubsConnectionConfig config,
                String partitionId,
                StartPosition startPosition,
                Consumer<StreamMessage> messageHandler) {
            this.sessionId = sessionId;
            this.connectionId = connectionId;
            this.config = config;
            this.partitionId = partitionId;
            this.startPosition = startPosition;
            this.messageHandler = messageHandler;
        }

        void start() {
            try {
                client = createClient();

                EventPosition position = getEventPosition();

                if (partitionId != null) {
                    // Stream from specific partition
                    Disposable subscription = client
                            .receiveFromPartition(partitionId, position)
                            .subscribe(
                                    this::handleEvent,
                                    error -> log.error("Event Hubs streaming error in session {}", sessionId, error),
                                    () -> log.info("Event Hubs streaming completed for session {}", sessionId)
                            );
                    subscriptions.add(subscription);
                } else {
                    // Stream from all partitions
                    client.getPartitionIds()
                            .flatMap(pid -> {
                                Disposable subscription = client
                                        .receiveFromPartition(pid, position)
                                        .subscribe(
                                                this::handleEvent,
                                                error -> log.error("Event Hubs streaming error for partition {}", pid, error)
                                        );
                                subscriptions.add(subscription);
                                return reactor.core.publisher.Mono.empty();
                            })
                            .subscribe();
                }

                log.info("Event Hubs streaming started for session {}", sessionId);
            } catch (Exception e) {
                log.error("Failed to start Event Hubs streaming session {}", sessionId, e);
                throw new RuntimeException("Failed to start Event Hubs streaming", e);
            }
        }

        void pause() {
            paused = true;
        }

        void resume() {
            paused = false;
        }

        void stop() {
            for (Disposable subscription : subscriptions) {
                if (!subscription.isDisposed()) {
                    subscription.dispose();
                }
            }
            subscriptions.clear();

            if (client != null) {
                client.close();
                client = null;
            }
        }

        private void handleEvent(PartitionEvent event) {
            if (paused) return;

            try {
                StreamMessage message = eventToMessage(event);
                messageHandler.accept(message);
            } catch (Exception e) {
                log.error("Error processing Event Hubs event", e);
            }
        }

        private StreamMessage eventToMessage(PartitionEvent partitionEvent) {
            var eventData = partitionEvent.getData();
            var context = partitionEvent.getPartitionContext();

            List<MessageHeader> headers = new ArrayList<>();
            if (eventData.getProperties() != null) {
                eventData.getProperties().forEach((key, value) ->
                        headers.add(new MessageHeader(key, value != null ? value.toString() : null))
                );
            }
            if (eventData.getSystemProperties() != null) {
                eventData.getSystemProperties().forEach((key, value) ->
                        headers.add(new MessageHeader("sys:" + key, value != null ? value.toString() : null))
                );
            }

            return StreamMessage.fromEventData(
                    connectionId,
                    context.getEventHubName(),
                    context.getPartitionId(),
                    eventData.getSequenceNumber(),
                    eventData.getBody(),
                    eventData.getEnqueuedTime(),
                    headers
            );
        }

        private EventHubConsumerAsyncClient createClient() {
            EventHubClientBuilder builder = new EventHubClientBuilder()
                    .consumerGroup(config.getConsumerGroup());

            switch (config.getAuthenticationType()) {
                case CONNECTION_STRING:
                    builder.connectionString(
                            config.getConnectionString(),
                            config.getEventHubName()
                    );
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
                    // Uses interactive browser login with token caching
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

            return builder.buildAsyncConsumerClient();
        }

        private EventPosition getEventPosition() {
            switch (startPosition) {
                case EARLIEST:
                    return EventPosition.earliest();
                case LATEST:
                default:
                    return EventPosition.latest();
            }
        }
    }
}
