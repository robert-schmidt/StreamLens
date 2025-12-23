package com.streamlens.service.streaming;

import com.streamlens.model.connection.KafkaConnectionConfig;
import com.streamlens.model.message.MessageHeader;
import com.streamlens.model.message.StreamMessage;
import com.streamlens.model.streaming.StreamingOptions;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Service for streaming messages from Apache Kafka topics.
 */
@Service
public class KafkaStreamingService {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamingService.class);

    private final int pollTimeoutMs;
    private final Map<String, StreamingSession> activeSessions = new HashMap<>();

    public KafkaStreamingService(
            @Value("${streamlens.streaming.poll-timeout-ms:100}") int pollTimeoutMs) {
        this.pollTimeoutMs = pollTimeoutMs;
    }

    /**
     * Starts streaming from a topic with simple start position.
     */
    public String startStreaming(
            String connectionId,
            KafkaConnectionConfig config,
            String topic,
            Integer partition,
            StartPosition startPosition,
            Consumer<StreamMessage> messageHandler) {

        StreamingOptions options = new StreamingOptions();
        options.setStartPosition(startPosition == StartPosition.EARLIEST
                ? StreamingOptions.StartPosition.EARLIEST
                : StreamingOptions.StartPosition.LATEST);

        return startStreaming(connectionId, config, topic, partition, options, messageHandler);
    }

    /**
     * Starts streaming from a topic with full streaming options.
     */
    public String startStreaming(
            String connectionId,
            KafkaConnectionConfig config,
            String topic,
            Integer partition,
            StreamingOptions options,
            Consumer<StreamMessage> messageHandler) {

        String sessionId = UUID.randomUUID().toString();
        log.info("Starting Kafka streaming session {} for topic {} partition {} with options: position={}, offset={}, timestamp={}",
                sessionId, topic, partition, options.getStartPosition(),
                options.getSpecificOffset(), options.getSpecificTimestamp());

        StreamingSession session = new StreamingSession(
                sessionId, connectionId, config, topic, partition, options, messageHandler
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
     * Fetches historical messages from a topic.
     */
    public List<StreamMessage> fetchHistorical(
            String connectionId,
            KafkaConnectionConfig config,
            String topic,
            Integer partition,
            long fromOffset,
            int maxMessages) {

        log.info("Fetching historical messages from {} partition {} offset {}",
                topic, partition, fromOffset);

        List<StreamMessage> messages = new ArrayList<>();
        Map<String, Object> props = config.toKafkaProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "streamlens-historical-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition != null ? partition : 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.seek(tp, fromOffset);

            while (messages.size() < maxMessages) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) break;

                for (ConsumerRecord<String, byte[]> record : records) {
                    messages.add(recordToMessage(connectionId, record));
                    if (messages.size() >= maxMessages) break;
                }
            }
        } catch (Exception e) {
            log.error("Failed to fetch historical messages", e);
            throw new RuntimeException("Failed to fetch historical messages", e);
        }

        log.info("Fetched {} historical messages", messages.size());
        return messages;
    }

    private StreamMessage recordToMessage(String connectionId, ConsumerRecord<String, byte[]> record) {
        List<MessageHeader> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            String value = header.value() != null
                    ? new String(header.value(), StandardCharsets.UTF_8)
                    : null;
            headers.add(new MessageHeader(header.key(), value));
        }

        return StreamMessage.fromKafkaRecord(
                connectionId,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                Instant.ofEpochMilli(record.timestamp()),
                headers
        );
    }

    /**
     * Start position for streaming.
     */
    public enum StartPosition {
        EARLIEST,
        LATEST,
        FROM_OFFSET,
        FROM_TIMESTAMP
    }

    /**
     * Internal streaming session.
     */
    private class StreamingSession {
        private final String sessionId;
        private final String connectionId;
        private final KafkaConnectionConfig config;
        private final String topic;
        private final Integer partition;
        private final StreamingOptions options;
        private final Consumer<StreamMessage> messageHandler;

        private Thread thread;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final AtomicBoolean paused = new AtomicBoolean(false);
        private final AtomicInteger messageCount = new AtomicInteger(0);

        StreamingSession(
                String sessionId,
                String connectionId,
                KafkaConnectionConfig config,
                String topic,
                Integer partition,
                StreamingOptions options,
                Consumer<StreamMessage> messageHandler) {
            this.sessionId = sessionId;
            this.connectionId = connectionId;
            this.config = config;
            this.topic = topic;
            this.partition = partition;
            this.options = options;
            this.messageHandler = messageHandler;
        }

        void start() {
            running.set(true);
            thread = new Thread(this::run, "kafka-streaming-" + sessionId);
            thread.setDaemon(true);
            thread.start();
        }

        void pause() {
            paused.set(true);
        }

        void resume() {
            paused.set(false);
        }

        void stop() {
            running.set(false);
            if (thread != null) {
                thread.interrupt();
            }
        }

        private void run() {
            Map<String, Object> props = config.toKafkaProperties();
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

            // Use custom consumer group if provided, otherwise generate one
            String groupId = options.getConsumerGroup() != null && !options.getConsumerGroup().isEmpty()
                    ? options.getConsumerGroup()
                    : "streamlens-streaming-" + sessionId;
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            // Set auto offset reset based on start position
            String autoReset = options.getStartPosition() == StreamingOptions.StartPosition.EARLIEST
                    ? "earliest" : "latest";
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoReset);

            // Disable auto commit - we'll commit manually if requested
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
                List<TopicPartition> assignedPartitions;

                if (partition != null) {
                    // Assign specific partition
                    TopicPartition tp = new TopicPartition(topic, partition);
                    assignedPartitions = Collections.singletonList(tp);
                } else {
                    // Get all partitions and assign directly (better for browsing than subscribe)
                    List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                    if (partitionInfos == null || partitionInfos.isEmpty()) {
                        log.warn("No partitions found for topic: {}", topic);
                        return;
                    }
                    assignedPartitions = new ArrayList<>();
                    for (PartitionInfo info : partitionInfos) {
                        assignedPartitions.add(new TopicPartition(topic, info.partition()));
                    }
                }

                // Assign partitions directly (not subscribe) for immediate control
                consumer.assign(assignedPartitions);
                log.info("Assigned {} partitions for topic {}", assignedPartitions.size(), topic);

                // Seek to appropriate position based on options
                seekToPosition(consumer, assignedPartitions);

                // Main poll loop
                while (running.get()) {
                    if (paused.get()) {
                        Thread.sleep(100);
                        continue;
                    }

                    ConsumerRecords<String, byte[]> records =
                            consumer.poll(Duration.ofMillis(pollTimeoutMs));

                    for (ConsumerRecord<String, byte[]> record : records) {
                        if (!running.get()) break;

                        // Check max messages limit
                        if (options.getMaxMessages() > 0 &&
                                messageCount.get() >= options.getMaxMessages()) {
                            log.info("Reached max messages limit ({}), stopping", options.getMaxMessages());
                            running.set(false);
                            break;
                        }

                        StreamMessage message = recordToMessage(connectionId, record);
                        messageHandler.accept(message);
                        messageCount.incrementAndGet();

                        // Commit offsets if requested
                        if (options.isCommitOffsets()) {
                            consumer.commitSync(Collections.singletonMap(
                                    new TopicPartition(record.topic(), record.partition()),
                                    new OffsetAndMetadata(record.offset() + 1)
                            ));
                        }
                    }
                }
            } catch (Exception e) {
                if (running.get()) {
                    log.error("Kafka streaming error in session {}", sessionId, e);
                }
            }
            log.info("Kafka streaming session {} ended, processed {} messages", sessionId, messageCount.get());
        }

        private void seekToPosition(KafkaConsumer<String, byte[]> consumer, List<TopicPartition> partitions) {
            switch (options.getStartPosition()) {
                case EARLIEST:
                    consumer.seekToBeginning(partitions);
                    log.debug("Seeking to beginning for partitions: {}", partitions);
                    break;

                case LATEST:
                    consumer.seekToEnd(partitions);
                    log.debug("Seeking to end for partitions: {}", partitions);
                    break;

                case FROM_OFFSET:
                    if (options.getSpecificOffset() != null) {
                        for (TopicPartition tp : partitions) {
                            consumer.seek(tp, options.getSpecificOffset());
                        }
                        log.debug("Seeking to offset {} for partitions: {}", options.getSpecificOffset(), partitions);
                    }
                    break;

                case FROM_TIMESTAMP:
                    if (options.getSpecificTimestamp() != null) {
                        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
                        for (TopicPartition tp : partitions) {
                            timestampsToSearch.put(tp, options.getSpecificTimestamp().toEpochMilli());
                        }

                        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                                consumer.offsetsForTimes(timestampsToSearch);

                        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
                            if (entry.getValue() != null) {
                                consumer.seek(entry.getKey(), entry.getValue().offset());
                                log.debug("Seeking partition {} to offset {} (timestamp {})",
                                        entry.getKey(), entry.getValue().offset(), options.getSpecificTimestamp());
                            } else {
                                // No messages at or after timestamp, seek to end
                                consumer.seekToEnd(Collections.singletonList(entry.getKey()));
                            }
                        }
                    }
                    break;
            }
        }
    }
}
