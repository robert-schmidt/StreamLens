package com.streamlens.model.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Unified message model for both Kafka and Event Hubs messages.
 */
public class StreamMessage {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private String id;
    private String connectionId;
    private String topic;
    private Integer partition;
    private Long offset;
    private String key;
    private byte[] value;
    private List<MessageHeader> headers;
    private Instant timestamp;
    private boolean starred;
    private boolean flagged;
    private Instant createdAt;

    // Source type for display purposes
    private SourceType sourceType;

    public StreamMessage() {
        this.id = UUID.randomUUID().toString();
        this.headers = new ArrayList<>();
        this.createdAt = Instant.now();
    }

    /**
     * Creates a StreamMessage from a Kafka ConsumerRecord.
     */
    public static StreamMessage fromKafkaRecord(
            String connectionId,
            String topic,
            int partition,
            long offset,
            String key,
            byte[] value,
            Instant timestamp,
            List<MessageHeader> headers) {

        StreamMessage msg = new StreamMessage();
        msg.setConnectionId(connectionId);
        msg.setTopic(topic);
        msg.setPartition(partition);
        msg.setOffset(offset);
        msg.setKey(key);
        msg.setValue(value);
        msg.setTimestamp(timestamp);
        msg.setHeaders(headers != null ? headers : new ArrayList<>());
        msg.setSourceType(SourceType.KAFKA);
        return msg;
    }

    /**
     * Creates a StreamMessage from an Azure Event Hub EventData.
     */
    public static StreamMessage fromEventData(
            String connectionId,
            String eventHubName,
            String partitionId,
            long sequenceNumber,
            byte[] body,
            Instant enqueuedTime,
            List<MessageHeader> properties) {

        StreamMessage msg = new StreamMessage();
        msg.setConnectionId(connectionId);
        msg.setTopic(eventHubName);
        msg.setPartition(partitionId != null ? Integer.parseInt(partitionId) : null);
        msg.setOffset(sequenceNumber);
        msg.setValue(body);
        msg.setTimestamp(enqueuedTime);
        msg.setHeaders(properties != null ? properties : new ArrayList<>());
        msg.setSourceType(SourceType.EVENTHUBS);
        return msg;
    }

    /**
     * Returns the message value as a UTF-8 string.
     */
    public String getValueAsString() {
        if (value == null) return "";
        return new String(value, StandardCharsets.UTF_8);
    }

    /**
     * Returns the message value as pretty-printed JSON if valid, otherwise raw string.
     */
    public String getValueAsPrettyJson() {
        String raw = getValueAsString();
        if (raw.isEmpty()) return "";

        try {
            // Try to parse and pretty-print as JSON
            Object json = OBJECT_MAPPER.readValue(raw, Object.class);
            return OBJECT_MAPPER.writeValueAsString(json);
        } catch (JsonProcessingException e) {
            // Not valid JSON, return as-is
            return raw;
        }
    }

    /**
     * Returns the message value as a hex dump.
     */
    public String getValueAsHex() {
        if (value == null || value.length == 0) return "";

        StringBuilder sb = new StringBuilder();
        StringBuilder ascii = new StringBuilder();

        for (int i = 0; i < value.length; i++) {
            if (i > 0 && i % 16 == 0) {
                sb.append("  ").append(ascii).append("\n");
                ascii = new StringBuilder();
            }
            if (i % 16 == 0) {
                sb.append(String.format("%08X  ", i));
            }

            sb.append(String.format("%02X ", value[i] & 0xFF));

            // Build ASCII representation
            char c = (char) (value[i] & 0xFF);
            ascii.append(c >= 32 && c < 127 ? c : '.');
        }

        // Pad last line if needed
        int remaining = 16 - (value.length % 16);
        if (remaining < 16) {
            for (int i = 0; i < remaining; i++) {
                sb.append("   ");
            }
        }
        sb.append("  ").append(ascii);

        return sb.toString();
    }

    // Getters and Setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public List<MessageHeader> getHeaders() {
        return headers;
    }

    public void setHeaders(List<MessageHeader> headers) {
        this.headers = headers;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isStarred() {
        return starred;
    }

    public void setStarred(boolean starred) {
        this.starred = starred;
    }

    public boolean isFlagged() {
        return flagged;
    }

    public void setFlagged(boolean flagged) {
        this.flagged = flagged;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public SourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }

    /**
     * Source type enumeration.
     */
    public enum SourceType {
        KAFKA,
        EVENTHUBS
    }
}
