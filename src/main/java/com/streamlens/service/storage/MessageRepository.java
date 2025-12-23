package com.streamlens.service.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamlens.model.message.MessageHeader;
import com.streamlens.model.message.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Repository for managing persisted messages in SQLite.
 */
@Repository
public class MessageRepository {

    private static final Logger log = LoggerFactory.getLogger(MessageRepository.class);

    private final DatabaseService databaseService;
    private final ObjectMapper objectMapper;

    public MessageRepository(DatabaseService databaseService) {
        this.databaseService = databaseService;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
    }

    /**
     * Saves a message to the database.
     */
    public void save(StreamMessage message) {
        try {
            String headersJson = serializeHeaders(message.getHeaders());
            long now = Instant.now().toEpochMilli();

            databaseService.executeUpdate(
                    "INSERT OR REPLACE INTO messages " +
                            "(id, connection_id, topic, partition, offset_value, message_key, " +
                            "message_value, headers, event_timestamp, flagged, starred, created_at) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    message.getId(),
                    message.getConnectionId(),
                    message.getTopic(),
                    message.getPartition(),
                    message.getOffset(),
                    message.getKey(),
                    message.getValue(),
                    headersJson,
                    message.getTimestamp() != null ? message.getTimestamp().toEpochMilli() : null,
                    message.isFlagged() ? 1 : 0,
                    message.isStarred() ? 1 : 0,
                    now
            );
            log.debug("Saved message: {} to topic {}", message.getId(), message.getTopic());
        } catch (Exception e) {
            log.error("Failed to save message", e);
            throw new RuntimeException("Failed to save message", e);
        }
    }

    /**
     * Updates the starred/flagged status of a message.
     */
    public void updateFlags(String messageId, boolean starred, boolean flagged) {
        try {
            databaseService.executeUpdate(
                    "UPDATE messages SET starred = ?, flagged = ? WHERE id = ?",
                    starred ? 1 : 0,
                    flagged ? 1 : 0,
                    messageId
            );
        } catch (Exception e) {
            log.error("Failed to update message flags", e);
            throw new RuntimeException("Failed to update message flags", e);
        }
    }

    /**
     * Deletes a message by ID.
     */
    public void delete(String id) {
        try {
            databaseService.executeUpdate("DELETE FROM messages WHERE id = ?", id);
            log.debug("Deleted message: {}", id);
        } catch (Exception e) {
            log.error("Failed to delete message", e);
            throw new RuntimeException("Failed to delete message", e);
        }
    }

    /**
     * Deletes all messages for a connection.
     */
    public void deleteByConnectionId(String connectionId) {
        try {
            databaseService.executeUpdate("DELETE FROM messages WHERE connection_id = ?", connectionId);
            log.info("Deleted all messages for connection: {}", connectionId);
        } catch (Exception e) {
            log.error("Failed to delete messages for connection", e);
            throw new RuntimeException("Failed to delete messages for connection", e);
        }
    }

    /**
     * Finds a message by ID.
     */
    public Optional<StreamMessage> findById(String id) {
        try (ResultSet rs = databaseService.executeQuery(
                "SELECT * FROM messages WHERE id = ?", id)) {
            if (rs.next()) {
                return Optional.of(mapRow(rs));
            }
            return Optional.empty();
        } catch (SQLException e) {
            log.error("Failed to find message", e);
            throw new RuntimeException("Failed to find message", e);
        }
    }

    /**
     * Finds all starred messages.
     */
    public List<StreamMessage> findStarred() {
        return findByQuery(
                "SELECT * FROM messages WHERE starred = 1 ORDER BY event_timestamp DESC"
        );
    }

    /**
     * Finds all flagged messages.
     */
    public List<StreamMessage> findFlagged() {
        return findByQuery(
                "SELECT * FROM messages WHERE flagged = 1 ORDER BY event_timestamp DESC"
        );
    }

    /**
     * Finds all saved messages (starred or flagged).
     */
    public List<StreamMessage> findSaved() {
        return findByQuery(
                "SELECT * FROM messages WHERE starred = 1 OR flagged = 1 " +
                        "ORDER BY event_timestamp DESC"
        );
    }

    /**
     * Finds messages by connection ID.
     */
    public List<StreamMessage> findByConnectionId(String connectionId) {
        return findByQuery(
                "SELECT * FROM messages WHERE connection_id = ? ORDER BY event_timestamp DESC",
                connectionId
        );
    }

    /**
     * Finds messages by connection ID and topic.
     */
    public List<StreamMessage> findByConnectionAndTopic(String connectionId, String topic) {
        return findByQuery(
                "SELECT * FROM messages WHERE connection_id = ? AND topic = ? " +
                        "ORDER BY event_timestamp DESC",
                connectionId,
                topic
        );
    }

    /**
     * Finds messages within a time range.
     */
    public List<StreamMessage> findByTimeRange(
            String connectionId,
            String topic,
            Instant startTime,
            Instant endTime) {
        return findByQuery(
                "SELECT * FROM messages WHERE connection_id = ? AND topic = ? " +
                        "AND event_timestamp >= ? AND event_timestamp <= ? " +
                        "ORDER BY event_timestamp DESC",
                connectionId,
                topic,
                startTime.toEpochMilli(),
                endTime.toEpochMilli()
        );
    }

    /**
     * Returns all saved messages.
     */
    public List<StreamMessage> findAll() {
        return findByQuery("SELECT * FROM messages ORDER BY event_timestamp DESC");
    }

    /**
     * Counts all saved messages.
     */
    public int count() {
        try (ResultSet rs = databaseService.executeQuery("SELECT COUNT(*) FROM messages")) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        } catch (SQLException e) {
            log.error("Failed to count messages", e);
            throw new RuntimeException("Failed to count messages", e);
        }
    }

    /**
     * Checks if a message exists by ID.
     */
    public boolean existsById(String id) {
        try (ResultSet rs = databaseService.executeQuery(
                "SELECT 1 FROM messages WHERE id = ?", id)) {
            return rs.next();
        } catch (SQLException e) {
            log.error("Failed to check message existence", e);
            throw new RuntimeException("Failed to check message existence", e);
        }
    }

    private List<StreamMessage> findByQuery(String sql, Object... params) {
        List<StreamMessage> messages = new ArrayList<>();
        try (ResultSet rs = databaseService.executeQuery(sql, params)) {
            while (rs.next()) {
                messages.add(mapRow(rs));
            }
        } catch (SQLException e) {
            log.error("Failed to query messages", e);
            throw new RuntimeException("Failed to query messages", e);
        }
        return messages;
    }

    private StreamMessage mapRow(ResultSet rs) throws SQLException {
        StreamMessage message = new StreamMessage();
        message.setId(rs.getString("id"));
        message.setConnectionId(rs.getString("connection_id"));
        message.setTopic(rs.getString("topic"));

        int partition = rs.getInt("partition");
        if (!rs.wasNull()) {
            message.setPartition(partition);
        }

        long offset = rs.getLong("offset_value");
        if (!rs.wasNull()) {
            message.setOffset(offset);
        }

        message.setKey(rs.getString("message_key"));
        message.setValue(rs.getBytes("message_value"));

        String headersJson = rs.getString("headers");
        if (headersJson != null && !headersJson.isEmpty()) {
            message.setHeaders(deserializeHeaders(headersJson));
        }

        long timestamp = rs.getLong("event_timestamp");
        if (!rs.wasNull()) {
            message.setTimestamp(Instant.ofEpochMilli(timestamp));
        }

        message.setFlagged(rs.getInt("flagged") == 1);
        message.setStarred(rs.getInt("starred") == 1);

        long createdAt = rs.getLong("created_at");
        if (!rs.wasNull()) {
            message.setCreatedAt(Instant.ofEpochMilli(createdAt));
        }

        return message;
    }

    private String serializeHeaders(List<MessageHeader> headers) {
        if (headers == null || headers.isEmpty()) {
            return "[]";
        }
        try {
            return objectMapper.writeValueAsString(headers);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize headers", e);
            return "[]";
        }
    }

    private List<MessageHeader> deserializeHeaders(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<List<MessageHeader>>() {});
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize headers", e);
            return new ArrayList<>();
        }
    }
}
