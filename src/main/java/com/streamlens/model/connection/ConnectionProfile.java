package com.streamlens.model.connection;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents a saved connection profile for Kafka or Azure Event Hubs.
 */
public class ConnectionProfile {

    private String id;
    private String name;
    private ConnectionType type;
    private String encryptedConfig;
    private Instant createdAt;
    private Instant updatedAt;

    // Transient - decrypted configuration (not persisted)
    private transient Object config;

    public ConnectionProfile() {
        this.id = UUID.randomUUID().toString();
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public ConnectionProfile(String name, ConnectionType type) {
        this();
        this.name = name;
        this.type = type;
    }

    // Getters and Setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ConnectionType getType() {
        return type;
    }

    public void setType(ConnectionType type) {
        this.type = type;
    }

    public String getEncryptedConfig() {
        return encryptedConfig;
    }

    public void setEncryptedConfig(String encryptedConfig) {
        this.encryptedConfig = encryptedConfig;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Object getConfig() {
        return config;
    }

    public void setConfig(Object config) {
        this.config = config;
    }

    /**
     * Gets the typed Kafka configuration.
     */
    @SuppressWarnings("unchecked")
    public KafkaConnectionConfig getKafkaConfig() {
        if (type != ConnectionType.KAFKA) {
            throw new IllegalStateException("Connection is not Kafka type");
        }
        return (KafkaConnectionConfig) config;
    }

    /**
     * Gets the typed Event Hubs configuration.
     */
    @SuppressWarnings("unchecked")
    public EventHubsConnectionConfig getEventHubsConfig() {
        if (type != ConnectionType.EVENTHUBS) {
            throw new IllegalStateException("Connection is not Event Hubs type");
        }
        return (EventHubsConnectionConfig) config;
    }

    @Override
    public String toString() {
        return name + " (" + type + ")";
    }

    /**
     * Connection type enumeration.
     */
    public enum ConnectionType {
        KAFKA,
        EVENTHUBS
    }
}
