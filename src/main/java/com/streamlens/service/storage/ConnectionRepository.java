package com.streamlens.service.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.model.connection.ConnectionProfile.ConnectionType;
import com.streamlens.model.connection.EventHubsConnectionConfig;
import com.streamlens.model.connection.KafkaConnectionConfig;
import com.streamlens.service.security.EncryptionService;
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
 * Repository for managing connection profiles in SQLite.
 */
@Repository
public class ConnectionRepository {

    private static final Logger log = LoggerFactory.getLogger(ConnectionRepository.class);

    private final DatabaseService databaseService;
    private final EncryptionService encryptionService;
    private final ObjectMapper objectMapper;

    public ConnectionRepository(DatabaseService databaseService, EncryptionService encryptionService) {
        this.databaseService = databaseService;
        this.encryptionService = encryptionService;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
    }

    /**
     * Saves a new connection profile.
     */
    public void save(ConnectionProfile profile) {
        try {
            String encryptedConfig = encryptConfig(profile);
            profile.setEncryptedConfig(encryptedConfig);

            long now = Instant.now().toEpochMilli();
            databaseService.executeUpdate(
                    "INSERT INTO connections (id, name, type, config_encrypted, created_at, updated_at) " +
                            "VALUES (?, ?, ?, ?, ?, ?)",
                    profile.getId(),
                    profile.getName(),
                    profile.getType().name(),
                    encryptedConfig,
                    now,
                    now
            );
            log.info("Saved connection profile: {}", profile.getName());
        } catch (Exception e) {
            log.error("Failed to save connection profile", e);
            throw new RuntimeException("Failed to save connection profile", e);
        }
    }

    /**
     * Updates an existing connection profile.
     */
    public void update(ConnectionProfile profile) {
        try {
            String encryptedConfig = encryptConfig(profile);
            profile.setEncryptedConfig(encryptedConfig);

            long now = Instant.now().toEpochMilli();
            databaseService.executeUpdate(
                    "UPDATE connections SET name = ?, type = ?, config_encrypted = ?, updated_at = ? " +
                            "WHERE id = ?",
                    profile.getName(),
                    profile.getType().name(),
                    encryptedConfig,
                    now,
                    profile.getId()
            );
            log.info("Updated connection profile: {}", profile.getName());
        } catch (Exception e) {
            log.error("Failed to update connection profile", e);
            throw new RuntimeException("Failed to update connection profile", e);
        }
    }

    /**
     * Deletes a connection profile by ID.
     */
    public void delete(String id) {
        try {
            databaseService.executeUpdate("DELETE FROM connections WHERE id = ?", id);
            log.info("Deleted connection profile: {}", id);
        } catch (Exception e) {
            log.error("Failed to delete connection profile", e);
            throw new RuntimeException("Failed to delete connection profile", e);
        }
    }

    /**
     * Finds a connection profile by ID.
     */
    public Optional<ConnectionProfile> findById(String id) {
        try (ResultSet rs = databaseService.executeQuery(
                "SELECT * FROM connections WHERE id = ?", id)) {
            if (rs.next()) {
                return Optional.of(mapRow(rs));
            }
            return Optional.empty();
        } catch (SQLException e) {
            log.error("Failed to find connection profile", e);
            throw new RuntimeException("Failed to find connection profile", e);
        }
    }

    /**
     * Finds a connection profile by name.
     */
    public Optional<ConnectionProfile> findByName(String name) {
        try (ResultSet rs = databaseService.executeQuery(
                "SELECT * FROM connections WHERE name = ?", name)) {
            if (rs.next()) {
                return Optional.of(mapRow(rs));
            }
            return Optional.empty();
        } catch (SQLException e) {
            log.error("Failed to find connection profile by name", e);
            throw new RuntimeException("Failed to find connection profile by name", e);
        }
    }

    /**
     * Returns all connection profiles.
     */
    public List<ConnectionProfile> findAll() {
        List<ConnectionProfile> profiles = new ArrayList<>();
        try (ResultSet rs = databaseService.executeQuery(
                "SELECT * FROM connections ORDER BY name")) {
            while (rs.next()) {
                profiles.add(mapRow(rs));
            }
        } catch (SQLException e) {
            log.error("Failed to list connection profiles", e);
            throw new RuntimeException("Failed to list connection profiles", e);
        }
        return profiles;
    }

    /**
     * Checks if a connection with the given name exists.
     */
    public boolean existsByName(String name) {
        return findByName(name).isPresent();
    }

    private ConnectionProfile mapRow(ResultSet rs) throws SQLException {
        ConnectionProfile profile = new ConnectionProfile();
        profile.setId(rs.getString("id"));
        profile.setName(rs.getString("name"));
        profile.setType(ConnectionType.valueOf(rs.getString("type")));
        profile.setEncryptedConfig(rs.getString("config_encrypted"));
        profile.setCreatedAt(Instant.ofEpochMilli(rs.getLong("created_at")));
        profile.setUpdatedAt(Instant.ofEpochMilli(rs.getLong("updated_at")));

        // Decrypt and set config
        decryptConfig(profile);

        return profile;
    }

    private String encryptConfig(ConnectionProfile profile) {
        try {
            Object config = profile.getConfig();
            if (config == null) {
                throw new IllegalArgumentException("Connection config is null");
            }
            String json = objectMapper.writeValueAsString(config);
            return encryptionService.encrypt(json, encryptionService.getMachineKey());
        } catch (Exception e) {
            throw new RuntimeException("Failed to encrypt connection config", e);
        }
    }

    private void decryptConfig(ConnectionProfile profile) {
        try {
            String json = encryptionService.decrypt(
                    profile.getEncryptedConfig(),
                    encryptionService.getMachineKey()
            );

            Class<?> configClass = profile.getType() == ConnectionType.KAFKA
                    ? KafkaConnectionConfig.class
                    : EventHubsConnectionConfig.class;

            Object config = objectMapper.readValue(json, configClass);
            profile.setConfig(config);
        } catch (Exception e) {
            log.error("Failed to decrypt connection config for: {}", profile.getName());
            throw new RuntimeException("Failed to decrypt connection config", e);
        }
    }
}
