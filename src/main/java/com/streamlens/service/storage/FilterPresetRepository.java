package com.streamlens.service.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streamlens.model.filter.FilterPreset;
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
 * Repository for managing filter presets in SQLite.
 */
@Repository
public class FilterPresetRepository {

    private static final Logger log = LoggerFactory.getLogger(FilterPresetRepository.class);

    private final DatabaseService databaseService;
    private final ObjectMapper objectMapper;

    public FilterPresetRepository(DatabaseService databaseService) {
        this.databaseService = databaseService;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Saves a new filter preset.
     */
    public void save(FilterPreset preset) {
        try {
            String configJson = serializeConfig(preset);
            long now = Instant.now().toEpochMilli();

            databaseService.executeUpdate(
                    "INSERT INTO filter_presets (id, name, config, created_at, updated_at) " +
                            "VALUES (?, ?, ?, ?, ?)",
                    preset.getId(),
                    preset.getName(),
                    configJson,
                    now,
                    now
            );
            log.info("Saved filter preset: {}", preset.getName());
        } catch (Exception e) {
            log.error("Failed to save filter preset", e);
            throw new RuntimeException("Failed to save filter preset", e);
        }
    }

    /**
     * Updates an existing filter preset.
     */
    public void update(FilterPreset preset) {
        try {
            String configJson = serializeConfig(preset);
            long now = Instant.now().toEpochMilli();

            databaseService.executeUpdate(
                    "UPDATE filter_presets SET name = ?, config = ?, updated_at = ? WHERE id = ?",
                    preset.getName(),
                    configJson,
                    now,
                    preset.getId()
            );
            log.info("Updated filter preset: {}", preset.getName());
        } catch (Exception e) {
            log.error("Failed to update filter preset", e);
            throw new RuntimeException("Failed to update filter preset", e);
        }
    }

    /**
     * Saves or updates a filter preset.
     */
    public void saveOrUpdate(FilterPreset preset) {
        if (existsById(preset.getId())) {
            update(preset);
        } else {
            save(preset);
        }
    }

    /**
     * Deletes a filter preset by ID.
     */
    public void delete(String id) {
        try {
            databaseService.executeUpdate("DELETE FROM filter_presets WHERE id = ?", id);
            log.info("Deleted filter preset: {}", id);
        } catch (Exception e) {
            log.error("Failed to delete filter preset", e);
            throw new RuntimeException("Failed to delete filter preset", e);
        }
    }

    /**
     * Finds a filter preset by ID.
     */
    public Optional<FilterPreset> findById(String id) {
        try (ResultSet rs = databaseService.executeQuery(
                "SELECT * FROM filter_presets WHERE id = ?", id)) {
            if (rs.next()) {
                return Optional.of(mapRow(rs));
            }
            return Optional.empty();
        } catch (SQLException e) {
            log.error("Failed to find filter preset", e);
            throw new RuntimeException("Failed to find filter preset", e);
        }
    }

    /**
     * Finds a filter preset by name.
     */
    public Optional<FilterPreset> findByName(String name) {
        try (ResultSet rs = databaseService.executeQuery(
                "SELECT * FROM filter_presets WHERE name = ?", name)) {
            if (rs.next()) {
                return Optional.of(mapRow(rs));
            }
            return Optional.empty();
        } catch (SQLException e) {
            log.error("Failed to find filter preset by name", e);
            throw new RuntimeException("Failed to find filter preset by name", e);
        }
    }

    /**
     * Returns all filter presets ordered by name.
     */
    public List<FilterPreset> findAll() {
        List<FilterPreset> presets = new ArrayList<>();
        try (ResultSet rs = databaseService.executeQuery(
                "SELECT * FROM filter_presets ORDER BY name")) {
            while (rs.next()) {
                presets.add(mapRow(rs));
            }
        } catch (SQLException e) {
            log.error("Failed to list filter presets", e);
            throw new RuntimeException("Failed to list filter presets", e);
        }
        return presets;
    }

    /**
     * Checks if a preset exists by ID.
     */
    public boolean existsById(String id) {
        try (ResultSet rs = databaseService.executeQuery(
                "SELECT 1 FROM filter_presets WHERE id = ?", id)) {
            return rs.next();
        } catch (SQLException e) {
            log.error("Failed to check preset existence", e);
            throw new RuntimeException("Failed to check preset existence", e);
        }
    }

    /**
     * Checks if a preset exists by name.
     */
    public boolean existsByName(String name) {
        return findByName(name).isPresent();
    }

    private FilterPreset mapRow(ResultSet rs) throws SQLException {
        String configJson = rs.getString("config");
        FilterPreset preset = deserializeConfig(configJson);

        // Override ID and timestamps from database
        preset.setId(rs.getString("id"));
        preset.setName(rs.getString("name"));
        preset.setCreatedAt(Instant.ofEpochMilli(rs.getLong("created_at")));
        preset.setUpdatedAt(Instant.ofEpochMilli(rs.getLong("updated_at")));

        return preset;
    }

    private String serializeConfig(FilterPreset preset) {
        try {
            return objectMapper.writeValueAsString(preset);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize filter preset config", e);
        }
    }

    private FilterPreset deserializeConfig(String json) {
        try {
            return objectMapper.readValue(json, FilterPreset.class);
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize filter preset config", e);
            return new FilterPreset();
        }
    }
}
