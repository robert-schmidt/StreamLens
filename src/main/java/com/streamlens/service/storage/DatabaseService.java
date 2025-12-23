package com.streamlens.service.storage;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

/**
 * Manages SQLite database connections and provides utility methods for database operations.
 */
@Service
public class DatabaseService {

    private static final Logger log = LoggerFactory.getLogger(DatabaseService.class);

    private final String databasePath;
    private Connection connection;

    public DatabaseService(@Value("${streamlens.database.path}") String databasePath) {
        this.databasePath = databasePath;
    }

    @PostConstruct
    public void initialize() {
        try {
            // Ensure parent directory exists
            Path dbPath = Paths.get(databasePath);
            Files.createDirectories(dbPath.getParent());

            // Connect to SQLite
            String jdbcUrl = "jdbc:sqlite:" + databasePath;
            connection = DriverManager.getConnection(jdbcUrl);

            // Enable foreign keys
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("PRAGMA foreign_keys = ON");
            }

            // Initialize schema
            initializeSchema();

            log.info("Database initialized at: {}", databasePath);
        } catch (Exception e) {
            log.error("Failed to initialize database", e);
            throw new DatabaseException("Failed to initialize database", e);
        }
    }

    private void initializeSchema() throws Exception {
        ClassPathResource resource = new ClassPathResource("schema.sql");
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream()))) {
            String schema = reader.lines().collect(Collectors.joining("\n"));

            // Split by semicolons and execute each statement
            for (String statement : schema.split(";")) {
                // Remove comment lines from the statement
                String cleaned = removeComments(statement);
                if (!cleaned.isEmpty()) {
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute(cleaned);
                    }
                }
            }
        }
        log.info("Database schema initialized");
    }

    private String removeComments(String sql) {
        StringBuilder result = new StringBuilder();
        for (String line : sql.split("\n")) {
            String trimmedLine = line.trim();
            // Skip pure comment lines
            if (!trimmedLine.startsWith("--") && !trimmedLine.isEmpty()) {
                // Remove inline comments
                int commentIndex = line.indexOf("--");
                if (commentIndex > 0) {
                    result.append(line.substring(0, commentIndex).trim());
                } else {
                    result.append(line);
                }
                result.append("\n");
            }
        }
        return result.toString().trim();
    }

    @PreDestroy
    public void shutdown() {
        if (connection != null) {
            try {
                connection.close();
                log.info("Database connection closed");
            } catch (SQLException e) {
                log.error("Error closing database connection", e);
            }
        }
    }

    /**
     * Gets the active database connection.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Executes a query and returns the result set.
     * Caller is responsible for closing the PreparedStatement.
     */
    public ResultSet executeQuery(String sql, Object... params) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        setParameters(stmt, params);
        return stmt.executeQuery();
    }

    /**
     * Executes an update (INSERT, UPDATE, DELETE) and returns the number of affected rows.
     */
    public int executeUpdate(String sql, Object... params) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            return stmt.executeUpdate();
        }
    }

    /**
     * Executes an insert and returns the generated key.
     */
    public String executeInsert(String sql, Object... params) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            setParameters(stmt, params);
            stmt.executeUpdate();
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }
        return null;
    }

    /**
     * Begins a transaction.
     */
    public void beginTransaction() throws SQLException {
        connection.setAutoCommit(false);
    }

    /**
     * Commits the current transaction.
     */
    public void commit() throws SQLException {
        connection.commit();
        connection.setAutoCommit(true);
    }

    /**
     * Rolls back the current transaction.
     */
    public void rollback() {
        try {
            connection.rollback();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            log.error("Error rolling back transaction", e);
        }
    }

    private void setParameters(PreparedStatement stmt, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            if (param == null) {
                stmt.setNull(i + 1, java.sql.Types.NULL);
            } else if (param instanceof String) {
                stmt.setString(i + 1, (String) param);
            } else if (param instanceof Integer) {
                stmt.setInt(i + 1, (Integer) param);
            } else if (param instanceof Long) {
                stmt.setLong(i + 1, (Long) param);
            } else if (param instanceof byte[]) {
                stmt.setBytes(i + 1, (byte[]) param);
            } else if (param instanceof Boolean) {
                stmt.setInt(i + 1, (Boolean) param ? 1 : 0);
            } else {
                stmt.setObject(i + 1, param);
            }
        }
    }

    /**
     * Gets the database file path.
     */
    public String getDatabasePath() {
        return databasePath;
    }

    /**
     * Gets a setting value from the database.
     */
    public String getSetting(String key, String defaultValue) {
        try (ResultSet rs = executeQuery("SELECT value FROM settings WHERE key = ?", key)) {
            if (rs.next()) {
                return rs.getString("value");
            }
        } catch (SQLException e) {
            log.error("Failed to get setting: {}", key, e);
        }
        return defaultValue;
    }

    /**
     * Sets a setting value in the database.
     */
    public void setSetting(String key, String value) {
        try {
            executeUpdate(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    key,
                    value
            );
        } catch (SQLException e) {
            log.error("Failed to set setting: {} = {}", key, value, e);
            throw new DatabaseException("Failed to save setting", e);
        }
    }

    /**
     * Deletes a setting from the database.
     */
    public void deleteSetting(String key) {
        try {
            executeUpdate("DELETE FROM settings WHERE key = ?", key);
        } catch (SQLException e) {
            log.error("Failed to delete setting: {}", key, e);
        }
    }

    /**
     * Custom exception for database operations.
     */
    public static class DatabaseException extends RuntimeException {
        public DatabaseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
