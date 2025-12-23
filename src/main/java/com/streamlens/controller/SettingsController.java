package com.streamlens.controller;

import com.streamlens.service.api.LocalApiService;
import com.streamlens.service.storage.DatabaseService;
import com.streamlens.service.storage.MessageRepository;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.ResourceBundle;

/**
 * Controller for the settings dialog.
 */
@Component
public class SettingsController implements Initializable {

    private final DatabaseService databaseService;
    private final MessageRepository messageRepository;
    private final LocalApiService localApiService;

    // Local API
    @FXML private CheckBox apiEnabledCheckBox;
    @FXML private TextField apiPortField;

    // Display
    @FXML private TextField maxMessagesField;
    @FXML private ComboBox<String> timestampFormatComboBox;

    // Data
    @FXML private TextField databasePathField;
    @FXML private Button clearSavedMessagesButton;
    @FXML private Label savedMessagesCountLabel;

    public SettingsController(DatabaseService databaseService, MessageRepository messageRepository,
                              LocalApiService localApiService) {
        this.databaseService = databaseService;
        this.messageRepository = messageRepository;
        this.localApiService = localApiService;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        // Initialize timestamp format options
        timestampFormatComboBox.setItems(FXCollections.observableArrayList(
                "yyyy-MM-dd HH:mm:ss.SSS",
                "yyyy-MM-dd HH:mm:ss",
                "MM/dd/yyyy HH:mm:ss",
                "dd/MM/yyyy HH:mm:ss",
                "HH:mm:ss.SSS",
                "ISO-8601"
        ));

        // Bind port field to API enabled state
        apiPortField.disableProperty().bind(apiEnabledCheckBox.selectedProperty().not());

        // Load current settings
        loadSettings();

        // Update saved messages count
        updateSavedMessagesCount();
    }

    private void loadSettings() {
        // Load from database or use defaults
        String apiEnabled = databaseService.getSetting("api.enabled", "false");
        String apiPort = databaseService.getSetting("api.port", "9876");
        String maxMessages = databaseService.getSetting("display.maxMessages", "10000");
        String timestampFormat = databaseService.getSetting("display.timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS");

        apiEnabledCheckBox.setSelected(Boolean.parseBoolean(apiEnabled));
        apiPortField.setText(apiPort);
        maxMessagesField.setText(maxMessages);
        timestampFormatComboBox.setValue(timestampFormat);

        // Database path (read-only)
        databasePathField.setText(databaseService.getDatabasePath());
    }

    private void updateSavedMessagesCount() {
        int count = messageRepository.count();
        savedMessagesCountLabel.setText("(" + count + " messages saved)");
    }

    @FXML
    private void handleClearSavedMessages() {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Clear Saved Messages");
        alert.setHeaderText("Delete all saved messages?");
        alert.setContentText("This action cannot be undone. All starred and flagged messages will be permanently deleted.");

        alert.showAndWait().ifPresent(response -> {
            if (response == ButtonType.OK) {
                try {
                    // Delete all messages
                    databaseService.executeUpdate("DELETE FROM messages");
                    updateSavedMessagesCount();
                } catch (Exception e) {
                    Alert errorAlert = new Alert(Alert.AlertType.ERROR);
                    errorAlert.setTitle("Error");
                    errorAlert.setHeaderText("Failed to clear messages");
                    errorAlert.setContentText(e.getMessage());
                    errorAlert.showAndWait();
                }
            }
        });
    }

    /**
     * Validates the settings and returns an error message, or null if valid.
     */
    public String validate() {
        // Validate port number
        if (apiEnabledCheckBox.isSelected()) {
            try {
                int port = Integer.parseInt(apiPortField.getText().trim());
                if (port < 1 || port > 65535) {
                    return "Port must be between 1 and 65535";
                }
            } catch (NumberFormatException e) {
                return "Invalid port number";
            }
        }

        // Validate max messages
        try {
            int maxMessages = Integer.parseInt(maxMessagesField.getText().trim());
            if (maxMessages < 100 || maxMessages > 100000) {
                return "Max messages must be between 100 and 100000";
            }
        } catch (NumberFormatException e) {
            return "Invalid max messages value";
        }

        return null;
    }

    /**
     * Saves the current settings to the database and applies changes.
     */
    public void saveSettings() {
        boolean wasApiEnabled = localApiService.isRunning();
        boolean apiEnabled = apiEnabledCheckBox.isSelected();
        int port = Integer.parseInt(apiPortField.getText().trim());

        // Save settings
        databaseService.setSetting("api.enabled", String.valueOf(apiEnabled));
        databaseService.setSetting("api.port", String.valueOf(port));
        databaseService.setSetting("display.maxMessages", maxMessagesField.getText().trim());
        databaseService.setSetting("display.timestampFormat", timestampFormatComboBox.getValue());

        // Apply API changes
        if (apiEnabled && !wasApiEnabled) {
            // Start API
            localApiService.start(port);
        } else if (!apiEnabled && wasApiEnabled) {
            // Stop API
            localApiService.stop();
        } else if (apiEnabled && wasApiEnabled && port != localApiService.getPort()) {
            // Restart with new port
            localApiService.stop();
            localApiService.start(port);
        }
    }

    // Getters for settings values

    public boolean isApiEnabled() {
        return apiEnabledCheckBox.isSelected();
    }

    public int getApiPort() {
        return Integer.parseInt(apiPortField.getText().trim());
    }

    public int getMaxMessages() {
        return Integer.parseInt(maxMessagesField.getText().trim());
    }

    public String getTimestampFormat() {
        return timestampFormatComboBox.getValue();
    }
}
