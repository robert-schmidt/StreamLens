package com.streamlens.controller;

import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.model.streaming.StreamingOptions;
import com.streamlens.model.streaming.StreamingOptions.StartPosition;
import com.streamlens.service.kafka.KafkaAdminService;
import com.streamlens.service.kafka.KafkaAdminService.OffsetRange;
import com.streamlens.service.storage.DatabaseService;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Controller for the streaming options dialog.
 * Allows users to configure offset position, consumer group, and other streaming settings.
 */
@Component
public class StreamingOptionsDialogController implements Initializable {

    private static final Logger log = LoggerFactory.getLogger(StreamingOptionsDialogController.class);
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final KafkaAdminService kafkaAdminService;
    private final DatabaseService databaseService;

    @FXML private Label topicLabel;
    @FXML private Label partitionLabel;
    @FXML private ComboBox<StartPosition> startPositionCombo;
    @FXML private HBox offsetPane;
    @FXML private TextField offsetField;
    @FXML private VBox timestampPane;
    @FXML private DatePicker datePicker;
    @FXML private TextField timeField;
    @FXML private VBox offsetInfoPane;
    @FXML private Label earliestOffsetLabel;
    @FXML private Label latestOffsetLabel;
    @FXML private Label messageCountLabel;
    @FXML private TextField consumerGroupField;
    @FXML private CheckBox commitOffsetsCheckBox;
    @FXML private TextField maxMessagesField;
    @FXML private CheckBox rememberSettingsCheckBox;

    private ConnectionProfile connectionProfile;
    private String topic;
    private Integer partition;
    private Map<Integer, OffsetRange> topicOffsets;

    public StreamingOptionsDialogController(KafkaAdminService kafkaAdminService, DatabaseService databaseService) {
        this.kafkaAdminService = kafkaAdminService;
        this.databaseService = databaseService;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        // Setup start position combo
        startPositionCombo.setItems(FXCollections.observableArrayList(StartPosition.values()));
        startPositionCombo.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            updatePositionPanes(newVal);
        });
        startPositionCombo.getSelectionModel().select(StartPosition.LATEST);

        // Setup date picker with today's date
        datePicker.setValue(LocalDate.now());
        timeField.setText("00:00:00");

        // Setup max messages field to only accept numbers
        maxMessagesField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal.matches("\\d*")) {
                maxMessagesField.setText(newVal.replaceAll("[^\\d]", ""));
            }
        });

        // Setup offset field to only accept numbers
        offsetField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal.matches("\\d*")) {
                offsetField.setText(newVal.replaceAll("[^\\d]", ""));
            }
        });

        // Commit offsets only makes sense with a consumer group
        commitOffsetsCheckBox.disableProperty().bind(
                consumerGroupField.textProperty().isEmpty()
        );
    }

    private void updatePositionPanes(StartPosition position) {
        offsetPane.setVisible(position == StartPosition.FROM_OFFSET);
        offsetPane.setManaged(position == StartPosition.FROM_OFFSET);
        timestampPane.setVisible(position == StartPosition.FROM_TIMESTAMP);
        timestampPane.setManaged(position == StartPosition.FROM_TIMESTAMP);
    }

    /**
     * Sets the context for streaming (connection, topic, partition).
     */
    public void setContext(ConnectionProfile profile, String topic, Integer partition) {
        this.connectionProfile = profile;
        this.topic = topic;
        this.partition = partition;

        topicLabel.setText(topic);
        partitionLabel.setText(partition != null ? String.valueOf(partition) : "All partitions");

        // Load saved settings if available
        loadSavedSettings();

        // Load offset information in background
        loadOffsetInfo();
    }

    private void loadSavedSettings() {
        String settingsKey = "streaming." + connectionProfile.getId() + "." + topic;

        String savedPosition = databaseService.getSetting(settingsKey + ".position", null);
        if (savedPosition != null) {
            try {
                startPositionCombo.getSelectionModel().select(StartPosition.valueOf(savedPosition));
            } catch (Exception e) {
                log.debug("Invalid saved position: {}", savedPosition);
            }
        }

        String savedGroup = databaseService.getSetting(settingsKey + ".consumerGroup", null);
        if (savedGroup != null) {
            consumerGroupField.setText(savedGroup);
        }

        String savedCommit = databaseService.getSetting(settingsKey + ".commitOffsets", "false");
        commitOffsetsCheckBox.setSelected(Boolean.parseBoolean(savedCommit));

        String savedMaxMessages = databaseService.getSetting(settingsKey + ".maxMessages", "0");
        maxMessagesField.setText(savedMaxMessages);
    }

    private void loadOffsetInfo() {
        if (connectionProfile.getType() != ConnectionProfile.ConnectionType.KAFKA) {
            offsetInfoPane.setVisible(false);
            offsetInfoPane.setManaged(false);
            return;
        }

        Task<Map<Integer, OffsetRange>> task = new Task<>() {
            @Override
            protected Map<Integer, OffsetRange> call() throws Exception {
                return kafkaAdminService.getTopicOffsets(connectionProfile.getKafkaConfig(), topic);
            }
        };

        task.setOnSucceeded(event -> {
            topicOffsets = task.getValue();
            Platform.runLater(() -> updateOffsetLabels());
        });

        task.setOnFailed(event -> {
            log.warn("Failed to load offset info", task.getException());
        });

        new Thread(task).start();
    }

    private void updateOffsetLabels() {
        if (topicOffsets == null || topicOffsets.isEmpty()) {
            return;
        }

        if (partition != null) {
            // Show info for specific partition
            OffsetRange range = topicOffsets.get(partition);
            if (range != null) {
                earliestOffsetLabel.setText(String.valueOf(range.getBeginning()));
                latestOffsetLabel.setText(String.valueOf(range.getEnd()));
                messageCountLabel.setText(String.valueOf(range.getMessageCount()));
            }
        } else {
            // Show aggregate for all partitions
            long totalEarliest = 0;
            long totalLatest = 0;
            long totalMessages = 0;
            for (OffsetRange range : topicOffsets.values()) {
                totalEarliest += range.getBeginning();
                totalLatest += range.getEnd();
                totalMessages += range.getMessageCount();
            }
            earliestOffsetLabel.setText(String.valueOf(totalEarliest));
            latestOffsetLabel.setText(String.valueOf(totalLatest));
            messageCountLabel.setText(String.valueOf(totalMessages));
        }
    }

    /**
     * Builds the streaming options from dialog values.
     */
    public StreamingOptions buildOptions() {
        StreamingOptions options = new StreamingOptions();

        StartPosition position = startPositionCombo.getValue();
        options.setStartPosition(position);

        if (position == StartPosition.FROM_OFFSET && !offsetField.getText().isEmpty()) {
            options.setSpecificOffset(Long.parseLong(offsetField.getText()));
        }

        if (position == StartPosition.FROM_TIMESTAMP) {
            LocalDate date = datePicker.getValue();
            LocalTime time = parseTime(timeField.getText());
            if (date != null && time != null) {
                options.setSpecificTimestamp(LocalDateTime.of(date, time)
                        .atZone(ZoneId.systemDefault()).toInstant());
            }
        }

        String consumerGroup = consumerGroupField.getText().trim();
        if (!consumerGroup.isEmpty()) {
            options.setConsumerGroup(consumerGroup);
            options.setCommitOffsets(commitOffsetsCheckBox.isSelected());
        }

        String maxMessagesStr = maxMessagesField.getText().trim();
        if (!maxMessagesStr.isEmpty()) {
            options.setMaxMessages(Integer.parseInt(maxMessagesStr));
        }

        // Save settings if requested
        if (rememberSettingsCheckBox.isSelected()) {
            saveSettings();
        }

        return options;
    }

    private void saveSettings() {
        String settingsKey = "streaming." + connectionProfile.getId() + "." + topic;

        databaseService.setSetting(settingsKey + ".position",
                startPositionCombo.getValue().name());
        databaseService.setSetting(settingsKey + ".consumerGroup",
                consumerGroupField.getText().trim());
        databaseService.setSetting(settingsKey + ".commitOffsets",
                String.valueOf(commitOffsetsCheckBox.isSelected()));
        databaseService.setSetting(settingsKey + ".maxMessages",
                maxMessagesField.getText().trim());
    }

    private LocalTime parseTime(String timeStr) {
        if (timeStr == null || timeStr.isEmpty()) {
            return LocalTime.MIDNIGHT;
        }
        try {
            return LocalTime.parse(timeStr, TIME_FORMAT);
        } catch (DateTimeParseException e) {
            return LocalTime.MIDNIGHT;
        }
    }

    /**
     * Validates the form fields.
     */
    public String validate() {
        StartPosition position = startPositionCombo.getValue();

        if (position == StartPosition.FROM_OFFSET) {
            if (offsetField.getText().trim().isEmpty()) {
                return "Offset is required when starting from specific offset";
            }
            try {
                long offset = Long.parseLong(offsetField.getText().trim());
                if (offset < 0) {
                    return "Offset must be non-negative";
                }
            } catch (NumberFormatException e) {
                return "Invalid offset value";
            }
        }

        if (position == StartPosition.FROM_TIMESTAMP) {
            if (datePicker.getValue() == null) {
                return "Date is required when starting from timestamp";
            }
        }

        String maxMessagesStr = maxMessagesField.getText().trim();
        if (!maxMessagesStr.isEmpty()) {
            try {
                int max = Integer.parseInt(maxMessagesStr);
                if (max < 0) {
                    return "Max messages must be non-negative";
                }
            } catch (NumberFormatException e) {
                return "Invalid max messages value";
            }
        }

        return null;
    }

    /**
     * Gets the earliest offset for the current partition.
     */
    public Long getEarliestOffset() {
        if (topicOffsets == null) return null;
        if (partition != null) {
            OffsetRange range = topicOffsets.get(partition);
            return range != null ? range.getBeginning() : null;
        }
        return topicOffsets.values().stream()
                .mapToLong(OffsetRange::getBeginning)
                .min().orElse(0L);
    }

    /**
     * Gets the latest offset for the current partition.
     */
    public Long getLatestOffset() {
        if (topicOffsets == null) return null;
        if (partition != null) {
            OffsetRange range = topicOffsets.get(partition);
            return range != null ? range.getEnd() : null;
        }
        return topicOffsets.values().stream()
                .mapToLong(OffsetRange::getEnd)
                .max().orElse(0L);
    }
}
