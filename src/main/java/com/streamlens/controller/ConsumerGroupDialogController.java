package com.streamlens.controller;

import com.streamlens.model.connection.KafkaConnectionConfig;
import com.streamlens.service.kafka.KafkaAdminService;
import com.streamlens.service.kafka.KafkaAdminService.*;
import javafx.application.Platform;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.util.converter.LongStringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Controller for consumer group management dialog.
 * Allows viewing and resetting consumer group offsets.
 */
@Component
public class ConsumerGroupDialogController implements Initializable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupDialogController.class);

    private final KafkaAdminService kafkaAdminService;

    @FXML private Label groupIdLabel;
    @FXML private Label stateLabel;
    @FXML private Label membersLabel;

    @FXML private ComboBox<String> topicCombo;
    @FXML private Button refreshButton;

    // Create group pane (for new groups)
    @FXML private VBox createGroupPane;
    @FXML private RadioButton latestRadio;
    @FXML private RadioButton earliestRadio;
    @FXML private Button createGroupButton;

    // Reset pane (for existing groups)
    @FXML private Label resetLabel;
    @FXML private HBox resetPane;

    @FXML private TableView<OffsetRow> offsetsTable;
    @FXML private TableColumn<OffsetRow, Integer> partitionCol;
    @FXML private TableColumn<OffsetRow, Long> earliestCol;
    @FXML private TableColumn<OffsetRow, Long> currentCol;
    @FXML private TableColumn<OffsetRow, Long> latestCol;
    @FXML private TableColumn<OffsetRow, Long> lagCol;
    @FXML private TableColumn<OffsetRow, Long> newOffsetCol;

    @FXML private Button resetEarliestButton;
    @FXML private Button resetLatestButton;
    @FXML private Button resetCustomButton;

    @FXML private DatePicker datePicker;
    @FXML private TextField timeField;
    @FXML private Button resetTimestampButton;

    @FXML private Label statusLabel;

    private KafkaConnectionConfig config;
    private String groupId;
    private boolean isNewGroup = false;

    private final ObservableList<OffsetRow> offsets = FXCollections.observableArrayList();
    private final ObservableList<String> topics = FXCollections.observableArrayList();

    public ConsumerGroupDialogController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        setupTable();
        topicCombo.setItems(topics);
        datePicker.setValue(LocalDate.now());
        timeField.setText("00:00:00");
    }

    private void setupTable() {
        offsetsTable.setItems(offsets);
        offsetsTable.setEditable(true);

        partitionCol.setCellValueFactory(data -> new SimpleIntegerProperty(data.getValue().partition).asObject());
        earliestCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().earliest).asObject());
        currentCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().current).asObject());
        latestCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().latest).asObject());
        lagCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().getLag()).asObject());

        // Make new offset column editable
        newOffsetCol.setCellValueFactory(data -> data.getValue().newOffsetProperty.asObject());
        newOffsetCol.setCellFactory(TextFieldTableCell.forTableColumn(new LongStringConverter()));
        newOffsetCol.setOnEditCommit(event -> {
            OffsetRow row = event.getRowValue();
            Long newValue = event.getNewValue();
            if (newValue != null && newValue >= row.earliest && newValue <= row.latest) {
                row.newOffsetProperty.set(newValue);
            } else {
                // Reset to current if invalid
                row.newOffsetProperty.set(row.current);
                showStatus("Offset must be between " + row.earliest + " and " + row.latest, true);
            }
        });

        // Style lag column
        lagCol.setCellFactory(col -> new TableCell<>() {
            @Override
            protected void updateItem(Long item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setStyle("");
                } else {
                    setText(item.toString());
                    if (item > 1000) {
                        setStyle("-fx-text-fill: red;");
                    } else if (item > 100) {
                        setStyle("-fx-text-fill: orange;");
                    } else {
                        setStyle("-fx-text-fill: green;");
                    }
                }
            }
        });
    }

    /**
     * Initialize for viewing/editing an existing consumer group.
     */
    public void initializeForGroup(KafkaConnectionConfig config, String groupId, String state, int members) {
        this.config = config;
        this.groupId = groupId;
        this.isNewGroup = false;

        groupIdLabel.setText(groupId);
        stateLabel.setText(state);
        membersLabel.setText(String.valueOf(members));

        loadTopics();
    }

    /**
     * Initialize for creating a new consumer group.
     */
    public void initializeForNewGroup(KafkaConnectionConfig config) {
        this.config = config;
        this.isNewGroup = true;

        // Show text field for entering new group ID
        groupIdLabel.setText("(New Group)");
        stateLabel.setText("N/A");
        membersLabel.setText("0");

        // Show create pane, hide reset pane for new groups
        createGroupPane.setVisible(true);
        createGroupPane.setManaged(true);
        resetLabel.setVisible(false);
        resetLabel.setManaged(false);
        resetPane.setVisible(false);
        resetPane.setManaged(false);

        loadTopics();
    }

    /**
     * Set group ID for new group creation.
     */
    public void setNewGroupId(String groupId) {
        this.groupId = groupId;
        groupIdLabel.setText(groupId);
    }

    private void loadTopics() {
        Task<List<String>> task = new Task<>() {
            @Override
            protected List<String> call() throws Exception {
                Map<String, Integer> topicsWithPartitions = kafkaAdminService.listTopicsWithPartitions(config);
                List<String> result = new ArrayList<>(topicsWithPartitions.keySet());
                Collections.sort(result);
                return result;
            }
        };

        task.setOnSucceeded(e -> Platform.runLater(() -> {
            topics.setAll(task.getValue());
            if (!topics.isEmpty()) {
                topicCombo.getSelectionModel().selectFirst();
                loadOffsets();
            }
        }));

        task.setOnFailed(e -> showStatus("Failed to load topics: " + task.getException().getMessage(), true));

        new Thread(task).start();
    }

    @FXML
    private void handleTopicSelected() {
        loadOffsets();
    }

    @FXML
    private void handleRefresh() {
        loadOffsets();
    }

    private void loadOffsets() {
        String selectedTopic = topicCombo.getValue();
        if (selectedTopic == null || selectedTopic.isEmpty()) {
            return;
        }

        Task<List<OffsetRow>> task = new Task<>() {
            @Override
            protected List<OffsetRow> call() throws Exception {
                List<OffsetRow> result = new ArrayList<>();

                // Get topic offsets (earliest/latest)
                Map<Integer, OffsetRange> topicOffsets = kafkaAdminService.getTopicOffsets(config, selectedTopic);

                // Get consumer group offsets (current committed)
                Map<Integer, Long> groupOffsets = new HashMap<>();
                if (!isNewGroup && groupId != null) {
                    try {
                        groupOffsets = kafkaAdminService.getConsumerGroupOffsets(config, groupId, selectedTopic);
                    } catch (Exception e) {
                        log.debug("No offsets for group {} on topic {}", groupId, selectedTopic);
                    }
                }

                for (Map.Entry<Integer, OffsetRange> entry : topicOffsets.entrySet()) {
                    int partition = entry.getKey();
                    OffsetRange range = entry.getValue();
                    long current = groupOffsets.getOrDefault(partition, -1L);

                    OffsetRow row = new OffsetRow(
                            partition,
                            range.getBeginning(),
                            current >= 0 ? current : range.getBeginning(),
                            range.getEnd()
                    );
                    // Initialize new offset to current
                    row.newOffsetProperty.set(row.current);
                    result.add(row);
                }

                result.sort(Comparator.comparingInt(r -> r.partition));
                return result;
            }
        };

        task.setOnSucceeded(e -> Platform.runLater(() -> {
            offsets.setAll(task.getValue());
            showStatus("Loaded offsets for " + selectedTopic, false);
        }));

        task.setOnFailed(e -> showStatus("Failed to load offsets: " + task.getException().getMessage(), true));

        new Thread(task).start();
    }

    @FXML
    private void handleCreateGroup() {
        boolean useLatest = latestRadio.isSelected();
        createGroupWithOffsets(useLatest);
    }

    private void createGroupWithOffsets(boolean useLatest) {
        String topic = topicCombo.getValue();
        if (topic == null || topic.isEmpty()) {
            showStatus("Select a topic first", true);
            return;
        }
        if (groupId == null || groupId.isEmpty()) {
            showStatus("Group ID is required", true);
            return;
        }

        String position = useLatest ? "latest" : "earliest";
        showStatus("Creating consumer group '" + groupId + "' at " + position + " offsets...", false);

        Task<Void> task = new Task<>() {
            @Override
            protected Void call() throws Exception {
                if (useLatest) {
                    kafkaAdminService.resetOffsetsToLatest(config, groupId, topic);
                } else {
                    kafkaAdminService.resetOffsetsToEarliest(config, groupId, topic);
                }
                return null;
            }
        };

        task.setOnSucceeded(e -> {
            isNewGroup = false;
            Platform.runLater(() -> {
                showStatus("Consumer group '" + groupId + "' created successfully!", false);
                stateLabel.setText("EMPTY");

                // Switch from create pane to reset pane
                createGroupPane.setVisible(false);
                createGroupPane.setManaged(false);
                resetLabel.setVisible(true);
                resetLabel.setManaged(true);
                resetPane.setVisible(true);
                resetPane.setManaged(true);

                loadOffsets();
            });
        });

        task.setOnFailed(e -> {
            log.error("Failed to create consumer group", task.getException());
            showStatus("Failed to create group: " + task.getException().getMessage(), true);
        });

        new Thread(task).start();
    }

    @FXML
    private void handleResetEarliest() {
        String topic = topicCombo.getValue();
        if (topic == null || groupId == null) {
            showStatus("Select a topic first", true);
            return;
        }

        confirm("Reset to Earliest", "Reset all partitions to earliest offset?", () -> {
            Task<Void> task = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    kafkaAdminService.resetOffsetsToEarliest(config, groupId, topic);
                    return null;
                }
            };

            task.setOnSucceeded(e -> {
                showStatus("Reset offsets to earliest", false);
                loadOffsets();
            });

            task.setOnFailed(e -> showStatus("Failed: " + task.getException().getMessage(), true));

            new Thread(task).start();
        });
    }

    @FXML
    private void handleResetLatest() {
        String topic = topicCombo.getValue();
        if (topic == null || groupId == null) {
            showStatus("Select a topic first", true);
            return;
        }

        confirm("Reset to Latest", "Reset all partitions to latest offset?", () -> {
            Task<Void> task = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    kafkaAdminService.resetOffsetsToLatest(config, groupId, topic);
                    return null;
                }
            };

            task.setOnSucceeded(e -> {
                showStatus("Reset offsets to latest", false);
                loadOffsets();
            });

            task.setOnFailed(e -> showStatus("Failed: " + task.getException().getMessage(), true));

            new Thread(task).start();
        });
    }

    @FXML
    private void handleResetCustom() {
        String topic = topicCombo.getValue();
        if (topic == null || groupId == null) {
            showStatus("Select a topic first", true);
            return;
        }

        // Collect custom offsets from the table
        List<OffsetRow> rowsToUpdate = new ArrayList<>();
        for (OffsetRow row : offsets) {
            if (row.newOffsetProperty.get() != row.current) {
                rowsToUpdate.add(row);
            }
        }

        if (rowsToUpdate.isEmpty()) {
            showStatus("No offset changes to apply", true);
            return;
        }

        confirm("Apply Custom Offsets", "Update " + rowsToUpdate.size() + " partition(s)?", () -> {
            Task<Void> task = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    for (OffsetRow row : rowsToUpdate) {
                        kafkaAdminService.resetOffsetsToSpecific(
                                config, groupId, topic,
                                row.partition, row.newOffsetProperty.get()
                        );
                    }
                    return null;
                }
            };

            task.setOnSucceeded(e -> {
                showStatus("Applied custom offsets to " + rowsToUpdate.size() + " partition(s)", false);
                loadOffsets();
            });

            task.setOnFailed(e -> showStatus("Failed: " + task.getException().getMessage(), true));

            new Thread(task).start();
        });
    }

    @FXML
    private void handleResetTimestamp() {
        String topic = topicCombo.getValue();
        if (topic == null || groupId == null) {
            showStatus("Select a topic first", true);
            return;
        }

        LocalDate date = datePicker.getValue();
        if (date == null) {
            showStatus("Select a date", true);
            return;
        }

        LocalTime time;
        try {
            time = LocalTime.parse(timeField.getText(), DateTimeFormatter.ofPattern("HH:mm:ss"));
        } catch (DateTimeParseException e) {
            showStatus("Invalid time format (use HH:mm:ss)", true);
            return;
        }

        Instant timestamp = date.atTime(time).atZone(ZoneId.systemDefault()).toInstant();

        confirm("Reset to Timestamp", "Reset offsets to " + date + " " + time + "?", () -> {
            Task<Void> task = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    kafkaAdminService.resetOffsetsToTimestamp(config, groupId, topic, timestamp);
                    return null;
                }
            };

            task.setOnSucceeded(e -> {
                showStatus("Reset offsets to timestamp", false);
                loadOffsets();
            });

            task.setOnFailed(e -> showStatus("Failed: " + task.getException().getMessage(), true));

            new Thread(task).start();
        });
    }

    private void confirm(String title, String message, Runnable action) {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle(title);
        alert.setHeaderText(title);
        alert.setContentText(message);

        alert.showAndWait().ifPresent(result -> {
            if (result == ButtonType.OK) {
                action.run();
            }
        });
    }

    private void showStatus(String message, boolean isError) {
        Platform.runLater(() -> {
            statusLabel.setText(message);
            statusLabel.getStyleClass().removeAll("status-error", "status-success");
            statusLabel.getStyleClass().add(isError ? "status-error" : "status-success");
        });
    }

    /**
     * Data class for offset table rows.
     */
    public static class OffsetRow {
        final int partition;
        final long earliest;
        final long current;
        final long latest;
        final SimpleLongProperty newOffsetProperty = new SimpleLongProperty();

        OffsetRow(int partition, long earliest, long current, long latest) {
            this.partition = partition;
            this.earliest = earliest;
            this.current = current;
            this.latest = latest;
            this.newOffsetProperty.set(current);
        }

        public long getLag() {
            return current >= 0 ? latest - current : 0;
        }
    }
}
