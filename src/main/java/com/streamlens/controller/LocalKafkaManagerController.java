package com.streamlens.controller;

import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.model.connection.KafkaConnectionConfig;
import com.streamlens.service.kafka.KafkaAdminService;
import com.streamlens.service.kafka.KafkaAdminService.*;
import com.streamlens.service.kafka.LocalKafkaService;
import com.streamlens.service.kafka.LocalKafkaService.*;
import javafx.application.HostServices;
import javafx.application.Platform;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.fxml.FXMLLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Controller for the Local Kafka Manager dialog.
 * Provides UI for starting/stopping local Kafka and managing topics/groups.
 */
@Component
public class LocalKafkaManagerController implements Initializable {

    private static final Logger log = LoggerFactory.getLogger(LocalKafkaManagerController.class);

    private final LocalKafkaService localKafkaService;
    private final KafkaAdminService kafkaAdminService;

    // Status section
    @FXML private Label statusLabel;
    @FXML private Label bootstrapLabel;
    @FXML private Button copyBootstrapButton;
    @FXML private Button startButton;
    @FXML private Button stopButton;
    @FXML private Button removeButton;

    // Stats
    @FXML private HBox statsPane;
    @FXML private HBox statsContent;
    @FXML private Label memoryLabel;
    @FXML private Label cpuLabel;

    // Docker warning
    @FXML private HBox dockerWarningPane;
    @FXML private Button retryDockerButton;
    @FXML private Hyperlink dockerLink;

    // Management tabs
    @FXML private TabPane managementTabs;

    // Topics tab
    @FXML private Button refreshTopicsButton;
    @FXML private Button createTopicButton;
    @FXML private TextField topicSearchField;
    @FXML private TableView<TopicRow> topicsTable;
    @FXML private VBox topicsLoadingPane;
    @FXML private TableColumn<TopicRow, String> topicNameCol;
    @FXML private TableColumn<TopicRow, Integer> topicPartitionsCol;
    @FXML private TableColumn<TopicRow, Long> topicMessagesCol;
    @FXML private TableColumn<TopicRow, Void> topicActionsCol;

    // Consumer Groups tab
    @FXML private Button refreshGroupsButton;
    @FXML private Button createGroupButton;
    @FXML private TextField groupSearchField;
    @FXML private TableView<GroupRow> groupsTable;
    @FXML private VBox groupsLoadingPane;
    @FXML private TableColumn<GroupRow, String> groupIdCol;
    @FXML private TableColumn<GroupRow, String> groupStateCol;
    @FXML private TableColumn<GroupRow, Integer> groupMembersCol;
    @FXML private TableColumn<GroupRow, Long> groupLagCol;
    @FXML private TableColumn<GroupRow, Void> groupActionsCol;

    // Logs tab
    @FXML private Button clearLogsButton;
    @FXML private CheckBox autoScrollCheck;
    @FXML private Button copyLogsButton;
    @FXML private TextArea logsArea;

    private final ObservableList<TopicRow> topics = FXCollections.observableArrayList();
    private final ObservableList<GroupRow> groups = FXCollections.observableArrayList();
    private FilteredList<TopicRow> filteredTopics;
    private FilteredList<GroupRow> filteredGroups;

    private ScheduledExecutorService statsExecutor;
    private KafkaConnectionConfig localKafkaConfig;

    public LocalKafkaManagerController(LocalKafkaService localKafkaService,
                                       KafkaAdminService kafkaAdminService) {
        this.localKafkaService = localKafkaService;
        this.kafkaAdminService = kafkaAdminService;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        setupStatusListener();
        setupTopicsTable();
        setupGroupsTable();
        setupSearchFilters();
        setupLogging();

        // Create local kafka config
        localKafkaConfig = new KafkaConnectionConfig();
        localKafkaConfig.setBootstrapServers(localKafkaService.getBootstrapServers());
        localKafkaConfig.setAuthenticationType(KafkaConnectionConfig.AuthenticationType.NONE);

        // Initial status update
        updateUIForStatus(localKafkaService.getStatus());

        log.info("LocalKafkaManagerController initialized");
    }

    private void setupStatusListener() {
        localKafkaService.setStatusListener(this::updateUIForStatus);
        localKafkaService.setLogListener(this::appendLog);
    }

    private void updateUIForStatus(LocalKafkaStatus status) {
        Platform.runLater(() -> {
            statusLabel.setText(status.getDisplayName());

            // Update status label style
            statusLabel.getStyleClass().removeAll("status-running", "status-stopped", "status-error", "status-loading");
            switch (status) {
                case RUNNING -> statusLabel.getStyleClass().add("status-running");
                case STOPPED, NOT_CREATED -> statusLabel.getStyleClass().add("status-stopped");
                case ERROR, DOCKER_NOT_AVAILABLE -> statusLabel.getStyleClass().add("status-error");
                default -> statusLabel.getStyleClass().add("status-loading");
            }

            // Update button states
            boolean dockerAvailable = localKafkaService.isDockerAvailable();
            boolean isRunning = status == LocalKafkaStatus.RUNNING;
            boolean isStopped = status == LocalKafkaStatus.STOPPED || status == LocalKafkaStatus.NOT_CREATED;
            boolean isTransitioning = status == LocalKafkaStatus.STARTING ||
                    status == LocalKafkaStatus.STOPPING ||
                    status == LocalKafkaStatus.CREATING ||
                    status == LocalKafkaStatus.PULLING_IMAGE;

            startButton.setDisable(!dockerAvailable || isRunning || isTransitioning);
            stopButton.setDisable(!dockerAvailable || !isRunning || isTransitioning);
            removeButton.setDisable(!dockerAvailable || isRunning || isTransitioning ||
                    status == LocalKafkaStatus.NOT_CREATED);

            // Show/hide Docker warning
            dockerWarningPane.setVisible(!dockerAvailable);
            dockerWarningPane.setManaged(!dockerAvailable);

            // Enable/disable management tabs
            managementTabs.setDisable(!isRunning);

            // Show/hide stats
            statsPane.setVisible(isRunning);
            statsPane.setManaged(isRunning);
            statsContent.setVisible(isRunning);
            statsContent.setManaged(isRunning);

            // Start/stop stats polling
            if (isRunning) {
                startStatsPolling();
                loadTopics();
                loadGroups();
            } else {
                stopStatsPolling();
            }
        });
    }

    private void setupTopicsTable() {
        filteredTopics = new FilteredList<>(topics, p -> true);
        topicsTable.setItems(filteredTopics);

        topicNameCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().name));
        topicPartitionsCol.setCellValueFactory(data -> new SimpleIntegerProperty(data.getValue().partitions).asObject());
        topicMessagesCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().messages).asObject());

        // Actions column
        topicActionsCol.setCellFactory(col -> new TableCell<>() {
            private final Button deleteBtn = new Button("Delete");
            private final Button addPartBtn = new Button("+Part");
            private final HBox buttons = new HBox(5, addPartBtn, deleteBtn);

            {
                buttons.setAlignment(Pos.CENTER);
                deleteBtn.getStyleClass().add("small-button");
                addPartBtn.getStyleClass().add("small-button");

                deleteBtn.setOnAction(e -> {
                    TopicRow row = getTableView().getItems().get(getIndex());
                    handleDeleteTopic(row.name);
                });

                addPartBtn.setOnAction(e -> {
                    TopicRow row = getTableView().getItems().get(getIndex());
                    handleAddPartitions(row.name, row.partitions);
                });
            }

            @Override
            protected void updateItem(Void item, boolean empty) {
                super.updateItem(item, empty);
                setGraphic(empty ? null : buttons);
            }
        });
    }

    private void setupGroupsTable() {
        filteredGroups = new FilteredList<>(groups, p -> true);
        groupsTable.setItems(filteredGroups);

        groupIdCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().groupId));
        groupStateCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().state));
        groupMembersCol.setCellValueFactory(data -> new SimpleIntegerProperty(data.getValue().members).asObject());
        groupLagCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().totalLag).asObject());

        // Actions column
        groupActionsCol.setCellFactory(col -> new TableCell<>() {
            private final Button deleteBtn = new Button("Delete");
            private final Button resetBtn = new Button("Reset");
            private final HBox buttons = new HBox(5, resetBtn, deleteBtn);

            {
                buttons.setAlignment(Pos.CENTER);
                deleteBtn.getStyleClass().add("small-button");
                resetBtn.getStyleClass().add("small-button");

                deleteBtn.setOnAction(e -> {
                    GroupRow row = getTableView().getItems().get(getIndex());
                    handleDeleteGroup(row.groupId);
                });

                resetBtn.setOnAction(e -> {
                    GroupRow row = getTableView().getItems().get(getIndex());
                    handleResetGroup(row.groupId);
                });
            }

            @Override
            protected void updateItem(Void item, boolean empty) {
                super.updateItem(item, empty);
                setGraphic(empty ? null : buttons);
            }
        });
    }

    private void setupSearchFilters() {
        topicSearchField.textProperty().addListener((obs, oldVal, newVal) -> {
            String filter = newVal.toLowerCase().trim();
            filteredTopics.setPredicate(topic ->
                    filter.isEmpty() || topic.name.toLowerCase().contains(filter));
        });

        groupSearchField.textProperty().addListener((obs, oldVal, newVal) -> {
            String filter = newVal.toLowerCase().trim();
            filteredGroups.setPredicate(group ->
                    filter.isEmpty() || group.groupId.toLowerCase().contains(filter));
        });
    }

    private void setupLogging() {
        // Stream logs from container
        if (localKafkaService.getStatus() == LocalKafkaStatus.RUNNING) {
            localKafkaService.streamLogs(this::appendLog);
        }
    }

    private void appendLog(String message) {
        Platform.runLater(() -> {
            logsArea.appendText(message + "\n");
            if (autoScrollCheck.isSelected()) {
                logsArea.setScrollTop(Double.MAX_VALUE);
            }
        });
    }

    private void startStatsPolling() {
        if (statsExecutor != null && !statsExecutor.isShutdown()) {
            return;
        }

        statsExecutor = Executors.newSingleThreadScheduledExecutor();
        statsExecutor.scheduleAtFixedRate(() -> {
            ContainerStats stats = localKafkaService.getStats();
            if (stats != null) {
                Platform.runLater(() -> {
                    memoryLabel.setText(stats.getMemoryUsageFormatted() + " / " + stats.getMemoryLimitFormatted());
                    cpuLabel.setText(stats.getCpuPercentFormatted());
                });
            }
        }, 0, 2, TimeUnit.SECONDS);
    }

    private void stopStatsPolling() {
        if (statsExecutor != null) {
            statsExecutor.shutdown();
            statsExecutor = null;
        }
    }

    // ============== Button Handlers ==============

    @FXML
    private void handleStart() {
        appendLog("Starting local Kafka server...");
        localKafkaService.start().thenAccept(success -> {
            if (success) {
                Platform.runLater(() -> {
                    appendLog("Local Kafka started successfully");
                    loadTopics();
                    loadGroups();
                });
            }
        });
    }

    @FXML
    private void handleStop() {
        appendLog("Stopping local Kafka server...");
        localKafkaService.stop();
    }

    @FXML
    private void handleRemove() {
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Remove Container");
        confirm.setHeaderText("Remove Local Kafka Container?");
        confirm.setContentText("This will delete the container and all its data. Continue?");

        if (confirm.showAndWait().orElse(ButtonType.CANCEL) == ButtonType.OK) {
            appendLog("Removing local Kafka container...");
            localKafkaService.remove();
        }
    }

    @FXML
    private void handleCopyBootstrap() {
        ClipboardContent content = new ClipboardContent();
        content.putString(localKafkaService.getBootstrapServers());
        Clipboard.getSystemClipboard().setContent(content);
        appendLog("Copied bootstrap servers to clipboard");
    }

    @FXML
    private void handleGetDocker() {
        // Open Docker download page
        try {
            java.awt.Desktop.getDesktop().browse(new java.net.URI("https://www.docker.com/products/docker-desktop/"));
        } catch (Exception e) {
            log.error("Failed to open Docker download page", e);
        }
    }

    @FXML
    private void handleRetryDocker() {
        appendLog("Retrying Docker connection...");
        retryDockerButton.setDisable(true);

        Task<Boolean> task = new Task<>() {
            @Override
            protected Boolean call() {
                return localKafkaService.retryDockerConnection();
            }
        };

        task.setOnSucceeded(e -> Platform.runLater(() -> {
            retryDockerButton.setDisable(false);
            if (task.getValue()) {
                appendLog("Docker connection established!");
                updateUIForStatus(localKafkaService.getStatus());
            } else {
                appendLog("Docker still not available. Make sure Docker Desktop is running.");
            }
        }));

        task.setOnFailed(e -> Platform.runLater(() -> {
            retryDockerButton.setDisable(false);
            appendLog("Failed to connect to Docker: " + task.getException().getMessage());
        }));

        new Thread(task).start();
    }

    @FXML
    private void handleRefreshTopics() {
        loadTopics();
    }

    @FXML
    private void handleCreateTopic() {
        Dialog<TopicCreationResult> dialog = new Dialog<>();
        dialog.setTitle("Create Topic");
        dialog.setHeaderText("Create a new Kafka topic");

        // Set up dialog content
        VBox content = new VBox(10);
        content.setPadding(new javafx.geometry.Insets(10));

        TextField nameField = new TextField();
        nameField.setPromptText("topic-name");

        Spinner<Integer> partitionsSpinner = new Spinner<>(1, 100, 3);
        partitionsSpinner.setEditable(true);

        Spinner<Integer> replicationSpinner = new Spinner<>(1, 10, 1);
        replicationSpinner.setEditable(true);

        content.getChildren().addAll(
                new Label("Topic Name:"), nameField,
                new Label("Partitions:"), partitionsSpinner,
                new Label("Replication Factor:"), replicationSpinner
        );

        dialog.getDialogPane().setContent(content);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        dialog.setResultConverter(button -> {
            if (button == ButtonType.OK) {
                return new TopicCreationResult(
                        nameField.getText().trim(),
                        partitionsSpinner.getValue(),
                        replicationSpinner.getValue()
                );
            }
            return null;
        });

        dialog.showAndWait().ifPresent(result -> {
            if (result.name.isEmpty()) {
                showError("Invalid Name", "Topic name cannot be empty");
                return;
            }

            Task<Void> task = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    kafkaAdminService.createTopic(localKafkaConfig, result.name,
                            result.partitions, (short) result.replication);
                    return null;
                }
            };

            task.setOnSucceeded(e -> {
                appendLog("Created topic: " + result.name);
                loadTopics();
            });

            task.setOnFailed(e -> {
                showError("Failed to Create Topic", task.getException().getMessage());
            });

            new Thread(task).start();
        });
    }

    private void handleDeleteTopic(String topicName) {
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Delete Topic");
        confirm.setHeaderText("Delete topic '" + topicName + "'?");
        confirm.setContentText("This action cannot be undone.");

        if (confirm.showAndWait().orElse(ButtonType.CANCEL) == ButtonType.OK) {
            Task<Void> task = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    kafkaAdminService.deleteTopic(localKafkaConfig, topicName);
                    return null;
                }
            };

            task.setOnSucceeded(e -> {
                appendLog("Deleted topic: " + topicName);
                loadTopics();
            });

            task.setOnFailed(e -> {
                showError("Failed to Delete Topic", task.getException().getMessage());
            });

            new Thread(task).start();
        }
    }

    private void handleAddPartitions(String topicName, int currentPartitions) {
        TextInputDialog dialog = new TextInputDialog(String.valueOf(currentPartitions + 1));
        dialog.setTitle("Add Partitions");
        dialog.setHeaderText("Add partitions to '" + topicName + "'");
        dialog.setContentText("New total partitions (currently " + currentPartitions + "):");

        dialog.showAndWait().ifPresent(input -> {
            try {
                int newTotal = Integer.parseInt(input.trim());
                if (newTotal <= currentPartitions) {
                    showError("Invalid Value", "New partition count must be greater than current count");
                    return;
                }

                Task<Void> task = new Task<>() {
                    @Override
                    protected Void call() throws Exception {
                        kafkaAdminService.addPartitions(localKafkaConfig, topicName, newTotal);
                        return null;
                    }
                };

                task.setOnSucceeded(e -> {
                    appendLog("Added partitions to " + topicName + ": " + currentPartitions + " -> " + newTotal);
                    loadTopics();
                });

                task.setOnFailed(e -> {
                    showError("Failed to Add Partitions", task.getException().getMessage());
                });

                new Thread(task).start();
            } catch (NumberFormatException e) {
                showError("Invalid Value", "Please enter a valid number");
            }
        });
    }

    @FXML
    private void handleRefreshGroups() {
        loadGroups();
    }

    private void handleDeleteGroup(String groupId) {
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Delete Consumer Group");
        confirm.setHeaderText("Delete consumer group '" + groupId + "'?");
        confirm.setContentText("This action cannot be undone.");

        if (confirm.showAndWait().orElse(ButtonType.CANCEL) == ButtonType.OK) {
            Task<Void> task = new Task<>() {
                @Override
                protected Void call() throws Exception {
                    kafkaAdminService.deleteConsumerGroup(localKafkaConfig, groupId);
                    return null;
                }
            };

            task.setOnSucceeded(e -> {
                appendLog("Deleted consumer group: " + groupId);
                loadGroups();
            });

            task.setOnFailed(e -> {
                showError("Failed to Delete Group", task.getException().getMessage());
            });

            new Thread(task).start();
        }
    }

    private void handleResetGroup(String groupId) {
        // Find the group info to pass to the dialog
        GroupRow group = groups.stream()
                .filter(g -> g.groupId.equals(groupId))
                .findFirst()
                .orElse(null);

        if (group == null) {
            showError("Group Not Found", "Could not find group: " + groupId);
            return;
        }

        showConsumerGroupDialog(groupId, group.state, group.members, false);
    }

    @FXML
    private void handleCreateGroup() {
        // Prompt for new group ID
        TextInputDialog dialog = new TextInputDialog("my-consumer-group");
        dialog.setTitle("Create Consumer Group");
        dialog.setHeaderText("Create a new consumer group");
        dialog.setContentText("Group ID:");

        dialog.showAndWait().ifPresent(groupId -> {
            if (groupId.trim().isEmpty()) {
                showError("Invalid Group ID", "Group ID cannot be empty");
                return;
            }

            showConsumerGroupDialog(groupId.trim(), "NEW", 0, true);
        });
    }

    private void showConsumerGroupDialog(String groupId, String state, int members, boolean isNewGroup) {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/consumer-group-dialog.fxml"));
            loader.setControllerFactory(clazz -> {
                if (clazz == ConsumerGroupDialogController.class) {
                    return new ConsumerGroupDialogController(kafkaAdminService);
                }
                throw new RuntimeException("Unknown controller: " + clazz);
            });

            DialogPane dialogPane = loader.load();
            ConsumerGroupDialogController controller = loader.getController();

            if (isNewGroup) {
                controller.initializeForNewGroup(localKafkaConfig);
                controller.setNewGroupId(groupId);
            } else {
                controller.initializeForGroup(localKafkaConfig, groupId, state, members);
            }

            Dialog<Void> dialog = new Dialog<>();
            dialog.setTitle(isNewGroup ? "Create Consumer Group" : "Manage Consumer Group");
            dialog.setDialogPane(dialogPane);
            dialog.showAndWait();

            // Refresh groups after dialog closes
            loadGroups();

        } catch (Exception e) {
            log.error("Failed to open consumer group dialog", e);
            showError("Error", "Failed to open consumer group dialog: " + e.getMessage());
        }
    }

    @FXML
    private void handleClearLogs() {
        logsArea.clear();
    }

    @FXML
    private void handleCopyLogs() {
        ClipboardContent content = new ClipboardContent();
        content.putString(logsArea.getText());
        Clipboard.getSystemClipboard().setContent(content);
        appendLog("Copied logs to clipboard");
    }

    // ============== Data Loading ==============

    private void loadTopics() {
        if (localKafkaService.getStatus() != LocalKafkaStatus.RUNNING) {
            return;
        }

        // Show loading overlay, hide table
        Platform.runLater(() -> {
            topicsTable.setVisible(false);
            topicsLoadingPane.setVisible(true);
            topicsLoadingPane.setManaged(true);
        });

        Task<List<TopicRow>> task = new Task<>() {
            @Override
            protected List<TopicRow> call() throws Exception {
                List<TopicRow> result = new ArrayList<>();
                Map<String, Integer> topicsWithPartitions = kafkaAdminService.listTopicsWithPartitions(localKafkaConfig);

                for (Map.Entry<String, Integer> entry : topicsWithPartitions.entrySet()) {
                    String topicName = entry.getKey();
                    int partitions = entry.getValue();

                    // Get message count
                    long messageCount = 0;
                    try {
                        Map<Integer, OffsetRange> offsets = kafkaAdminService.getTopicOffsets(localKafkaConfig, topicName);
                        for (OffsetRange range : offsets.values()) {
                            messageCount += range.getEnd() - range.getBeginning();
                        }
                    } catch (Exception e) {
                        log.debug("Failed to get offsets for {}: {}", topicName, e.getMessage());
                    }

                    result.add(new TopicRow(topicName, partitions, messageCount));
                }

                result.sort(Comparator.comparing(r -> r.name));
                return result;
            }
        };

        task.setOnSucceeded(e -> Platform.runLater(() -> {
            topicsLoadingPane.setVisible(false);
            topicsLoadingPane.setManaged(false);
            topicsTable.setVisible(true);
            topics.setAll(task.getValue());
            appendLog("Loaded " + topics.size() + " topics");
        }));

        task.setOnFailed(e -> Platform.runLater(() -> {
            topicsLoadingPane.setVisible(false);
            topicsLoadingPane.setManaged(false);
            topicsTable.setVisible(true);
            log.error("Failed to load topics", task.getException());
            appendLog("Failed to load topics: " + task.getException().getMessage());
        }));

        new Thread(task).start();
    }

    private void loadGroups() {
        if (localKafkaService.getStatus() != LocalKafkaStatus.RUNNING) {
            return;
        }

        // Show loading overlay, hide table
        Platform.runLater(() -> {
            groupsTable.setVisible(false);
            groupsLoadingPane.setVisible(true);
            groupsLoadingPane.setManaged(true);
        });

        Task<List<GroupRow>> task = new Task<>() {
            @Override
            protected List<GroupRow> call() throws Exception {
                List<GroupRow> result = new ArrayList<>();
                List<ConsumerGroupInfo> groupInfos = kafkaAdminService.listConsumerGroups(localKafkaConfig);

                for (ConsumerGroupInfo info : groupInfos) {
                    result.add(new GroupRow(
                            info.getGroupId(),
                            info.getState(),
                            info.getMemberCount(),
                            0L // Lag would require per-topic calculation
                    ));
                }

                result.sort(Comparator.comparing(r -> r.groupId));
                return result;
            }
        };

        task.setOnSucceeded(e -> Platform.runLater(() -> {
            groupsLoadingPane.setVisible(false);
            groupsLoadingPane.setManaged(false);
            groupsTable.setVisible(true);
            groups.setAll(task.getValue());
            appendLog("Loaded " + groups.size() + " consumer groups");
        }));

        task.setOnFailed(e -> Platform.runLater(() -> {
            groupsLoadingPane.setVisible(false);
            groupsLoadingPane.setManaged(false);
            groupsTable.setVisible(true);
            log.error("Failed to load groups", task.getException());
        }));

        new Thread(task).start();
    }

    private void showError(String title, String message) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Error");
            alert.setHeaderText(title);
            alert.setContentText(message);
            alert.showAndWait();
        });
    }

    /**
     * Cleanup when dialog closes.
     */
    public void cleanup() {
        stopStatsPolling();
        localKafkaService.setStatusListener(null);
        localKafkaService.setLogListener(null);
    }

    // ============== Data Classes ==============

    public static class TopicRow {
        final String name;
        final int partitions;
        final long messages;

        TopicRow(String name, int partitions, long messages) {
            this.name = name;
            this.partitions = partitions;
            this.messages = messages;
        }
    }

    public static class GroupRow {
        final String groupId;
        final String state;
        final int members;
        final long totalLag;

        GroupRow(String groupId, String state, int members, long totalLag) {
            this.groupId = groupId;
            this.state = state;
            this.members = members;
            this.totalLag = totalLag;
        }
    }

    private record TopicCreationResult(String name, int partitions, int replication) {}
}
