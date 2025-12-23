package com.streamlens.controller;

import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.model.connection.ConnectionProfile.ConnectionType;
import com.streamlens.model.filter.FilterPreset;
import com.streamlens.model.message.MessageHeader;
import com.streamlens.model.message.StreamMessage;
import com.streamlens.model.streaming.StreamingOptions;
import com.streamlens.service.browser.TopicBrowserService;
import com.streamlens.service.connection.ConnectionService;
import com.streamlens.service.export.ExportService;
import com.streamlens.service.filter.FilterService;
import com.streamlens.service.api.LocalApiService;
import com.streamlens.service.kafka.KafkaAdminService;
import com.streamlens.service.kafka.KafkaProducerService;
import com.streamlens.service.kafka.LocalKafkaService;
import com.streamlens.service.storage.ConnectionRepository;
import com.streamlens.service.storage.DatabaseService;
import com.streamlens.service.storage.MessageRepository;
import com.streamlens.service.streaming.EventHubsStreamingService;
import com.streamlens.service.streaming.KafkaStreamingService;
import javafx.stage.FileChooser;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.HBox;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Main controller for the StreamLens application window.
 * Manages the connection tree, message table, and inspector panel.
 */
@Component
public class MainController implements Initializable {

    private static final Logger log = LoggerFactory.getLogger(MainController.class);
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    // Services
    private final ApplicationContext applicationContext;
    private final DatabaseService databaseService;
    private final ConnectionService connectionService;
    private final ConnectionRepository connectionRepository;
    private final TopicBrowserService topicBrowserService;
    private final KafkaStreamingService kafkaStreamingService;
    private final EventHubsStreamingService eventHubsStreamingService;
    private final KafkaAdminService kafkaAdminService;
    private final KafkaProducerService kafkaProducerService;
    private final LocalKafkaService localKafkaService;
    private final FilterService filterService;
    private final ExportService exportService;
    private final MessageRepository messageRepository;
    private final LocalApiService localApiService;

    // Toolbar
    @FXML private ToolBar toolbar;
    @FXML private Button connectButton;
    @FXML private Button disconnectButton;
    @FXML private Button startButton;
    @FXML private Button pauseButton;
    @FXML private Button stopButton;
    @FXML private Button filterButton;
    @FXML private Button exportButton;
    @FXML private Button produceButton;
    @FXML private Button localKafkaButton;
    @FXML private Button aboutButton;
    @FXML private Button settingsButton;

    // Connection Tree
    @FXML private TreeView<String> connectionTree;
    @FXML private ProgressIndicator connectionLoadingIndicator;
    @FXML private Button addConnectionButton;
    @FXML private Button editConnectionButton;
    @FXML private Button deleteConnectionButton;

    // Message Table
    @FXML private TableView<StreamMessage> messageTable;
    @FXML private TableColumn<StreamMessage, String> starColumn;
    @FXML private TableColumn<StreamMessage, Long> offsetColumn;
    @FXML private TableColumn<StreamMessage, Integer> partitionColumn;
    @FXML private TableColumn<StreamMessage, String> keyColumn;
    @FXML private TableColumn<StreamMessage, String> timestampColumn;
    @FXML private TableColumn<StreamMessage, String> previewColumn;
    @FXML private Label messageCountLabel;
    @FXML private Label streamingStatusLabel;

    // Filter Status Bar
    @FXML private HBox filterStatusBar;
    @FXML private Label filterSummaryLabel;
    @FXML private Button clearFilterButton;

    // Inspector Panel
    @FXML private ToggleGroup viewToggleGroup;
    @FXML private ToggleButton jsonViewButton;
    @FXML private ToggleButton rawViewButton;
    @FXML private ToggleButton hexViewButton;
    @FXML private TextArea messageBody;
    @FXML private TableView<MessageHeader> headersTable;
    @FXML private TableColumn<MessageHeader, String> headerKeyColumn;
    @FXML private TableColumn<MessageHeader, String> headerValueColumn;
    @FXML private Button copyButton;
    @FXML private Button saveMessageButton;

    // Status Bar
    @FXML private Label statusLabel;
    @FXML private Label connectionStatusLabel;

    // Data
    private final ObservableList<StreamMessage> messages = FXCollections.observableArrayList();
    private final ObservableList<MessageHeader> headers = FXCollections.observableArrayList();
    private final Map<String, ConnectionProfile> connectionProfiles = new HashMap<>();
    private final Map<String, TreeItem<String>> connectionTreeItems = new HashMap<>();

    private StreamMessage selectedMessage;
    private ConnectionProfile selectedConnection;
    private String selectedTopic;
    private Integer selectedPartition;
    private String activeStreamingSessionId;
    private FilterPreset activeFilterPreset;
    private final ObservableList<StreamMessage> unfilteredMessages = FXCollections.observableArrayList();
    private javafx.application.HostServices hostServices;

    public MainController(
            ApplicationContext applicationContext,
            DatabaseService databaseService,
            ConnectionService connectionService,
            ConnectionRepository connectionRepository,
            TopicBrowserService topicBrowserService,
            KafkaStreamingService kafkaStreamingService,
            EventHubsStreamingService eventHubsStreamingService,
            KafkaAdminService kafkaAdminService,
            KafkaProducerService kafkaProducerService,
            LocalKafkaService localKafkaService,
            FilterService filterService,
            ExportService exportService,
            MessageRepository messageRepository,
            LocalApiService localApiService) {
        this.applicationContext = applicationContext;
        this.databaseService = databaseService;
        this.connectionService = connectionService;
        this.connectionRepository = connectionRepository;
        this.topicBrowserService = topicBrowserService;
        this.kafkaStreamingService = kafkaStreamingService;
        this.eventHubsStreamingService = eventHubsStreamingService;
        this.kafkaAdminService = kafkaAdminService;
        this.kafkaProducerService = kafkaProducerService;
        this.localKafkaService = localKafkaService;
        this.filterService = filterService;
        this.exportService = exportService;
        this.messageRepository = messageRepository;
        this.localApiService = localApiService;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        setupConnectionTree();
        setupMessageTable();
        setupHeadersTable();
        setupInspector();
        loadConnections();
        initializeLocalApi();

        // Setup keyboard shortcuts when scene becomes available
        connectionTree.sceneProperty().addListener((obs, oldScene, newScene) -> {
            if (newScene != null) {
                setupKeyboardShortcuts(newScene);
            }
        });

        log.info("MainController initialized");
        updateStatus("Ready");
    }

    private void setupKeyboardShortcuts(Scene scene) {
        // Cmd+N: New connection
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.N, KeyCombination.SHORTCUT_DOWN),
                this::handleAddConnection
        );

        // Cmd+E: Edit connection
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.E, KeyCombination.SHORTCUT_DOWN),
                () -> { if (!editConnectionButton.isDisabled()) handleEditConnection(); }
        );

        // Cmd+Backspace: Delete connection
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.BACK_SPACE, KeyCombination.SHORTCUT_DOWN),
                () -> { if (!deleteConnectionButton.isDisabled()) handleDeleteConnection(); }
        );

        // Cmd+R: Start/Resume streaming
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.R, KeyCombination.SHORTCUT_DOWN),
                () -> { if (!startButton.isDisabled()) handleStart(); }
        );

        // Cmd+P: Pause streaming
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.P, KeyCombination.SHORTCUT_DOWN),
                () -> { if (!pauseButton.isDisabled()) handlePause(); }
        );

        // Cmd+.: Stop streaming
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.PERIOD, KeyCombination.SHORTCUT_DOWN),
                () -> { if (!stopButton.isDisabled()) handleStop(); }
        );

        // Cmd+F: Filter
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.F, KeyCombination.SHORTCUT_DOWN),
                this::handleFilter
        );

        // Cmd+Shift+E: Export
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.E, KeyCombination.SHORTCUT_DOWN, KeyCombination.SHIFT_DOWN),
                () -> { if (!exportButton.isDisabled()) handleExport(); }
        );

        // Cmd+C: Copy (when message selected)
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN),
                () -> { if (!copyButton.isDisabled()) handleCopy(); }
        );

        // Cmd+S: Save message
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.S, KeyCombination.SHORTCUT_DOWN),
                () -> { if (!saveMessageButton.isDisabled()) handleSaveMessage(); }
        );

        // Cmd+,: Settings
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.COMMA, KeyCombination.SHORTCUT_DOWN),
                this::handleSettings
        );

        // Cmd+1/2/3: Switch view modes
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.DIGIT1, KeyCombination.SHORTCUT_DOWN),
                () -> jsonViewButton.setSelected(true)
        );
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.DIGIT2, KeyCombination.SHORTCUT_DOWN),
                () -> rawViewButton.setSelected(true)
        );
        scene.getAccelerators().put(
                new KeyCodeCombination(KeyCode.DIGIT3, KeyCombination.SHORTCUT_DOWN),
                () -> hexViewButton.setSelected(true)
        );

        log.debug("Keyboard shortcuts configured");
    }

    private void initializeLocalApi() {
        // Set message supplier for API
        localApiService.setMessageSupplier(() -> new ArrayList<>(messages));

        // Start API if enabled
        String apiEnabled = databaseService.getSetting("api.enabled", "false");
        if (Boolean.parseBoolean(apiEnabled)) {
            String portStr = databaseService.getSetting("api.port", "9876");
            try {
                int port = Integer.parseInt(portStr);
                localApiService.start(port);
            } catch (Exception e) {
                log.error("Failed to start local API", e);
            }
        }
    }

    private static final String SAVED_MESSAGES_NODE = "\u2605 Saved Messages";
    private static final String STARRED_NODE = "Starred";
    private static final String FLAGGED_NODE = "Flagged";

    private void setupConnectionTree() {
        TreeItem<String> root = new TreeItem<>("Connections");
        root.setExpanded(true);
        connectionTree.setRoot(root);
        connectionTree.setShowRoot(false);

        // Add Saved Messages node at the top
        TreeItem<String> savedMessagesItem = new TreeItem<>(SAVED_MESSAGES_NODE);
        savedMessagesItem.getChildren().add(new TreeItem<>(STARRED_NODE));
        savedMessagesItem.getChildren().add(new TreeItem<>(FLAGGED_NODE));
        root.getChildren().add(savedMessagesItem);

        connectionTree.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            handleTreeSelection(newVal);
        });

        // Setup context menus for tree items
        connectionTree.setCellFactory(tv -> new TreeCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setContextMenu(null);
                } else {
                    setText(item);
                    setContextMenu(createTreeContextMenu(getTreeItem()));
                }
            }
        });
    }

    private ContextMenu createTreeContextMenu(TreeItem<String> item) {
        if (item == null) return null;

        int depth = getTreeItemDepth(item);
        String itemValue = item.getValue();

        // Skip context menu for Saved Messages and its children
        if (SAVED_MESSAGES_NODE.equals(itemValue) || STARRED_NODE.equals(itemValue) || FLAGGED_NODE.equals(itemValue)) {
            return null;
        }

        TreeItem<String> parent = item.getParent();
        if (parent != null && SAVED_MESSAGES_NODE.equals(parent.getValue())) {
            return null;
        }

        ContextMenu menu = new ContextMenu();

        if (depth == 1) {
            // Connection level
            MenuItem refreshItem = new MenuItem("Refresh Topics");
            refreshItem.setOnAction(e -> {
                ConnectionProfile profile = connectionProfiles.get(itemValue);
                if (profile != null) {
                    loadTopicsForConnection(item, profile);
                }
            });
            menu.getItems().add(refreshItem);

        } else if (depth == 2) {
            // Topic level
            TreeItem<String> connectionItem = item.getParent();
            ConnectionProfile profile = connectionProfiles.get(connectionItem.getValue());

            if (profile != null && profile.getType() == ConnectionType.KAFKA) {
                MenuItem infoItem = new MenuItem("Topic Info...");
                infoItem.setOnAction(e -> showTopicInfoDialog(profile, itemValue));

                MenuItem produceItem = new MenuItem("Produce Messages...");
                produceItem.setOnAction(e -> showProduceMessageDialog(profile, itemValue));

                MenuItem seekItem = new MenuItem("Seek to Offset...");
                seekItem.setOnAction(e -> {
                    selectedConnection = profile;
                    selectedTopic = itemValue;
                    selectedPartition = null;
                    showStreamingOptionsDialog();
                });

                menu.getItems().addAll(infoItem, new SeparatorMenuItem(), produceItem, new SeparatorMenuItem(), seekItem);

            } else if (profile != null && profile.getType() == ConnectionType.EVENTHUBS) {
                MenuItem infoItem = new MenuItem("Event Hub Info...");
                infoItem.setOnAction(e -> showEventHubInfoDialog(profile));

                menu.getItems().add(infoItem);
            }
        }

        return menu.getItems().isEmpty() ? null : menu;
    }

    private void setupMessageTable() {
        messageTable.setItems(messages);

        starColumn.setCellValueFactory(data ->
                new SimpleStringProperty(data.getValue().isStarred() ? "\u2605" : ""));
        starColumn.setStyle("-fx-alignment: CENTER;");

        offsetColumn.setCellValueFactory(new PropertyValueFactory<>("offset"));
        partitionColumn.setCellValueFactory(new PropertyValueFactory<>("partition"));

        keyColumn.setCellValueFactory(data ->
                new SimpleStringProperty(truncate(data.getValue().getKey(), 50)));

        timestampColumn.setCellValueFactory(data -> {
            Instant ts = data.getValue().getTimestamp();
            return new SimpleStringProperty(ts != null ? TIMESTAMP_FORMAT.format(ts) : "");
        });

        previewColumn.setCellValueFactory(data ->
                new SimpleStringProperty(truncate(data.getValue().getValueAsString(), 100)));

        messageTable.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            selectedMessage = newVal;
            displayMessage(newVal);
            copyButton.setDisable(newVal == null);
            saveMessageButton.setDisable(newVal == null);
        });

        messageTable.setOnMouseClicked(event -> {
            if (event.getClickCount() == 2 && selectedMessage != null) {
                toggleStar(selectedMessage);
            }
        });

        ContextMenu contextMenu = new ContextMenu();
        MenuItem starItem = new MenuItem("Toggle Star");
        starItem.setOnAction(e -> { if (selectedMessage != null) toggleStar(selectedMessage); });
        MenuItem flagItem = new MenuItem("Toggle Flag");
        flagItem.setOnAction(e -> { if (selectedMessage != null) toggleFlag(selectedMessage); });
        MenuItem copyItem = new MenuItem("Copy Value");
        copyItem.setOnAction(e -> handleCopy());
        contextMenu.getItems().addAll(starItem, flagItem, new SeparatorMenuItem(), copyItem);
        messageTable.setContextMenu(contextMenu);
    }

    private void setupHeadersTable() {
        headersTable.setItems(headers);
        headerKeyColumn.setCellValueFactory(new PropertyValueFactory<>("key"));
        headerValueColumn.setCellValueFactory(new PropertyValueFactory<>("value"));
    }

    private void setupInspector() {
        viewToggleGroup.selectedToggleProperty().addListener((obs, oldVal, newVal) -> {
            if (selectedMessage != null) {
                displayMessage(selectedMessage);
            }
        });
    }

    private void loadConnections() {
        TreeItem<String> root = connectionTree.getRoot();

        // Remove all items except Saved Messages
        root.getChildren().removeIf(item -> !SAVED_MESSAGES_NODE.equals(item.getValue()));

        // Re-add Saved Messages if it was removed
        if (root.getChildren().isEmpty() ||
                !SAVED_MESSAGES_NODE.equals(root.getChildren().get(0).getValue())) {
            TreeItem<String> savedMessagesItem = new TreeItem<>(SAVED_MESSAGES_NODE);
            savedMessagesItem.getChildren().add(new TreeItem<>(STARRED_NODE));
            savedMessagesItem.getChildren().add(new TreeItem<>(FLAGGED_NODE));
            root.getChildren().add(0, savedMessagesItem);
        }

        connectionProfiles.clear();
        connectionTreeItems.clear();

        List<ConnectionProfile> profiles = connectionRepository.findAll();
        for (ConnectionProfile profile : profiles) {
            addConnectionToTree(profile);
        }

        if (profiles.isEmpty()) {
            updateStatus("No connections configured. Click '+' to add one.");
        } else {
            updateStatus("Loaded " + profiles.size() + " connection(s)");
        }
    }

    private void addConnectionToTree(ConnectionProfile profile) {
        TreeItem<String> item = new TreeItem<>(profile.getName());
        connectionTree.getRoot().getChildren().add(item);
        connectionProfiles.put(profile.getName(), profile);
        connectionTreeItems.put(profile.getId(), item);
    }

    private void handleTreeSelection(TreeItem<String> item) {
        if (item == null) {
            selectedConnection = null;
            selectedTopic = null;
            selectedPartition = null;
            updateControlStates();
            return;
        }

        String itemValue = item.getValue();

        // Check if this is a Saved Messages node
        if (SAVED_MESSAGES_NODE.equals(itemValue)) {
            // Parent node - expand to show children
            selectedConnection = null;
            selectedTopic = null;
            selectedPartition = null;
            editConnectionButton.setDisable(true);
            deleteConnectionButton.setDisable(true);
            item.setExpanded(true);
            updateControlStates();
            return;
        }

        // Check if this is a child of Saved Messages
        TreeItem<String> parent = item.getParent();
        if (parent != null && SAVED_MESSAGES_NODE.equals(parent.getValue())) {
            selectedConnection = null;
            selectedTopic = null;
            selectedPartition = null;
            editConnectionButton.setDisable(true);
            deleteConnectionButton.setDisable(true);

            if (STARRED_NODE.equals(itemValue)) {
                loadStarredMessages();
            } else if (FLAGGED_NODE.equals(itemValue)) {
                loadFlaggedMessages();
            }
            updateControlStates();
            return;
        }

        int depth = getTreeItemDepth(item);

        if (depth == 1) {
            // Connection level
            selectedConnection = connectionProfiles.get(item.getValue());
            selectedTopic = null;
            selectedPartition = null;
            editConnectionButton.setDisable(false);
            deleteConnectionButton.setDisable(false);

            // Load topics if not already loaded
            if (item.getChildren().isEmpty() && selectedConnection != null) {
                loadTopicsForConnection(item, selectedConnection);
            }
        } else if (depth == 2) {
            // Topic level
            TreeItem<String> connectionItem = item.getParent();
            selectedConnection = connectionProfiles.get(connectionItem.getValue());
            selectedTopic = item.getValue();
            selectedPartition = null;
            editConnectionButton.setDisable(true);
            deleteConnectionButton.setDisable(true);
        } else if (depth == 3) {
            // Partition level
            TreeItem<String> topicItem = item.getParent();
            TreeItem<String> connectionItem = topicItem.getParent();
            selectedConnection = connectionProfiles.get(connectionItem.getValue());
            selectedTopic = topicItem.getValue();
            String partitionStr = item.getValue().replace("Partition ", "");
            selectedPartition = Integer.parseInt(partitionStr);
            editConnectionButton.setDisable(true);
            deleteConnectionButton.setDisable(true);
        }

        updateControlStates();
    }

    private void loadStarredMessages() {
        updateStatus("Loading starred messages...");
        Task<List<StreamMessage>> task = new Task<>() {
            @Override
            protected List<StreamMessage> call() {
                return messageRepository.findStarred();
            }
        };

        task.setOnSucceeded(event -> {
            List<StreamMessage> starredMessages = task.getValue();
            Platform.runLater(() -> {
                messages.clear();
                unfilteredMessages.clear();
                messages.addAll(starredMessages);
                unfilteredMessages.addAll(starredMessages);
                updateMessageCount();
                updateStatus("Loaded " + starredMessages.size() + " starred message(s)");
            });
        });

        task.setOnFailed(event -> {
            Platform.runLater(() -> {
                updateStatus("Failed to load starred messages: " + task.getException().getMessage());
                showError("Failed to Load", task.getException().getMessage());
            });
        });

        new Thread(task).start();
    }

    private void loadFlaggedMessages() {
        updateStatus("Loading flagged messages...");
        Task<List<StreamMessage>> task = new Task<>() {
            @Override
            protected List<StreamMessage> call() {
                return messageRepository.findFlagged();
            }
        };

        task.setOnSucceeded(event -> {
            List<StreamMessage> flaggedMessages = task.getValue();
            Platform.runLater(() -> {
                messages.clear();
                unfilteredMessages.clear();
                messages.addAll(flaggedMessages);
                unfilteredMessages.addAll(flaggedMessages);
                updateMessageCount();
                updateStatus("Loaded " + flaggedMessages.size() + " flagged message(s)");
            });
        });

        task.setOnFailed(event -> {
            Platform.runLater(() -> {
                updateStatus("Failed to load flagged messages: " + task.getException().getMessage());
                showError("Failed to Load", task.getException().getMessage());
            });
        });

        new Thread(task).start();
    }

    private void loadTopicsForConnection(TreeItem<String> connectionItem, ConnectionProfile profile) {
        updateStatus("Loading topics...");

        // Show loading indicator
        connectionLoadingIndicator.setVisible(true);
        connectionLoadingIndicator.setManaged(true);

        Task<Map<String, Integer>> task = new Task<>() {
            @Override
            protected Map<String, Integer> call() {
                return topicBrowserService.getTopicsWithPartitions(profile);
            }
        };

        task.setOnSucceeded(event -> {
            Map<String, Integer> topics = task.getValue();
            Platform.runLater(() -> {
                connectionLoadingIndicator.setVisible(false);
                connectionLoadingIndicator.setManaged(false);

                connectionItem.getChildren().clear();
                for (Map.Entry<String, Integer> entry : topics.entrySet()) {
                    TreeItem<String> topicItem = new TreeItem<>(entry.getKey());
                    for (int i = 0; i < entry.getValue(); i++) {
                        topicItem.getChildren().add(new TreeItem<>("Partition " + i));
                    }
                    connectionItem.getChildren().add(topicItem);
                }
                connectionItem.setExpanded(true);
                updateStatus("Loaded " + topics.size() + " topic(s)");
            });
        });

        task.setOnFailed(event -> {
            Platform.runLater(() -> {
                connectionLoadingIndicator.setVisible(false);
                connectionLoadingIndicator.setManaged(false);

                updateStatus("Failed to load topics: " + task.getException().getMessage());
                showError("Failed to Load Topics", task.getException().getMessage());
            });
        });

        new Thread(task).start();
    }

    private void updateControlStates() {
        boolean canStream = selectedTopic != null;
        boolean isStreaming = activeStreamingSessionId != null;
        boolean canProduce = selectedTopic != null && selectedConnection != null &&
                selectedConnection.getType() == ConnectionType.KAFKA;

        startButton.setDisable(!canStream || isStreaming);
        pauseButton.setDisable(!isStreaming);
        stopButton.setDisable(!isStreaming);
        exportButton.setDisable(messages.isEmpty());
        produceButton.setDisable(!canProduce);

        if (selectedConnection != null) {
            connectionStatusLabel.setText("Selected: " + selectedConnection.getName());
        } else {
            connectionStatusLabel.setText("No connection selected");
        }
    }

    private int getTreeItemDepth(TreeItem<String> item) {
        int depth = 0;
        TreeItem<String> current = item;
        while (current.getParent() != null) {
            depth++;
            current = current.getParent();
        }
        return depth;
    }

    // ============== Event Handlers ==============

    @FXML
    private void handleConnect() {
        if (selectedConnection == null) {
            showError("No Connection Selected", "Please select a connection from the tree.");
            return;
        }
        loadTopicsForConnection(connectionTreeItems.get(selectedConnection.getId()), selectedConnection);
    }

    @FXML
    private void handleDisconnect() {
        handleStop();
        updateStatus("Disconnected");
    }

    @FXML
    private void handleStart() {
        if (selectedConnection == null || selectedTopic == null) {
            showError("Cannot Start Streaming", "Please select a topic or partition to stream from.");
            return;
        }

        // For Kafka connections, show the streaming options dialog
        if (selectedConnection.getType() == ConnectionType.KAFKA) {
            showStreamingOptionsDialog();
        } else {
            // For Event Hubs, use default options (can be enhanced later)
            startStreamingWithOptions(StreamingOptions.latest());
        }
    }

    private void showStreamingOptionsDialog() {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/streaming-options-dialog.fxml"));
            loader.setControllerFactory(applicationContext::getBean);
            DialogPane dialogPane = loader.load();

            StreamingOptionsDialogController controller = loader.getController();
            controller.setContext(selectedConnection, selectedTopic, selectedPartition);

            Dialog<ButtonType> dialog = new Dialog<>();
            dialog.setTitle("Streaming Options");
            dialog.setDialogPane(dialogPane);
            dialog.initOwner(connectionTree.getScene().getWindow());

            Optional<ButtonType> result = dialog.showAndWait();
            if (result.isPresent() && result.get() == ButtonType.OK) {
                String validationError = controller.validate();
                if (validationError != null) {
                    showError("Invalid Options", validationError);
                    return;
                }

                StreamingOptions options = controller.buildOptions();
                startStreamingWithOptions(options);
            }
        } catch (Exception e) {
            log.error("Failed to show streaming options dialog", e);
            showError("Error", "Failed to open streaming options: " + e.getMessage());
        }
    }

    private void startStreamingWithOptions(StreamingOptions options) {
        clearMessages();
        updateStatus("Starting stream...");

        if (selectedConnection.getType() == ConnectionType.KAFKA) {
            activeStreamingSessionId = kafkaStreamingService.startStreaming(
                    selectedConnection.getId(),
                    selectedConnection.getKafkaConfig(),
                    selectedTopic,
                    selectedPartition,
                    options,
                    this::addMessage
            );
        } else {
            String partitionId = selectedPartition != null ? String.valueOf(selectedPartition) : null;
            activeStreamingSessionId = eventHubsStreamingService.startStreaming(
                    selectedConnection.getId(),
                    selectedConnection.getEventHubsConfig(),
                    partitionId,
                    EventHubsStreamingService.StartPosition.LATEST,
                    this::addMessage
            );
        }

        startButton.setDisable(true);
        pauseButton.setDisable(false);
        stopButton.setDisable(false);
        streamingStatusLabel.setText("Streaming...");

        String statusMsg = "Streaming from " + selectedTopic;
        if (options.getStartPosition() != StreamingOptions.StartPosition.LATEST) {
            statusMsg += " (" + options.getStartPosition().getDisplayName() + ")";
        }
        if (options.getConsumerGroup() != null && !options.getConsumerGroup().isEmpty()) {
            statusMsg += " [Group: " + options.getConsumerGroup() + "]";
        }
        updateStatus(statusMsg);
    }

    @FXML
    private void handlePause() {
        if (activeStreamingSessionId != null) {
            if (selectedConnection.getType() == ConnectionType.KAFKA) {
                kafkaStreamingService.pauseStreaming(activeStreamingSessionId);
            } else {
                eventHubsStreamingService.pauseStreaming(activeStreamingSessionId);
            }
        }
        pauseButton.setDisable(true);
        startButton.setDisable(false);
        startButton.setText("Resume");
        streamingStatusLabel.setText("Paused");
        updateStatus("Streaming paused");
    }

    @FXML
    private void handleStop() {
        if (activeStreamingSessionId != null) {
            if (selectedConnection != null && selectedConnection.getType() == ConnectionType.KAFKA) {
                kafkaStreamingService.stopStreaming(activeStreamingSessionId);
            } else if (selectedConnection != null) {
                eventHubsStreamingService.stopStreaming(activeStreamingSessionId);
            }
            activeStreamingSessionId = null;
        }

        startButton.setDisable(false);
        startButton.setText("Start");
        pauseButton.setDisable(true);
        stopButton.setDisable(true);
        streamingStatusLabel.setText("");
        updateStatus("Streaming stopped. " + messages.size() + " messages received.");
    }

    @FXML
    private void handleFilter() {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/filter-dialog.fxml"));
            loader.setControllerFactory(applicationContext::getBean);
            DialogPane dialogPane = loader.load();

            FilterDialogController controller = loader.getController();
            controller.setCurrentPreset(activeFilterPreset);

            Dialog<ButtonType> dialog = new Dialog<>();
            dialog.setTitle("Filter Messages");
            dialog.setDialogPane(dialogPane);

            dialog.showAndWait().ifPresent(response -> {
                if (response.getButtonData() == ButtonBar.ButtonData.OK_DONE) {
                    // Apply Filter button
                    String validationError = controller.validate();
                    if (validationError != null) {
                        showError("Validation Error", validationError);
                        return;
                    }

                    activeFilterPreset = controller.buildPreset();
                    applyFilter();
                } else if (response.getButtonData() == ButtonBar.ButtonData.OTHER) {
                    // Clear All button
                    controller.clearAll();
                    activeFilterPreset = null;
                    applyFilter();
                }
                // CANCEL does nothing - keeps current filter
            });
        } catch (Exception e) {
            log.error("Failed to show filter dialog", e);
            showError("Error", "Failed to open filter dialog: " + e.getMessage());
        }
    }

    private void applyFilter() {
        if (activeFilterPreset == null || !activeFilterPreset.hasActiveFilters()) {
            // No filter - show all messages
            messages.setAll(unfilteredMessages);
            filterButton.setStyle("");
            filterStatusBar.setVisible(false);
            filterStatusBar.setManaged(false);
            updateStatus("Filter cleared. Showing all " + messages.size() + " messages.");
        } else {
            // Apply filter
            List<StreamMessage> filtered = filterService.filter(
                    new ArrayList<>(unfilteredMessages), activeFilterPreset);
            messages.setAll(filtered);
            filterButton.setStyle("-fx-background-color: #4caf50;");

            // Update filter status bar
            filterSummaryLabel.setText(buildFilterSummary(activeFilterPreset));
            filterStatusBar.setVisible(true);
            filterStatusBar.setManaged(true);

            updateStatus("Filter applied. Showing " + filtered.size() + " of " + unfilteredMessages.size() + " messages.");
        }
        updateMessageCount();
    }

    private String buildFilterSummary(FilterPreset preset) {
        List<String> parts = new ArrayList<>();

        if (preset.hasQuickSearch()) {
            parts.add("Search: \"" + truncate(preset.getQuickSearch(), 20) + "\"");
        }
        if (preset.hasKeyFilter()) {
            String keyDesc = preset.isKeyPatternIsRegex() ? "Key (regex): " : "Key: ";
            parts.add(keyDesc + "\"" + truncate(preset.getKeyPattern(), 15) + "\"");
        }
        if (preset.hasJsonPathFilter()) {
            parts.add("JSONPath: " + truncate(preset.getJsonPathExpression(), 20));
        }
        if (preset.hasTimeRangeFilter()) {
            parts.add("Time range active");
        }

        return String.join(" | ", parts);
    }

    @FXML
    private void handleClearFilter() {
        activeFilterPreset = null;
        applyFilter();
    }

    @FXML
    private void handleExport() {
        if (messages.isEmpty()) {
            showError("No Messages", "There are no messages to export.");
            return;
        }

        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Export Messages");
        fileChooser.setInitialFileName("messages");
        fileChooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("JSON", "*.json"),
                new FileChooser.ExtensionFilter("NDJSON", "*.ndjson"),
                new FileChooser.ExtensionFilter("CSV", "*.csv")
        );

        java.io.File file = fileChooser.showSaveDialog(messageTable.getScene().getWindow());
        if (file == null) return;

        String fileName = file.getName().toLowerCase();
        updateStatus("Exporting " + messages.size() + " messages...");

        Task<Void> task = new Task<>() {
            @Override
            protected Void call() throws Exception {
                java.util.List<StreamMessage> toExport = new java.util.ArrayList<>(messages);
                if (fileName.endsWith(".ndjson")) {
                    exportService.exportToNdjson(toExport, file, count ->
                            Platform.runLater(() -> updateStatus("Exported " + count + " messages...")));
                } else if (fileName.endsWith(".csv")) {
                    exportService.exportToCsv(toExport, file, count ->
                            Platform.runLater(() -> updateStatus("Exported " + count + " messages...")));
                } else {
                    exportService.exportToJson(toExport, file, count ->
                            Platform.runLater(() -> updateStatus("Exported " + count + " messages...")));
                }
                return null;
            }
        };

        task.setOnSucceeded(e -> updateStatus("Exported " + messages.size() + " messages to " + file.getName()));
        task.setOnFailed(e -> {
            log.error("Export failed", task.getException());
            showError("Export Failed", task.getException().getMessage());
        });

        new Thread(task).start();
    }

    @FXML
    private void handleSettings() {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/settings.fxml"));
            loader.setControllerFactory(applicationContext::getBean);
            DialogPane dialogPane = loader.load();

            SettingsController controller = loader.getController();

            Dialog<ButtonType> dialog = new Dialog<>();
            dialog.setTitle("Settings");
            dialog.setDialogPane(dialogPane);

            dialog.showAndWait().ifPresent(response -> {
                if (response == ButtonType.OK) {
                    String validationError = controller.validate();
                    if (validationError != null) {
                        showError("Validation Error", validationError);
                        return;
                    }
                    controller.saveSettings();
                    updateStatus("Settings saved");
                }
            });
        } catch (Exception e) {
            log.error("Failed to show settings dialog", e);
            showError("Error", "Failed to open settings dialog: " + e.getMessage());
        }
    }

    @FXML
    private void handleAbout() {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/about-dialog.fxml"));
            loader.setControllerFactory(applicationContext::getBean);
            DialogPane dialogPane = loader.load();

            AboutDialogController controller = loader.getController();
            controller.setHostServices(hostServices);

            Dialog<ButtonType> dialog = new Dialog<>();
            dialog.setTitle("About StreamLens");
            dialog.setDialogPane(dialogPane);
            dialog.initOwner(connectionTree.getScene().getWindow());

            dialog.showAndWait();
        } catch (Exception e) {
            log.error("Failed to show about dialog", e);
            showError("Error", "Failed to open about dialog: " + e.getMessage());
        }
    }

    @FXML
    private void handleProduceMessage() {
        if (selectedConnection == null || selectedTopic == null) {
            showError("No Topic Selected", "Please select a topic to produce messages to.");
            return;
        }
        if (selectedConnection.getType() != ConnectionType.KAFKA) {
            showError("Not Supported", "Message production is only supported for Kafka connections.");
            return;
        }
        showProduceMessageDialog(selectedConnection, selectedTopic);
    }

    /**
     * Set the host services for opening URLs in the default browser.
     * Should be called from the Application class after initialization.
     */
    public void setHostServices(javafx.application.HostServices hostServices) {
        this.hostServices = hostServices;
    }

    @FXML
    private void handleLocalKafka() {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/local-kafka-manager.fxml"));
            loader.setControllerFactory(applicationContext::getBean);
            DialogPane dialogPane = loader.load();

            LocalKafkaManagerController controller = loader.getController();

            Dialog<ButtonType> dialog = new Dialog<>();
            dialog.setTitle("Local Kafka Server");
            dialog.setDialogPane(dialogPane);
            dialog.initOwner(connectionTree.getScene().getWindow());
            dialog.setResizable(true);

            dialog.setOnHidden(e -> {
                controller.cleanup();
                // Refresh connections to show any newly created Local Kafka connection
                loadConnections();
            });

            dialog.show();
        } catch (Exception e) {
            log.error("Failed to show local Kafka manager", e);
            showError("Error", "Failed to open Local Kafka manager: " + e.getMessage());
        }
    }

    @FXML
    private void handleAddConnection() {
        showConnectionDialog(null);
    }

    @FXML
    private void handleEditConnection() {
        if (selectedConnection != null) {
            showConnectionDialog(selectedConnection);
        }
    }

    @FXML
    private void handleDeleteConnection() {
        if (selectedConnection == null) return;

        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Delete Connection");
        alert.setHeaderText("Delete '" + selectedConnection.getName() + "'?");
        alert.setContentText("This action cannot be undone.");

        alert.showAndWait().ifPresent(response -> {
            if (response == ButtonType.OK) {
                try {
                    connectionService.deleteConnection(selectedConnection.getId());
                    loadConnections();
                    updateStatus("Connection deleted");
                } catch (Exception e) {
                    showError("Failed to Delete", e.getMessage());
                }
            }
        });
    }

    private void showConnectionDialog(ConnectionProfile profile) {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/connection-dialog.fxml"));
            loader.setControllerFactory(applicationContext::getBean);
            DialogPane dialogPane = loader.load();

            ConnectionDialogController controller = loader.getController();
            controller.setProfile(profile);

            Dialog<ButtonType> dialog = new Dialog<>();
            dialog.setTitle(profile == null ? "New Connection" : "Edit Connection");
            dialog.setDialogPane(dialogPane);

            dialog.showAndWait().ifPresent(response -> {
                if (response == ButtonType.OK) {
                    String validationError = controller.validate();
                    if (validationError != null) {
                        showError("Validation Error", validationError);
                        return;
                    }

                    try {
                        ConnectionProfile newProfile = controller.buildProfile();
                        if (profile == null) {
                            connectionRepository.save(newProfile);
                            updateStatus("Connection created: " + newProfile.getName());
                        } else {
                            connectionRepository.update(newProfile);
                            updateStatus("Connection updated: " + newProfile.getName());
                        }
                        loadConnections();
                    } catch (Exception e) {
                        showError("Failed to Save", e.getMessage());
                    }
                }
            });
        } catch (Exception e) {
            log.error("Failed to show connection dialog", e);
            showError("Error", "Failed to open connection dialog: " + e.getMessage());
        }
    }

    private void showTopicInfoDialog(ConnectionProfile profile, String topic) {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/topic-info-dialog.fxml"));
            loader.setControllerFactory(applicationContext::getBean);
            DialogPane dialogPane = loader.load();

            TopicInfoDialogController controller = loader.getController();
            controller.setContext(profile, topic);

            Dialog<ButtonType> dialog = new Dialog<>();
            dialog.setTitle("Topic Information - " + topic);
            dialog.setDialogPane(dialogPane);
            dialog.initOwner(connectionTree.getScene().getWindow());
            dialog.setResizable(true);

            dialog.showAndWait();
        } catch (Exception e) {
            log.error("Failed to show topic info dialog", e);
            showError("Error", "Failed to open topic info dialog: " + e.getMessage());
        }
    }

    private void showEventHubInfoDialog(ConnectionProfile profile) {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/eventhub-info-dialog.fxml"));
            loader.setControllerFactory(applicationContext::getBean);
            DialogPane dialogPane = loader.load();

            EventHubInfoDialogController controller = loader.getController();
            controller.setContext(profile);

            // Set callback for editing connection
            controller.setOnEditConnection(() -> {
                showConnectionDialog(profile);
            });

            Dialog<ButtonType> dialog = new Dialog<>();
            dialog.setTitle("Event Hub Information - " + profile.getEventHubsConfig().getEventHubName());
            dialog.setDialogPane(dialogPane);
            dialog.initOwner(connectionTree.getScene().getWindow());
            dialog.setResizable(true);

            dialog.showAndWait();
        } catch (Exception e) {
            log.error("Failed to show Event Hub info dialog", e);
            showError("Error", "Failed to open Event Hub info dialog: " + e.getMessage());
        }
    }

    private void showProduceMessageDialog(ConnectionProfile profile, String topic) {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/produce-message-dialog.fxml"));
            loader.setControllerFactory(applicationContext::getBean);
            DialogPane dialogPane = loader.load();

            ProduceMessageDialogController controller = loader.getController();
            controller.setContext(profile, topic);

            Dialog<ButtonType> dialog = new Dialog<>();
            dialog.setTitle("Produce Messages - " + topic);
            dialog.setDialogPane(dialogPane);
            dialog.initOwner(connectionTree.getScene().getWindow());
            dialog.setResizable(true);

            controller.setParentDialog(dialog);

            // Custom result converter to handle the send action
            dialog.setResultConverter(buttonType -> {
                if (buttonType != null && buttonType.getButtonData() == ButtonBar.ButtonData.OK_DONE) {
                    String validationError = controller.validate();
                    if (validationError != null) {
                        showError("Validation Error", validationError);
                        return null;
                    }
                    if (controller.send()) {
                        return buttonType; // Close dialog on success
                    }
                    return null; // Don't close on failure
                }
                return buttonType;
            });

            dialog.showAndWait();
        } catch (Exception e) {
            log.error("Failed to show produce message dialog", e);
            showError("Error", "Failed to open produce message dialog: " + e.getMessage());
        }
    }

    @FXML
    private void handleCopy() {
        if (selectedMessage != null) {
            String content = getDisplayContent(selectedMessage);
            javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
            javafx.scene.input.ClipboardContent clipboardContent = new javafx.scene.input.ClipboardContent();
            clipboardContent.putString(content);
            clipboard.setContent(clipboardContent);
            updateStatus("Copied to clipboard");
        }
    }

    @FXML
    private void handleSaveMessage() {
        if (selectedMessage != null) {
            try {
                messageRepository.save(selectedMessage);
                updateStatus("Message saved to database");
            } catch (Exception e) {
                log.error("Failed to save message", e);
                showError("Save Failed", "Failed to save message: " + e.getMessage());
            }
        }
    }

    // ============== Helper Methods ==============

    private void displayMessage(StreamMessage message) {
        if (message == null) {
            messageBody.clear();
            headers.clear();
            return;
        }

        String content = getDisplayContent(message);
        messageBody.setText(content);

        headers.clear();
        if (message.getHeaders() != null) {
            headers.addAll(message.getHeaders());
        }
    }

    private String getDisplayContent(StreamMessage message) {
        if (jsonViewButton.isSelected()) {
            return message.getValueAsPrettyJson();
        } else if (hexViewButton.isSelected()) {
            return message.getValueAsHex();
        } else {
            return message.getValueAsString();
        }
    }

    private void toggleStar(StreamMessage message) {
        message.setStarred(!message.isStarred());
        messageTable.refresh();

        // Persist if message was previously saved
        if (messageRepository.existsById(message.getId())) {
            messageRepository.updateFlags(message.getId(), message.isStarred(), message.isFlagged());
        } else if (message.isStarred()) {
            // Auto-save when starring
            messageRepository.save(message);
            updateStatus("Message starred and saved");
        }
    }

    private void toggleFlag(StreamMessage message) {
        message.setFlagged(!message.isFlagged());
        messageTable.refresh();

        // Persist if message was previously saved
        if (messageRepository.existsById(message.getId())) {
            messageRepository.updateFlags(message.getId(), message.isStarred(), message.isFlagged());
        } else if (message.isFlagged()) {
            // Auto-save when flagging
            messageRepository.save(message);
            updateStatus("Message flagged and saved");
        }
    }

    private void updateStatus(String status) {
        Platform.runLater(() -> statusLabel.setText(status));
    }

    private void updateMessageCount() {
        Platform.runLater(() -> messageCountLabel.setText("(" + messages.size() + " messages)"));
    }

    private String truncate(String text, int maxLength) {
        if (text == null) return "";
        if (text.length() <= maxLength) return text;
        return text.substring(0, maxLength) + "...";
    }

    private void showError(String title, String message) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle(title);
            alert.setHeaderText(null);
            alert.setContentText(message);
            alert.showAndWait();
        });
    }

    public void addMessage(StreamMessage message) {
        Platform.runLater(() -> {
            // Add to unfiltered list
            unfilteredMessages.add(0, message);

            // Limit messages in memory
            if (unfilteredMessages.size() > 10000) {
                unfilteredMessages.remove(unfilteredMessages.size() - 1);
            }

            // Check if message passes filter
            boolean matchesFilter = activeFilterPreset == null || !activeFilterPreset.hasActiveFilters() ||
                    filterService.matchesFilter(message, activeFilterPreset);

            if (matchesFilter) {
                messages.add(0, message);
                if (messages.size() > 10000) {
                    messages.remove(messages.size() - 1);
                }

                // Broadcast to WebSocket clients
                if (localApiService.isRunning()) {
                    localApiService.broadcastMessage(message);
                }
            }

            updateMessageCount();
        });
    }

    public void clearMessages() {
        Platform.runLater(() -> {
            messages.clear();
            unfilteredMessages.clear();
            updateMessageCount();
        });
    }
}
