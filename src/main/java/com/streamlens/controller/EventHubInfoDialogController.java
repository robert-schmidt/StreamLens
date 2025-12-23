package com.streamlens.controller;

import com.streamlens.StreamLensApplication;
import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.model.connection.EventHubsConnectionConfig;
import com.streamlens.service.eventhubs.EventHubsAdminService;
import com.streamlens.service.eventhubs.EventHubsAdminService.*;
import javafx.application.Platform;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Controller for the Event Hub Information dialog.
 * Displays Event Hub metadata, partition details, and consumer group information.
 */
@Component
public class EventHubInfoDialogController implements Initializable {

    private static final Logger log = LoggerFactory.getLogger(EventHubInfoDialogController.class);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private final EventHubsAdminService eventHubsAdminService;

    // Overview tab
    @FXML private Label eventHubNameLabel;
    @FXML private Label namespaceLabel;
    @FXML private Label partitionCountLabel;
    @FXML private Label totalMessagesLabel;
    @FXML private Label consumerGroupLabel;

    // Partition table
    @FXML private TableView<PartitionRow> partitionTable;
    @FXML private TableColumn<PartitionRow, String> partitionIdCol;
    @FXML private TableColumn<PartitionRow, Long> beginSeqCol;
    @FXML private TableColumn<PartitionRow, Long> lastSeqCol;
    @FXML private TableColumn<PartitionRow, String> lastOffsetCol;
    @FXML private TableColumn<PartitionRow, String> lastTimeCol;
    @FXML private TableColumn<PartitionRow, Long> messageCountCol;

    // Consumer Groups tab
    @FXML private TextField currentGroupField;
    @FXML private Button openAzurePortalButton;
    @FXML private Button editConnectionButton;
    @FXML private Button refreshButton;

    private ConnectionProfile connectionProfile;
    private EventHubsConnectionConfig config;
    private final ObservableList<PartitionRow> partitionRows = FXCollections.observableArrayList();

    // Callback for editing connection
    private Runnable onEditConnection;

    public EventHubInfoDialogController(EventHubsAdminService eventHubsAdminService) {
        this.eventHubsAdminService = eventHubsAdminService;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        setupPartitionTable();
    }

    private void setupPartitionTable() {
        partitionIdCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().partitionId));
        beginSeqCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().beginSequence).asObject());
        lastSeqCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().lastSequence).asObject());
        lastOffsetCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().lastOffset));
        lastTimeCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().lastTime));
        messageCountCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().messageCount).asObject());
        partitionTable.setItems(partitionRows);
    }

    /**
     * Sets the Event Hub context and loads information.
     */
    public void setContext(ConnectionProfile profile) {
        this.connectionProfile = profile;
        this.config = profile.getEventHubsConfig();

        eventHubNameLabel.setText(config.getEventHubName());
        namespaceLabel.setText(config.getFullyQualifiedNamespace());
        consumerGroupLabel.setText(config.getConsumerGroup());
        currentGroupField.setText(config.getConsumerGroup());

        loadEventHubInfo();
    }

    /**
     * Sets callback for edit connection action.
     */
    public void setOnEditConnection(Runnable callback) {
        this.onEditConnection = callback;
    }

    private void loadEventHubInfo() {
        Task<EventHubInfo> infoTask = new Task<>() {
            @Override
            protected EventHubInfo call() throws Exception {
                return eventHubsAdminService.getEventHubInfo(config);
            }
        };

        infoTask.setOnSucceeded(event -> {
            EventHubInfo info = infoTask.getValue();
            Platform.runLater(() -> {
                partitionCountLabel.setText(String.valueOf(info.getPartitionIds().size()));
                loadPartitionDetails();
            });
        });

        infoTask.setOnFailed(event -> {
            log.error("Failed to load Event Hub info", infoTask.getException());
            Platform.runLater(() -> {
                partitionCountLabel.setText("Error");
                totalMessagesLabel.setText("Error");
            });
        });

        new Thread(infoTask).start();
    }

    private void loadPartitionDetails() {
        Task<List<PartitionInfo>> task = new Task<>() {
            @Override
            protected List<PartitionInfo> call() throws Exception {
                return eventHubsAdminService.getPartitionInfo(config);
            }
        };

        task.setOnSucceeded(event -> {
            List<PartitionInfo> partitions = task.getValue();
            Platform.runLater(() -> {
                partitionRows.clear();
                long totalMessages = 0;

                for (PartitionInfo pInfo : partitions) {
                    String lastTime = pInfo.getLastEnqueuedTime() != null
                            ? TIME_FORMATTER.format(pInfo.getLastEnqueuedTime())
                            : "-";

                    partitionRows.add(new PartitionRow(
                            pInfo.getPartitionId(),
                            pInfo.getBeginningSequenceNumber(),
                            pInfo.getLastEnqueuedSequenceNumber(),
                            pInfo.getLastEnqueuedOffset(),
                            lastTime,
                            pInfo.getMessageCount()
                    ));

                    totalMessages += pInfo.getMessageCount();
                }

                totalMessagesLabel.setText(String.format("%,d", totalMessages));
            });
        });

        task.setOnFailed(event -> {
            log.error("Failed to load partition details", task.getException());
        });

        new Thread(task).start();
    }

    @FXML
    private void handleRefresh() {
        loadEventHubInfo();
    }

    @FXML
    private void handleOpenAzurePortal() {
        String namespace = config.getFullyQualifiedNamespace();
        if (namespace != null) {
            // Extract just the namespace name
            String nsName = namespace.replace(".servicebus.windows.net", "");
            String url = "https://portal.azure.com/#browse/Microsoft.EventHub%2Fnamespaces";

            try {
                var hostServices = StreamLensApplication.getAppHostServices();
                if (hostServices != null) {
                    hostServices.showDocument(url);
                }
            } catch (Exception e) {
                log.error("Failed to open Azure Portal", e);
                showError("Could not open browser", "Please navigate to Azure Portal manually.");
            }
        }
    }

    @FXML
    private void handleEditConnection() {
        if (onEditConnection != null) {
            // Close this dialog first
            partitionTable.getScene().getWindow().hide();
            onEditConnection.run();
        }
    }

    private void showError(String title, String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(title);
        alert.setContentText(message);
        alert.showAndWait();
    }

    // ============== Data Classes ==============

    public static class PartitionRow {
        final String partitionId;
        final long beginSequence;
        final long lastSequence;
        final String lastOffset;
        final String lastTime;
        final long messageCount;

        PartitionRow(String partitionId, long beginSequence, long lastSequence,
                     String lastOffset, String lastTime, long messageCount) {
            this.partitionId = partitionId;
            this.beginSequence = beginSequence;
            this.lastSequence = lastSequence;
            this.lastOffset = lastOffset;
            this.lastTime = lastTime;
            this.messageCount = messageCount;
        }
    }
}
