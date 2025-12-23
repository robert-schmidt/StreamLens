package com.streamlens.controller;

import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.service.kafka.KafkaAdminService;
import com.streamlens.service.kafka.KafkaAdminService.*;
import javafx.application.Platform;
import javafx.beans.property.SimpleIntegerProperty;
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
import java.util.*;
import java.util.stream.Collectors;

/**
 * Controller for the Topic Information dialog.
 * Displays topic metadata, partition details, configuration, and consumer group information.
 */
@Component
public class TopicInfoDialogController implements Initializable {

    private static final Logger log = LoggerFactory.getLogger(TopicInfoDialogController.class);

    private final KafkaAdminService kafkaAdminService;

    // Overview tab
    @FXML private Label topicNameLabel;
    @FXML private Label partitionCountLabel;
    @FXML private Label totalMessagesLabel;
    @FXML private Label internalLabel;

    // Partition table
    @FXML private TableView<PartitionRow> partitionTable;
    @FXML private TableColumn<PartitionRow, Integer> partitionCol;
    @FXML private TableColumn<PartitionRow, Integer> leaderCol;
    @FXML private TableColumn<PartitionRow, String> replicasCol;
    @FXML private TableColumn<PartitionRow, String> isrCol;
    @FXML private TableColumn<PartitionRow, Long> beginOffsetCol;
    @FXML private TableColumn<PartitionRow, Long> endOffsetCol;
    @FXML private TableColumn<PartitionRow, Long> messagesCol;

    // Configuration tab
    @FXML private TableView<ConfigRow> configTable;
    @FXML private TableColumn<ConfigRow, String> configKeyCol;
    @FXML private TableColumn<ConfigRow, String> configValueCol;
    @FXML private Label configHintLabel;

    // Consumer Groups tab
    @FXML private ComboBox<String> consumerGroupCombo;
    @FXML private Button refreshGroupsButton;
    @FXML private TableView<LagRow> lagTable;
    @FXML private TableColumn<LagRow, Integer> lagPartitionCol;
    @FXML private TableColumn<LagRow, Long> currentOffsetCol;
    @FXML private TableColumn<LagRow, Long> endOffsetLagCol;
    @FXML private TableColumn<LagRow, Long> lagCol;
    @FXML private Label totalLagLabel;
    @FXML private Button resetToEarliestButton;
    @FXML private Button resetToLatestButton;

    private ConnectionProfile connectionProfile;
    private String topic;
    private final ObservableList<PartitionRow> partitionRows = FXCollections.observableArrayList();
    private final ObservableList<ConfigRow> configRows = FXCollections.observableArrayList();
    private final ObservableList<LagRow> lagRows = FXCollections.observableArrayList();

    public TopicInfoDialogController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        // Setup partition table
        partitionCol.setCellValueFactory(data -> new SimpleIntegerProperty(data.getValue().partition).asObject());
        leaderCol.setCellValueFactory(data -> new SimpleIntegerProperty(data.getValue().leader).asObject());
        replicasCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().replicas));
        isrCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().isr));
        beginOffsetCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().beginOffset).asObject());
        endOffsetCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().endOffset).asObject());
        messagesCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().messages).asObject());
        partitionTable.setItems(partitionRows);

        // Setup config table
        configKeyCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().key));
        configValueCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().value));
        configTable.setItems(configRows);

        // Setup lag table
        lagPartitionCol.setCellValueFactory(data -> new SimpleIntegerProperty(data.getValue().partition).asObject());
        currentOffsetCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().currentOffset).asObject());
        endOffsetLagCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().endOffset).asObject());
        lagCol.setCellValueFactory(data -> new SimpleLongProperty(data.getValue().lag).asObject());
        lagTable.setItems(lagRows);

        // Setup consumer group combo listener
        consumerGroupCombo.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null && !newVal.isEmpty()) {
                loadConsumerGroupLag(newVal);
            }
        });

        // Disable reset buttons until a group is selected
        resetToEarliestButton.setDisable(true);
        resetToLatestButton.setDisable(true);
    }

    /**
     * Sets the topic context and loads information.
     */
    public void setContext(ConnectionProfile profile, String topic) {
        this.connectionProfile = profile;
        this.topic = topic;

        topicNameLabel.setText(topic);
        loadTopicInfo();
        loadConsumerGroups();
    }

    private void loadTopicInfo() {
        Task<TopicInfo> task = new Task<>() {
            @Override
            protected TopicInfo call() throws Exception {
                return kafkaAdminService.getTopicInfo(connectionProfile.getKafkaConfig(), topic);
            }
        };

        task.setOnSucceeded(event -> {
            TopicInfo info = task.getValue();
            Platform.runLater(() -> {
                partitionCountLabel.setText(String.valueOf(info.getPartitionCount()));
                internalLabel.setText(info.isInternal() ? "Yes" : "No");

                // Load configuration
                configRows.clear();
                if (info.getConfig() != null) {
                    info.getConfig().forEach((key, value) -> configRows.add(new ConfigRow(key, value)));
                }
                if (configRows.isEmpty()) {
                    configHintLabel.setText("All configuration values are at their defaults.");
                }

                // Load partition info with offsets
                loadPartitionOffsets(info.getPartitions());
            });
        });

        task.setOnFailed(event -> {
            log.error("Failed to load topic info", task.getException());
            Platform.runLater(() -> {
                partitionCountLabel.setText("Error");
                totalMessagesLabel.setText("Error");
            });
        });

        new Thread(task).start();
    }

    private void loadPartitionOffsets(List<PartitionInfo> partitions) {
        Task<Map<Integer, OffsetRange>> task = new Task<>() {
            @Override
            protected Map<Integer, OffsetRange> call() throws Exception {
                return kafkaAdminService.getTopicOffsets(connectionProfile.getKafkaConfig(), topic);
            }
        };

        task.setOnSucceeded(event -> {
            Map<Integer, OffsetRange> offsets = task.getValue();
            Platform.runLater(() -> {
                partitionRows.clear();
                long totalMessages = 0;

                for (PartitionInfo pInfo : partitions) {
                    OffsetRange range = offsets.get(pInfo.getPartition());
                    long begin = range != null ? range.getBeginning() : 0;
                    long end = range != null ? range.getEnd() : 0;
                    long messages = end - begin;
                    totalMessages += messages;

                    partitionRows.add(new PartitionRow(
                            pInfo.getPartition(),
                            pInfo.getLeader(),
                            formatList(pInfo.getReplicas()),
                            formatList(pInfo.getIsr()),
                            begin, end, messages
                    ));
                }

                totalMessagesLabel.setText(String.format("%,d", totalMessages));
            });
        });

        task.setOnFailed(event -> {
            log.error("Failed to load partition offsets", task.getException());
        });

        new Thread(task).start();
    }

    private void loadConsumerGroups() {
        Task<List<ConsumerGroupInfo>> task = new Task<>() {
            @Override
            protected List<ConsumerGroupInfo> call() throws Exception {
                return kafkaAdminService.listConsumerGroups(connectionProfile.getKafkaConfig());
            }
        };

        task.setOnSucceeded(event -> {
            List<ConsumerGroupInfo> groups = task.getValue();
            Platform.runLater(() -> {
                List<String> groupIds = groups.stream()
                        .map(ConsumerGroupInfo::getGroupId)
                        .sorted()
                        .collect(Collectors.toList());
                consumerGroupCombo.setItems(FXCollections.observableArrayList(groupIds));
            });
        });

        task.setOnFailed(event -> {
            log.error("Failed to load consumer groups", task.getException());
        });

        new Thread(task).start();
    }

    private void loadConsumerGroupLag(String groupId) {
        Task<Map<Integer, LagInfo>> task = new Task<>() {
            @Override
            protected Map<Integer, LagInfo> call() throws Exception {
                return kafkaAdminService.getConsumerGroupLag(connectionProfile.getKafkaConfig(), groupId, topic);
            }
        };

        task.setOnSucceeded(event -> {
            Map<Integer, LagInfo> lagInfoMap = task.getValue();
            Platform.runLater(() -> {
                lagRows.clear();
                long totalLag = 0;

                for (Map.Entry<Integer, LagInfo> entry : lagInfoMap.entrySet()) {
                    LagInfo info = entry.getValue();
                    lagRows.add(new LagRow(
                            info.getPartition(),
                            info.getCommittedOffset(),
                            info.getLatestOffset(),
                            info.getLag() >= 0 ? info.getLag() : 0
                    ));
                    if (info.getLag() > 0) {
                        totalLag += info.getLag();
                    }
                }

                totalLagLabel.setText("Total Lag: " + String.format("%,d", totalLag));
                resetToEarliestButton.setDisable(false);
                resetToLatestButton.setDisable(false);
            });
        });

        task.setOnFailed(event -> {
            log.error("Failed to load consumer group lag", task.getException());
            Platform.runLater(() -> {
                lagRows.clear();
                totalLagLabel.setText("Total Lag: Error");
            });
        });

        new Thread(task).start();
    }

    @FXML
    private void handleRefreshGroups() {
        loadConsumerGroups();
    }

    @FXML
    private void handleResetToEarliest() {
        String groupId = consumerGroupCombo.getValue();
        if (groupId == null || groupId.isEmpty()) return;

        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Reset Offsets");
        confirm.setHeaderText("Reset to Earliest?");
        confirm.setContentText("This will reset consumer group '" + groupId +
                "' to the earliest offset for topic '" + topic + "'. Continue?");

        if (confirm.showAndWait().orElse(ButtonType.CANCEL) == ButtonType.OK) {
            resetOffsets(groupId, true);
        }
    }

    @FXML
    private void handleResetToLatest() {
        String groupId = consumerGroupCombo.getValue();
        if (groupId == null || groupId.isEmpty()) return;

        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Reset Offsets");
        confirm.setHeaderText("Reset to Latest?");
        confirm.setContentText("This will reset consumer group '" + groupId +
                "' to the latest offset for topic '" + topic + "'. Continue?");

        if (confirm.showAndWait().orElse(ButtonType.CANCEL) == ButtonType.OK) {
            resetOffsets(groupId, false);
        }
    }

    private void resetOffsets(String groupId, boolean toEarliest) {
        Task<Void> task = new Task<>() {
            @Override
            protected Void call() throws Exception {
                if (toEarliest) {
                    kafkaAdminService.resetOffsetsToEarliest(connectionProfile.getKafkaConfig(), groupId, topic);
                } else {
                    kafkaAdminService.resetOffsetsToLatest(connectionProfile.getKafkaConfig(), groupId, topic);
                }
                return null;
            }
        };

        task.setOnSucceeded(event -> {
            Platform.runLater(() -> {
                Alert alert = new Alert(Alert.AlertType.INFORMATION);
                alert.setTitle("Offsets Reset");
                alert.setHeaderText("Success");
                alert.setContentText("Consumer group offsets have been reset.");
                alert.showAndWait();
                loadConsumerGroupLag(groupId);
            });
        });

        task.setOnFailed(event -> {
            Platform.runLater(() -> {
                Alert alert = new Alert(Alert.AlertType.ERROR);
                alert.setTitle("Error");
                alert.setHeaderText("Failed to Reset Offsets");
                alert.setContentText(task.getException().getMessage());
                alert.showAndWait();
            });
        });

        new Thread(task).start();
    }

    private String formatList(List<Integer> list) {
        if (list == null || list.isEmpty()) return "-";
        return list.stream().map(String::valueOf).collect(Collectors.joining(", "));
    }

    // ============== Data Classes ==============

    public static class PartitionRow {
        final int partition;
        final int leader;
        final String replicas;
        final String isr;
        final long beginOffset;
        final long endOffset;
        final long messages;

        PartitionRow(int partition, int leader, String replicas, String isr,
                     long beginOffset, long endOffset, long messages) {
            this.partition = partition;
            this.leader = leader;
            this.replicas = replicas;
            this.isr = isr;
            this.beginOffset = beginOffset;
            this.endOffset = endOffset;
            this.messages = messages;
        }
    }

    public static class ConfigRow {
        final String key;
        final String value;

        ConfigRow(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class LagRow {
        final int partition;
        final long currentOffset;
        final long endOffset;
        final long lag;

        LagRow(int partition, long currentOffset, long endOffset, long lag) {
            this.partition = partition;
            this.currentOffset = currentOffset;
            this.endOffset = endOffset;
            this.lag = lag;
        }
    }
}
