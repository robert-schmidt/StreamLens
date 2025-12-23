package com.streamlens.controller;

import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.service.kafka.KafkaAdminService;
import com.streamlens.service.kafka.KafkaProducerService;
import com.streamlens.service.kafka.KafkaProducerService.*;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Controller for the Produce Message dialog.
 * Supports single message and mass message production with variable substitution.
 */
@Component
public class ProduceMessageDialogController implements Initializable {

    private static final Logger log = LoggerFactory.getLogger(ProduceMessageDialogController.class);

    private static final String VARIABLES_HELP = """
            Variables are replaced with dynamic values for each message:

            {{index}} or {{i}}    - Message index (0-based)
            {{index1}}            - Message index (1-based)
            {{uuid}}              - Random UUID
            {{timestamp}}         - Current timestamp (ISO format)
            {{timestamp_ms}}      - Timestamp in milliseconds
            {{date}}              - Current date (yyyy-MM-dd)
            {{datetime}}          - Current datetime
            {{random}}            - Random integer
            {{random:min:max}}    - Random integer in range
            {{seq:name}}          - Named sequence (auto-increment)
            {{seq:name:start}}    - Named sequence starting at value
            {{string:length}}     - Random alphanumeric string
            {{choice:a:b:c}}      - Random choice from options

            Example:
            {"id": "{{uuid}}", "index": {{index}}, "type": "{{choice:A:B:C}}"}
            """;

    private final KafkaProducerService kafkaProducerService;
    private final KafkaAdminService kafkaAdminService;

    @FXML private Label topicLabel;
    @FXML private ComboBox<String> partitionCombo;
    @FXML private RadioButton singleModeRadio;
    @FXML private RadioButton massModeRadio;
    @FXML private ToggleGroup modeToggle;
    @FXML private TextField keyField;
    @FXML private TextArea valueField;

    // Headers
    @FXML private TableView<HeaderRow> headersTable;
    @FXML private TableColumn<HeaderRow, String> headerKeyCol;
    @FXML private TableColumn<HeaderRow, String> headerValueCol;
    @FXML private TextField newHeaderKeyField;
    @FXML private TextField newHeaderValueField;

    // Mass produce options
    @FXML private HBox massOptionsInline;
    @FXML private TitledPane massOptionsPane;
    @FXML private TextField messageCountField;
    @FXML private TextField delayField;
    @FXML private TextArea variablesHelpArea;
    @FXML private Button formatJsonButton;

    private final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    // Progress
    @FXML private VBox progressPane;
    @FXML private ProgressBar progressBar;
    @FXML private Label progressLabel;
    @FXML private Label rateLabel;

    private ConnectionProfile connectionProfile;
    private String topic;
    private int partitionCount;
    private final ObservableList<HeaderRow> headers = FXCollections.observableArrayList();
    private CompletableFuture<MassProduceResult> activeMassProduce;
    private Dialog<?> parentDialog;

    public ProduceMessageDialogController(KafkaProducerService kafkaProducerService,
                                          KafkaAdminService kafkaAdminService) {
        this.kafkaProducerService = kafkaProducerService;
        this.kafkaAdminService = kafkaAdminService;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        // Setup mode toggle
        singleModeRadio.setSelected(true);
        modeToggle.selectedToggleProperty().addListener((obs, oldVal, newVal) -> {
            boolean isMassMode = newVal == massModeRadio;
            massOptionsInline.setVisible(isMassMode);
            massOptionsInline.setManaged(isMassMode);
            massOptionsPane.setVisible(isMassMode);
            massOptionsPane.setManaged(isMassMode);
        });

        // Setup headers table
        headerKeyCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().key));
        headerValueCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().value));
        headersTable.setItems(headers);

        // Setup variables help
        variablesHelpArea.setText(VARIABLES_HELP);

        // Setup number-only fields
        messageCountField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal.matches("\\d*")) {
                messageCountField.setText(newVal.replaceAll("[^\\d]", ""));
            }
        });
        delayField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal.matches("\\d*")) {
                delayField.setText(newVal.replaceAll("[^\\d]", ""));
            }
        });

        // Default values
        messageCountField.setText("100");
        delayField.setText("0");
    }

    /**
     * Sets the context and loads partition information.
     */
    public void setContext(ConnectionProfile profile, String topic) {
        this.connectionProfile = profile;
        this.topic = topic;

        topicLabel.setText(topic);
        loadPartitions();
    }

    /**
     * Sets the parent dialog for button control.
     */
    public void setParentDialog(Dialog<?> dialog) {
        this.parentDialog = dialog;
    }

    private void loadPartitions() {
        Task<Map<Integer, KafkaAdminService.OffsetRange>> task = new Task<>() {
            @Override
            protected Map<Integer, KafkaAdminService.OffsetRange> call() throws Exception {
                return kafkaAdminService.getTopicOffsets(connectionProfile.getKafkaConfig(), topic);
            }
        };

        task.setOnSucceeded(event -> {
            Map<Integer, KafkaAdminService.OffsetRange> offsets = task.getValue();
            partitionCount = offsets.size();

            Platform.runLater(() -> {
                List<String> partitionItems = new ArrayList<>();
                partitionItems.add("(Automatic)");
                for (int i = 0; i < partitionCount; i++) {
                    partitionItems.add("Partition " + i);
                }
                partitionCombo.setItems(FXCollections.observableArrayList(partitionItems));
                partitionCombo.getSelectionModel().selectFirst();
            });
        });

        task.setOnFailed(event -> {
            log.warn("Failed to load partitions", task.getException());
            Platform.runLater(() -> {
                partitionCombo.setItems(FXCollections.observableArrayList("(Automatic)"));
                partitionCombo.getSelectionModel().selectFirst();
            });
        });

        new Thread(task).start();
    }

    @FXML
    private void handleAddHeader() {
        String key = newHeaderKeyField.getText().trim();
        String value = newHeaderValueField.getText().trim();

        if (!key.isEmpty()) {
            headers.add(new HeaderRow(key, value));
            newHeaderKeyField.clear();
            newHeaderValueField.clear();
        }
    }

    @FXML
    private void handleRemoveHeader() {
        HeaderRow selected = headersTable.getSelectionModel().getSelectedItem();
        if (selected != null) {
            headers.remove(selected);
        }
    }

    @FXML
    private void handleFormatJson() {
        String text = valueField.getText();
        if (text == null || text.trim().isEmpty()) {
            return;
        }

        try {
            // Parse and re-serialize to format
            Object json = objectMapper.readValue(text, Object.class);
            String formatted = objectMapper.writeValueAsString(json);
            valueField.setText(formatted);
        } catch (Exception e) {
            // Not valid JSON, show error
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Invalid JSON");
            alert.setHeaderText("Could not format JSON");
            alert.setContentText("The text is not valid JSON: " + e.getMessage());
            alert.showAndWait();
        }
    }

    /**
     * Sends the message(s).
     * @return true if successful (for single mode) or started (for mass mode)
     */
    public boolean send() {
        if (valueField.getText().trim().isEmpty()) {
            showError("Value Required", "Message value cannot be empty.");
            return false;
        }

        Integer partition = getSelectedPartition();
        Map<String, String> headerMap = buildHeaderMap();

        if (singleModeRadio.isSelected()) {
            return sendSingleMessage(partition, headerMap);
        } else {
            return startMassProduce(partition, headerMap);
        }
    }

    private boolean sendSingleMessage(Integer partition, Map<String, String> headerMap) {
        String key = keyField.getText().trim().isEmpty() ? null : keyField.getText().trim();
        String value = valueField.getText();

        SendResult result = kafkaProducerService.sendMessage(
                connectionProfile.getKafkaConfig(),
                topic, partition, key, value, headerMap
        );

        if (result.isSuccess()) {
            showInfo("Message Sent",
                    String.format("Message sent to partition %d at offset %d",
                            result.getPartition(), result.getOffset()));
            return true;
        } else {
            showError("Send Failed", result.getError());
            return false;
        }
    }

    private boolean startMassProduce(Integer partition, Map<String, String> headerMap) {
        int messageCount;
        int delayMs;

        try {
            messageCount = Integer.parseInt(messageCountField.getText().trim());
            if (messageCount <= 0) {
                showError("Invalid Count", "Message count must be greater than 0.");
                return false;
            }
        } catch (NumberFormatException e) {
            showError("Invalid Count", "Please enter a valid message count.");
            return false;
        }

        try {
            delayMs = delayField.getText().trim().isEmpty() ? 0 : Integer.parseInt(delayField.getText().trim());
        } catch (NumberFormatException e) {
            showError("Invalid Delay", "Please enter a valid delay in milliseconds.");
            return false;
        }

        String keyTemplate = keyField.getText().trim().isEmpty() ? null : keyField.getText().trim();
        String valueTemplate = valueField.getText();

        // Show progress
        progressPane.setVisible(true);
        progressPane.setManaged(true);
        progressBar.setProgress(0);
        progressLabel.setText("Sending 0 / " + messageCount);
        rateLabel.setText("");

        // Disable send button
        if (parentDialog != null) {
            parentDialog.getDialogPane().lookupButton(ButtonType.OK).setDisable(true);
        }

        activeMassProduce = kafkaProducerService.sendMassMessages(
                connectionProfile.getKafkaConfig(),
                topic, partition, keyTemplate, valueTemplate, headerMap,
                messageCount, delayMs,
                progress -> Platform.runLater(() -> updateProgress(progress))
        );

        activeMassProduce.thenAccept(result -> Platform.runLater(() -> {
            progressPane.setVisible(false);
            progressPane.setManaged(false);

            if (parentDialog != null) {
                parentDialog.getDialogPane().lookupButton(ButtonType.OK).setDisable(false);
            }

            showInfo("Mass Produce Complete",
                    String.format("Sent %,d messages (%,d failed) in %,.0f ms\nAverage rate: %,.1f msg/s",
                            result.getSuccessCount(), result.getFailCount(),
                            (double) result.getTotalTimeMs(), result.getAverageRate()));
        }));

        activeMassProduce.exceptionally(ex -> {
            Platform.runLater(() -> {
                progressPane.setVisible(false);
                progressPane.setManaged(false);
                showError("Mass Produce Failed", ex.getMessage());
            });
            return null;
        });

        return false; // Don't close dialog yet
    }

    private void updateProgress(MassProduceProgress progress) {
        progressBar.setProgress(progress.getPercentage() / 100.0);
        progressLabel.setText(String.format("Sent %,d / %,d (Success: %,d, Failed: %,d)",
                progress.getSent(), progress.getTotal(),
                progress.getSuccess(), progress.getFailed()));
        rateLabel.setText(String.format("%.1f msg/s", progress.getRate()));
    }

    private Integer getSelectedPartition() {
        String selected = partitionCombo.getValue();
        if (selected == null || selected.startsWith("(")) {
            return null;
        }
        return Integer.parseInt(selected.replace("Partition ", ""));
    }

    private Map<String, String> buildHeaderMap() {
        Map<String, String> map = new LinkedHashMap<>();
        for (HeaderRow header : headers) {
            map.put(header.key, header.value);
        }
        return map.isEmpty() ? null : map;
    }

    /**
     * Validates the form fields.
     */
    public String validate() {
        if (valueField.getText().trim().isEmpty()) {
            return "Message value is required";
        }

        if (massModeRadio.isSelected()) {
            try {
                int count = Integer.parseInt(messageCountField.getText().trim());
                if (count <= 0) {
                    return "Message count must be greater than 0";
                }
            } catch (NumberFormatException e) {
                return "Invalid message count";
            }
        }

        return null;
    }

    private void showError(String title, String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(title);
        alert.setContentText(message);
        alert.showAndWait();
    }

    private void showInfo(String title, String message) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Success");
        alert.setHeaderText(title);
        alert.setContentText(message);
        alert.showAndWait();
    }

    // ============== Data Classes ==============

    public static class HeaderRow {
        final String key;
        final String value;

        HeaderRow(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
