package com.streamlens.controller;

import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.model.connection.ConnectionProfile.ConnectionType;
import com.streamlens.model.connection.EventHubsConnectionConfig;
import com.streamlens.model.connection.KafkaConnectionConfig;
import com.streamlens.service.connection.ConnectionService;
import com.streamlens.service.connection.ConnectionService.ConnectionTestResult;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import org.springframework.stereotype.Component;

import java.io.File;
import java.net.URL;
import java.util.ResourceBundle;

/**
 * Controller for the connection dialog.
 */
@Component
public class ConnectionDialogController implements Initializable {

    private final ConnectionService connectionService;

    // General fields
    @FXML private TextField nameField;
    @FXML private ComboBox<String> typeComboBox;

    // Kafka fields
    @FXML private VBox kafkaConfigPane;
    @FXML private TextField bootstrapServersField;
    @FXML private ComboBox<String> kafkaAuthComboBox;
    @FXML private VBox kafkaSaslPane;
    @FXML private TextField kafkaUsernameField;
    @FXML private PasswordField kafkaPasswordField;
    @FXML private ComboBox<String> saslMechanismComboBox;
    @FXML private CheckBox kafkaSslCheckBox;
    @FXML private VBox kafkaSslPane;
    @FXML private TextField truststorePathField;
    @FXML private PasswordField truststorePasswordField;

    // Event Hubs fields
    @FXML private VBox eventHubsConfigPane;
    @FXML private ComboBox<String> eventHubsAuthComboBox;
    @FXML private VBox connectionStringPane;
    @FXML private TextArea connectionStringField;
    @FXML private TextField eventHubNameField;
    @FXML private VBox servicePrincipalPane;
    @FXML private TextField namespaceField;
    @FXML private TextField eventHubNameSpField;
    @FXML private TextField tenantIdField;
    @FXML private TextField clientIdField;
    @FXML private PasswordField clientSecretField;
    @FXML private VBox managedIdentityPane;
    @FXML private TextField namespaceMiField;
    @FXML private TextField eventHubNameMiField;
    @FXML private VBox interactiveBrowserPane;
    @FXML private TextField namespaceIbField;
    @FXML private TextField eventHubNameIbField;
    @FXML private TextField tenantIdIbField;
    @FXML private TextField consumerGroupField;

    // Test button
    @FXML private Button testButton;
    @FXML private Label testResultLabel;

    private ConnectionProfile editingProfile;

    public ConnectionDialogController(ConnectionService connectionService) {
        this.connectionService = connectionService;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        // Setup type combo box
        typeComboBox.setItems(FXCollections.observableArrayList("Apache Kafka", "Azure Event Hubs"));
        typeComboBox.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            updateConfigPanes(newVal);
        });
        typeComboBox.getSelectionModel().selectFirst();

        // Setup Kafka auth combo box
        kafkaAuthComboBox.setItems(FXCollections.observableArrayList(
                "No Authentication", "SASL/PLAIN", "SASL/SCRAM-SHA-256", "SASL/SCRAM-SHA-512", "SSL Client Certificate"
        ));
        kafkaAuthComboBox.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            updateKafkaAuthPanes(newVal);
        });
        kafkaAuthComboBox.getSelectionModel().selectFirst();

        // Setup SASL mechanism combo box
        saslMechanismComboBox.setItems(FXCollections.observableArrayList(
                "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"
        ));
        saslMechanismComboBox.getSelectionModel().selectFirst();

        // Setup SSL checkbox
        kafkaSslCheckBox.selectedProperty().addListener((obs, oldVal, newVal) -> {
            kafkaSslPane.setVisible(newVal);
            kafkaSslPane.setManaged(newVal);
        });

        // Setup Event Hubs auth combo box
        eventHubsAuthComboBox.setItems(FXCollections.observableArrayList(
                "Connection String", "Microsoft Entra ID (Interactive)", "Service Principal", "Managed Identity"
        ));
        eventHubsAuthComboBox.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            updateEventHubsAuthPanes(newVal);
        });
        eventHubsAuthComboBox.getSelectionModel().selectFirst();

        // Set default consumer group
        consumerGroupField.setText("$Default");
    }

    private void updateConfigPanes(String type) {
        boolean isKafka = "Apache Kafka".equals(type);
        kafkaConfigPane.setVisible(isKafka);
        kafkaConfigPane.setManaged(isKafka);
        eventHubsConfigPane.setVisible(!isKafka);
        eventHubsConfigPane.setManaged(!isKafka);
    }

    private void updateKafkaAuthPanes(String auth) {
        boolean needsSasl = auth != null && auth.startsWith("SASL/");
        kafkaSaslPane.setVisible(needsSasl);
        kafkaSaslPane.setManaged(needsSasl);

        if (auth != null && auth.contains("SCRAM")) {
            String mechanism = auth.replace("SASL/", "");
            saslMechanismComboBox.getSelectionModel().select(mechanism);
        }
    }

    private void updateEventHubsAuthPanes(String auth) {
        connectionStringPane.setVisible("Connection String".equals(auth));
        connectionStringPane.setManaged("Connection String".equals(auth));
        interactiveBrowserPane.setVisible("Microsoft Entra ID (Interactive)".equals(auth));
        interactiveBrowserPane.setManaged("Microsoft Entra ID (Interactive)".equals(auth));
        servicePrincipalPane.setVisible("Service Principal".equals(auth));
        servicePrincipalPane.setManaged("Service Principal".equals(auth));
        managedIdentityPane.setVisible("Managed Identity".equals(auth));
        managedIdentityPane.setManaged("Managed Identity".equals(auth));
    }

    @FXML
    private void handleBrowseTruststore() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Select Truststore File");
        fileChooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("Keystore Files", "*.jks", "*.p12", "*.pfx"),
                new FileChooser.ExtensionFilter("All Files", "*.*")
        );
        File file = fileChooser.showOpenDialog(truststorePathField.getScene().getWindow());
        if (file != null) {
            truststorePathField.setText(file.getAbsolutePath());
        }
    }

    @FXML
    private void handleTestConnection() {
        testButton.setDisable(true);
        testResultLabel.setText("Testing...");
        testResultLabel.setStyle("-fx-text-fill: #666666;");

        Task<ConnectionTestResult> task = new Task<>() {
            @Override
            protected ConnectionTestResult call() {
                if (isKafkaSelected()) {
                    return connectionService.testKafkaConnection(buildKafkaConfig());
                } else {
                    return connectionService.testEventHubsConnection(buildEventHubsConfig());
                }
            }
        };

        task.setOnSucceeded(event -> {
            ConnectionTestResult result = task.getValue();
            Platform.runLater(() -> {
                testButton.setDisable(false);
                if (result.isSuccess()) {
                    testResultLabel.setText("Connected! " + result.getMessage());
                    testResultLabel.setStyle("-fx-text-fill: #4caf50;");
                } else {
                    testResultLabel.setText("Failed: " + result.getMessage());
                    testResultLabel.setStyle("-fx-text-fill: #f44336;");
                }
            });
        });

        task.setOnFailed(event -> {
            Platform.runLater(() -> {
                testButton.setDisable(false);
                testResultLabel.setText("Error: " + task.getException().getMessage());
                testResultLabel.setStyle("-fx-text-fill: #f44336;");
            });
        });

        new Thread(task).start();
    }

    /**
     * Sets the profile to edit (null for new connection).
     */
    public void setProfile(ConnectionProfile profile) {
        this.editingProfile = profile;
        if (profile != null) {
            nameField.setText(profile.getName());
            if (profile.getType() == ConnectionType.KAFKA) {
                typeComboBox.getSelectionModel().select("Apache Kafka");
                populateKafkaFields(profile.getKafkaConfig());
            } else {
                typeComboBox.getSelectionModel().select("Azure Event Hubs");
                populateEventHubsFields(profile.getEventHubsConfig());
            }
        }
    }

    private void populateKafkaFields(KafkaConnectionConfig config) {
        bootstrapServersField.setText(config.getBootstrapServers());
        switch (config.getAuthenticationType()) {
            case SASL_PLAIN:
                kafkaAuthComboBox.getSelectionModel().select("SASL/PLAIN");
                break;
            case SASL_SCRAM:
                String mechanism = config.getSaslMechanism();
                kafkaAuthComboBox.getSelectionModel().select("SASL/" + mechanism);
                break;
            case SSL:
                kafkaAuthComboBox.getSelectionModel().select("SSL Client Certificate");
                break;
            default:
                kafkaAuthComboBox.getSelectionModel().select("No Authentication");
        }
        kafkaUsernameField.setText(config.getUsername());
        kafkaPasswordField.setText(config.getPassword());
        kafkaSslCheckBox.setSelected(config.isSslEnabled());
        truststorePathField.setText(config.getTruststorePath());
        truststorePasswordField.setText(config.getTruststorePassword());
    }

    private void populateEventHubsFields(EventHubsConnectionConfig config) {
        consumerGroupField.setText(config.getConsumerGroup());
        switch (config.getAuthenticationType()) {
            case CONNECTION_STRING:
                eventHubsAuthComboBox.getSelectionModel().select("Connection String");
                connectionStringField.setText(config.getConnectionString());
                eventHubNameField.setText(config.getEventHubName());
                break;
            case INTERACTIVE_BROWSER:
                eventHubsAuthComboBox.getSelectionModel().select("Microsoft Entra ID (Interactive)");
                namespaceIbField.setText(config.getNamespace());
                eventHubNameIbField.setText(config.getEventHubName());
                tenantIdIbField.setText(config.getTenantId());
                break;
            case SERVICE_PRINCIPAL:
                eventHubsAuthComboBox.getSelectionModel().select("Service Principal");
                namespaceField.setText(config.getNamespace());
                eventHubNameSpField.setText(config.getEventHubName());
                tenantIdField.setText(config.getTenantId());
                clientIdField.setText(config.getClientId());
                clientSecretField.setText(config.getClientSecret());
                break;
            case MANAGED_IDENTITY:
                eventHubsAuthComboBox.getSelectionModel().select("Managed Identity");
                namespaceMiField.setText(config.getNamespace());
                eventHubNameMiField.setText(config.getEventHubName());
                break;
        }
    }

    /**
     * Builds a ConnectionProfile from the dialog fields.
     */
    public ConnectionProfile buildProfile() {
        ConnectionProfile profile = editingProfile != null ? editingProfile : new ConnectionProfile();
        profile.setName(nameField.getText().trim());

        if (isKafkaSelected()) {
            profile.setType(ConnectionType.KAFKA);
            profile.setConfig(buildKafkaConfig());
        } else {
            profile.setType(ConnectionType.EVENTHUBS);
            profile.setConfig(buildEventHubsConfig());
        }

        return profile;
    }

    private boolean isKafkaSelected() {
        return "Apache Kafka".equals(typeComboBox.getValue());
    }

    private KafkaConnectionConfig buildKafkaConfig() {
        KafkaConnectionConfig config = new KafkaConnectionConfig();
        config.setBootstrapServers(bootstrapServersField.getText().trim());

        String auth = kafkaAuthComboBox.getValue();
        if ("SASL/PLAIN".equals(auth)) {
            config.setAuthenticationType(KafkaConnectionConfig.AuthenticationType.SASL_PLAIN);
            config.setUsername(kafkaUsernameField.getText());
            config.setPassword(kafkaPasswordField.getText());
        } else if (auth != null && auth.startsWith("SASL/SCRAM")) {
            config.setAuthenticationType(KafkaConnectionConfig.AuthenticationType.SASL_SCRAM);
            config.setUsername(kafkaUsernameField.getText());
            config.setPassword(kafkaPasswordField.getText());
            config.setSaslMechanism(saslMechanismComboBox.getValue());
        } else if ("SSL Client Certificate".equals(auth)) {
            config.setAuthenticationType(KafkaConnectionConfig.AuthenticationType.SSL);
        } else {
            config.setAuthenticationType(KafkaConnectionConfig.AuthenticationType.NONE);
        }

        config.setSslEnabled(kafkaSslCheckBox.isSelected());
        if (kafkaSslCheckBox.isSelected()) {
            config.setTruststorePath(truststorePathField.getText());
            config.setTruststorePassword(truststorePasswordField.getText());
        }

        return config;
    }

    private EventHubsConnectionConfig buildEventHubsConfig() {
        EventHubsConnectionConfig config = new EventHubsConnectionConfig();
        config.setConsumerGroup(consumerGroupField.getText().trim());

        String auth = eventHubsAuthComboBox.getValue();
        if ("Connection String".equals(auth)) {
            config.setAuthenticationType(EventHubsConnectionConfig.AuthenticationType.CONNECTION_STRING);
            config.setConnectionString(connectionStringField.getText().trim());
            String hubName = eventHubNameField.getText().trim();
            if (!hubName.isEmpty()) {
                config.setEventHubName(hubName);
            }
        } else if ("Microsoft Entra ID (Interactive)".equals(auth)) {
            config.setAuthenticationType(EventHubsConnectionConfig.AuthenticationType.INTERACTIVE_BROWSER);
            config.setNamespace(namespaceIbField.getText().trim());
            config.setEventHubName(eventHubNameIbField.getText().trim());
            String tenantId = tenantIdIbField.getText().trim();
            if (!tenantId.isEmpty()) {
                config.setTenantId(tenantId);
            }
        } else if ("Service Principal".equals(auth)) {
            config.setAuthenticationType(EventHubsConnectionConfig.AuthenticationType.SERVICE_PRINCIPAL);
            config.setNamespace(namespaceField.getText().trim());
            config.setEventHubName(eventHubNameSpField.getText().trim());
            config.setTenantId(tenantIdField.getText().trim());
            config.setClientId(clientIdField.getText().trim());
            config.setClientSecret(clientSecretField.getText());
        } else if ("Managed Identity".equals(auth)) {
            config.setAuthenticationType(EventHubsConnectionConfig.AuthenticationType.MANAGED_IDENTITY);
            config.setNamespace(namespaceMiField.getText().trim());
            config.setEventHubName(eventHubNameMiField.getText().trim());
        }

        return config;
    }

    /**
     * Validates the form fields.
     */
    public String validate() {
        if (nameField.getText().trim().isEmpty()) {
            return "Connection name is required";
        }

        if (isKafkaSelected()) {
            if (bootstrapServersField.getText().trim().isEmpty()) {
                return "Bootstrap servers is required";
            }
            String auth = kafkaAuthComboBox.getValue();
            if (auth != null && auth.startsWith("SASL/")) {
                if (kafkaUsernameField.getText().isEmpty() || kafkaPasswordField.getText().isEmpty()) {
                    return "Username and password are required for SASL authentication";
                }
            }
        } else {
            String auth = eventHubsAuthComboBox.getValue();
            if ("Connection String".equals(auth)) {
                if (connectionStringField.getText().trim().isEmpty()) {
                    return "Connection string is required";
                }
            } else if ("Microsoft Entra ID (Interactive)".equals(auth)) {
                if (namespaceIbField.getText().trim().isEmpty() ||
                    eventHubNameIbField.getText().trim().isEmpty()) {
                    return "Namespace and Event Hub name are required";
                }
            } else if ("Service Principal".equals(auth)) {
                if (namespaceField.getText().trim().isEmpty() ||
                    eventHubNameSpField.getText().trim().isEmpty() ||
                    tenantIdField.getText().trim().isEmpty() ||
                    clientIdField.getText().trim().isEmpty() ||
                    clientSecretField.getText().isEmpty()) {
                    return "All Service Principal fields are required";
                }
            } else if ("Managed Identity".equals(auth)) {
                if (namespaceMiField.getText().trim().isEmpty() ||
                    eventHubNameMiField.getText().trim().isEmpty()) {
                    return "Namespace and Event Hub name are required";
                }
            }
        }

        return null;
    }
}
