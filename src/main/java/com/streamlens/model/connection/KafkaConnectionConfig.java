package com.streamlens.model.connection;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Kafka connections.
 * Supports various authentication mechanisms.
 */
public class KafkaConnectionConfig {

    private String bootstrapServers;
    private AuthenticationType authenticationType;

    // SASL credentials
    private String username;
    private String password;
    private String saslMechanism; // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512

    // SSL configuration
    private boolean sslEnabled;
    private String truststorePath;
    private String truststorePassword;
    private String keystorePath;
    private String keystorePassword;
    private String keyPassword;

    // Additional properties
    private Map<String, String> additionalProperties = new HashMap<>();

    public KafkaConnectionConfig() {
        this.authenticationType = AuthenticationType.NONE;
        this.sslEnabled = false;
    }

    /**
     * Builds Kafka client properties from this configuration.
     * WARNING: Returned map contains sensitive data - handle carefully.
     */
    public Map<String, Object> toKafkaProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put("bootstrap.servers", bootstrapServers);

        // Configure security
        switch (authenticationType) {
            case SASL_PLAIN:
            case SASL_SCRAM:
                configureSasl(props);
                break;
            case SSL:
                configureSsl(props);
                break;
            case NONE:
            default:
                if (sslEnabled) {
                    props.put("security.protocol", "SSL");
                    configureSslCertificates(props);
                } else {
                    props.put("security.protocol", "PLAINTEXT");
                }
                break;
        }

        // Add additional properties
        props.putAll(additionalProperties);

        return props;
    }

    private void configureSasl(Map<String, Object> props) {
        String protocol = sslEnabled ? "SASL_SSL" : "SASL_PLAINTEXT";
        props.put("security.protocol", protocol);

        String mechanism = authenticationType == AuthenticationType.SASL_PLAIN
                ? "PLAIN"
                : (saslMechanism != null ? saslMechanism : "SCRAM-SHA-256");
        props.put("sasl.mechanism", mechanism);

        String jaasConfig = buildJaasConfig(mechanism);
        props.put("sasl.jaas.config", jaasConfig);

        if (sslEnabled) {
            configureSslCertificates(props);
        }
    }

    private String buildJaasConfig(String mechanism) {
        if ("PLAIN".equals(mechanism)) {
            return String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                            "username=\"%s\" password=\"%s\";",
                    escapeJaasValue(username),
                    escapeJaasValue(password)
            );
        } else {
            // SCRAM-SHA-256 or SCRAM-SHA-512
            return String.format(
                    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                            "username=\"%s\" password=\"%s\";",
                    escapeJaasValue(username),
                    escapeJaasValue(password)
            );
        }
    }

    private String escapeJaasValue(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private void configureSsl(Map<String, Object> props) {
        props.put("security.protocol", "SSL");
        configureSslCertificates(props);
    }

    private void configureSslCertificates(Map<String, Object> props) {
        if (truststorePath != null && !truststorePath.isEmpty()) {
            props.put("ssl.truststore.location", truststorePath);
            if (truststorePassword != null) {
                props.put("ssl.truststore.password", truststorePassword);
            }
        }

        if (keystorePath != null && !keystorePath.isEmpty()) {
            props.put("ssl.keystore.location", keystorePath);
            if (keystorePassword != null) {
                props.put("ssl.keystore.password", keystorePassword);
            }
            if (keyPassword != null) {
                props.put("ssl.key.password", keyPassword);
            }
        }
    }

    // Getters and Setters

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public AuthenticationType getAuthenticationType() {
        return authenticationType;
    }

    public void setAuthenticationType(AuthenticationType authenticationType) {
        this.authenticationType = authenticationType;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public String getTruststorePath() {
        return truststorePath;
    }

    public void setTruststorePath(String truststorePath) {
        this.truststorePath = truststorePath;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public String getKeystorePath() {
        return keystorePath;
    }

    public void setKeystorePath(String keystorePath) {
        this.keystorePath = keystorePath;
    }

    public String getKeystorePassword() {
        return keystorePassword;
    }

    public void setKeystorePassword(String keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public Map<String, String> getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    /**
     * Authentication type for Kafka connections.
     */
    public enum AuthenticationType {
        NONE,
        SASL_PLAIN,
        SASL_SCRAM,
        SSL
    }
}
