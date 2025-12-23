package com.streamlens.model.connection;

/**
 * Configuration for Azure Event Hubs connections.
 * Supports connection string, managed identity, and service principal authentication.
 */
public class EventHubsConnectionConfig {

    private AuthenticationType authenticationType;

    // Namespace and Event Hub name (required for all auth types)
    private String namespace;        // e.g., "mynamespace.servicebus.windows.net"
    private String eventHubName;     // e.g., "myeventhub"

    // Connection String authentication
    private String connectionString;

    // Service Principal authentication
    private String tenantId;
    private String clientId;
    private String clientSecret;

    // Consumer group (default: $Default)
    private String consumerGroup = "$Default";

    public EventHubsConnectionConfig() {
        this.authenticationType = AuthenticationType.CONNECTION_STRING;
    }

    /**
     * Gets the fully qualified namespace.
     */
    public String getFullyQualifiedNamespace() {
        if (namespace == null) return null;
        if (namespace.contains(".servicebus.windows.net")) {
            return namespace;
        }
        return namespace + ".servicebus.windows.net";
    }

    /**
     * Extracts the namespace from a connection string if present.
     */
    public static String extractNamespaceFromConnectionString(String connectionString) {
        if (connectionString == null) return null;
        // Format: Endpoint=sb://namespace.servicebus.windows.net/;...
        int start = connectionString.indexOf("sb://");
        if (start == -1) return null;
        start += 5;
        int end = connectionString.indexOf(".servicebus.windows.net", start);
        if (end == -1) return null;
        return connectionString.substring(start, end);
    }

    /**
     * Extracts the Event Hub name from a connection string if present.
     */
    public static String extractEventHubFromConnectionString(String connectionString) {
        if (connectionString == null) return null;
        // Format: ...;EntityPath=eventhubname
        String marker = "EntityPath=";
        int start = connectionString.indexOf(marker);
        if (start == -1) return null;
        start += marker.length();
        int end = connectionString.indexOf(";", start);
        if (end == -1) {
            return connectionString.substring(start);
        }
        return connectionString.substring(start, end);
    }

    // Getters and Setters

    public AuthenticationType getAuthenticationType() {
        return authenticationType;
    }

    public void setAuthenticationType(AuthenticationType authenticationType) {
        this.authenticationType = authenticationType;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getEventHubName() {
        return eventHubName;
    }

    public void setEventHubName(String eventHubName) {
        this.eventHubName = eventHubName;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
        // Auto-extract namespace and event hub name
        if (namespace == null) {
            this.namespace = extractNamespaceFromConnectionString(connectionString);
        }
        if (eventHubName == null) {
            this.eventHubName = extractEventHubFromConnectionString(connectionString);
        }
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup != null ? consumerGroup : "$Default";
    }

    /**
     * Authentication type for Event Hubs connections.
     */
    public enum AuthenticationType {
        /**
         * Connection string with SAS (Shared Access Signature).
         */
        CONNECTION_STRING,

        /**
         * Azure Managed Identity (system-assigned or user-assigned).
         */
        MANAGED_IDENTITY,

        /**
         * Azure AD Service Principal (client credentials).
         */
        SERVICE_PRINCIPAL,

        /**
         * Interactive browser login with Microsoft Entra ID.
         * Opens browser for OAuth authentication and caches tokens.
         */
        INTERACTIVE_BROWSER
    }
}
