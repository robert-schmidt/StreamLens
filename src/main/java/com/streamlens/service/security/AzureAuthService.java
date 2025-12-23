package com.streamlens.service.security;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for managing Azure authentication with token caching.
 * Supports interactive browser login with persistent token cache.
 */
@Service
public class AzureAuthService {

    private static final Logger log = LoggerFactory.getLogger(AzureAuthService.class);

    // Azure Event Hubs scope for authentication
    private static final String EVENT_HUBS_SCOPE = "https://eventhubs.azure.net/.default";

    // Cache directory for Azure tokens
    private final Path tokenCachePath;

    // Cached credentials by connection ID
    private final Map<String, TokenCredential> credentialCache = new ConcurrentHashMap<>();

    public AzureAuthService() {
        // Store token cache in ~/.streamlens/azure-tokens
        String userHome = System.getProperty("user.home");
        this.tokenCachePath = Paths.get(userHome, ".streamlens", "azure-tokens");
    }

    /**
     * Gets or creates an interactive browser credential for the given connection.
     * Opens browser for authentication if needed, caches tokens for reuse.
     *
     * @param connectionId Unique identifier for this connection (used for token cache)
     * @param tenantId     Optional tenant ID (null for multi-tenant)
     * @return TokenCredential that can be used with Azure SDK clients
     */
    public TokenCredential getInteractiveBrowserCredential(String connectionId, String tenantId) {
        return credentialCache.computeIfAbsent(connectionId, id -> {
            log.info("Creating interactive browser credential for connection: {}", connectionId);

            InteractiveBrowserCredentialBuilder builder = new InteractiveBrowserCredentialBuilder()
                    .redirectUrl("http://localhost:8400");

            // Set tenant if specified (otherwise allows any Azure AD tenant)
            if (tenantId != null && !tenantId.isBlank()) {
                builder.tenantId(tenantId);
            }

            // Enable token cache persistence
            try {
                TokenCachePersistenceOptions cacheOptions = new TokenCachePersistenceOptions()
                        .setName("streamlens-" + connectionId);

                builder.tokenCachePersistenceOptions(cacheOptions);
                log.debug("Token cache persistence enabled for connection: {}", connectionId);
            } catch (Exception e) {
                log.warn("Token cache persistence not available, tokens will not be persisted: {}", e.getMessage());
            }

            return builder.build();
        });
    }

    /**
     * Gets a service principal credential (client credentials flow).
     *
     * @param tenantId     Azure AD tenant ID
     * @param clientId     Application (client) ID
     * @param clientSecret Client secret
     * @return TokenCredential for service principal authentication
     */
    public TokenCredential getServicePrincipalCredential(String tenantId, String clientId, String clientSecret) {
        return new ClientSecretCredentialBuilder()
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();
    }

    /**
     * Gets a managed identity credential.
     * Uses DefaultAzureCredential which tries multiple authentication methods.
     *
     * @return TokenCredential for managed identity authentication
     */
    public TokenCredential getManagedIdentityCredential() {
        return new DefaultAzureCredentialBuilder().build();
    }

    /**
     * Clears cached credential for a connection.
     * Call this when user wants to sign in with a different account.
     *
     * @param connectionId Connection identifier
     */
    public void clearCredential(String connectionId) {
        credentialCache.remove(connectionId);
        log.info("Cleared cached credential for connection: {}", connectionId);
    }

    /**
     * Clears all cached credentials.
     */
    public void clearAllCredentials() {
        credentialCache.clear();
        log.info("Cleared all cached credentials");
    }

    /**
     * Tests if the credential can authenticate successfully.
     * This will trigger the browser login if using interactive credentials.
     *
     * @param credential The credential to test
     * @return true if authentication succeeds
     */
    public boolean testCredential(TokenCredential credential) {
        try {
            // Request a token to verify authentication works
            var request = new com.azure.core.credential.TokenRequestContext()
                    .addScopes(EVENT_HUBS_SCOPE);

            credential.getToken(request).block();
            return true;
        } catch (Exception e) {
            log.error("Credential test failed", e);
            return false;
        }
    }
}
