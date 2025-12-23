package com.streamlens.service.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streamlens.model.message.StreamMessage;
import com.streamlens.service.storage.DatabaseService;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.websocket.WsContext;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Local API service providing REST and WebSocket endpoints for messages.
 */
@Service
public class LocalApiService {

    private static final Logger log = LoggerFactory.getLogger(LocalApiService.class);

    private final DatabaseService databaseService;
    private final ObjectMapper objectMapper;

    private Javalin app;
    private int port = 9876;
    private boolean running = false;

    // WebSocket clients
    private final Set<WsContext> wsClients = ConcurrentHashMap.newKeySet();

    // Message supplier (set by MainController)
    private Supplier<List<StreamMessage>> messageSupplier;

    public LocalApiService(DatabaseService databaseService) {
        this.databaseService = databaseService;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * Starts the API server on the configured port.
     */
    public synchronized void start(int port) {
        if (running) {
            log.warn("API server already running");
            return;
        }

        this.port = port;

        try {
            app = Javalin.create(config -> {
                config.showJavalinBanner = false;
            });

            // REST endpoints
            app.get("/api/messages", this::handleGetMessages);
            app.get("/api/messages/{id}", this::handleGetMessage);
            app.get("/api/status", this::handleStatus);

            // WebSocket endpoint
            app.ws("/ws/messages", ws -> {
                ws.onConnect(ctx -> {
                    log.info("WebSocket client connected: {}", ctx.session.getRemoteAddress());
                    wsClients.add(ctx);
                });
                ws.onClose(ctx -> {
                    log.info("WebSocket client disconnected: {}", ctx.session.getRemoteAddress());
                    wsClients.remove(ctx);
                });
                ws.onError(ctx -> {
                    log.error("WebSocket error: {}", ctx.error() != null ? ctx.error().getMessage() : "unknown");
                    wsClients.remove(ctx);
                });
            });

            app.start(port);
            running = true;
            log.info("Local API server started on port {}", port);
        } catch (Exception e) {
            log.error("Failed to start API server on port {}", port, e);
            throw new RuntimeException("Failed to start API server: " + e.getMessage(), e);
        }
    }

    /**
     * Stops the API server.
     */
    @PreDestroy
    public synchronized void stop() {
        if (!running || app == null) {
            return;
        }

        try {
            // Close all WebSocket connections
            for (WsContext client : wsClients) {
                try {
                    client.closeSession();
                } catch (Exception e) {
                    // Ignore
                }
            }
            wsClients.clear();

            app.stop();
            running = false;
            log.info("Local API server stopped");
        } catch (Exception e) {
            log.error("Error stopping API server", e);
        }
    }

    /**
     * Checks if the API server is running.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Gets the current port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the message supplier for retrieving current messages.
     */
    public void setMessageSupplier(Supplier<List<StreamMessage>> supplier) {
        this.messageSupplier = supplier;
    }

    /**
     * Broadcasts a new message to all connected WebSocket clients.
     */
    public void broadcastMessage(StreamMessage message) {
        if (wsClients.isEmpty()) {
            return;
        }

        try {
            String json = objectMapper.writeValueAsString(message);
            for (WsContext client : wsClients) {
                try {
                    client.send(json);
                } catch (Exception e) {
                    log.debug("Failed to send to WebSocket client: {}", e.getMessage());
                    wsClients.remove(client);
                }
            }
        } catch (Exception e) {
            log.error("Failed to serialize message for broadcast", e);
        }
    }

    // REST Handlers

    private void handleGetMessages(Context ctx) {
        try {
            List<StreamMessage> messages;

            if (messageSupplier != null) {
                messages = messageSupplier.get();
            } else {
                messages = Collections.emptyList();
            }

            // Optional query parameters
            String limitParam = ctx.queryParam("limit");
            String offsetParam = ctx.queryParam("offset");

            int limit = limitParam != null ? Integer.parseInt(limitParam) : 100;
            int offset = offsetParam != null ? Integer.parseInt(offsetParam) : 0;

            // Apply pagination
            int endIndex = Math.min(offset + limit, messages.size());
            List<StreamMessage> pagedMessages = offset < messages.size()
                    ? messages.subList(offset, endIndex)
                    : Collections.emptyList();

            ctx.json(new MessagesResponse(pagedMessages, messages.size(), offset, limit));
        } catch (Exception e) {
            log.error("Error handling GET /api/messages", e);
            ctx.status(500).json(new ErrorResponse("Internal server error: " + e.getMessage()));
        }
    }

    private void handleGetMessage(Context ctx) {
        try {
            String id = ctx.pathParam("id");

            if (messageSupplier != null) {
                for (StreamMessage msg : messageSupplier.get()) {
                    if (msg.getId().equals(id)) {
                        ctx.json(msg);
                        return;
                    }
                }
            }

            ctx.status(404).json(new ErrorResponse("Message not found: " + id));
        } catch (Exception e) {
            log.error("Error handling GET /api/messages/{}", ctx.pathParam("id"), e);
            ctx.status(500).json(new ErrorResponse("Internal server error: " + e.getMessage()));
        }
    }

    private void handleStatus(Context ctx) {
        String apiEnabled = databaseService.getSetting("api.enabled", "false");
        ctx.json(new StatusResponse(
                running,
                port,
                wsClients.size(),
                messageSupplier != null ? messageSupplier.get().size() : 0
        ));
    }

    // Response DTOs

    public record MessagesResponse(
            List<StreamMessage> messages,
            int total,
            int offset,
            int limit
    ) {}

    public record ErrorResponse(String error) {}

    public record StatusResponse(
            boolean running,
            int port,
            int connectedClients,
            int messageCount
    ) {}
}
