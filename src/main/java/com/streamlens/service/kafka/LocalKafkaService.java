package com.streamlens.service.kafka;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.zerodep.ZerodepDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.streamlens.model.connection.ConnectionProfile;
import com.streamlens.model.connection.KafkaConnectionConfig;
import com.streamlens.service.storage.ConnectionRepository;
import javafx.application.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Service for managing a local Kafka instance via Docker.
 * Provides one-click start/stop of a development Kafka server.
 */
@Service
public class LocalKafkaService {

    private static final Logger log = LoggerFactory.getLogger(LocalKafkaService.class);

    private static final String KAFKA_IMAGE = "apache/kafka:latest";
    private static final String CONTAINER_NAME = "streamlens-kafka";
    private static final String LOCAL_CONNECTION_NAME = "Local Kafka";
    private static final int KAFKA_PORT = 9092;
    private static final int CONTROLLER_PORT = 9093;

    private final ConnectionRepository connectionRepository;

    private DockerClient dockerClient;
    private String containerId;
    private LocalKafkaStatus status = LocalKafkaStatus.UNKNOWN;
    private Consumer<LocalKafkaStatus> statusListener;
    private Consumer<String> logListener;

    public LocalKafkaService(ConnectionRepository connectionRepository) {
        this.connectionRepository = connectionRepository;
        initializeDockerClient();
    }

    private void initializeDockerClient() {
        try {
            // Determine Docker host based on OS
            String dockerHost = System.getenv("DOCKER_HOST");
            if (dockerHost == null || dockerHost.isEmpty()) {
                String os = System.getProperty("os.name", "").toLowerCase();
                if (os.contains("mac") || os.contains("linux")) {
                    dockerHost = "unix:///var/run/docker.sock";
                } else if (os.contains("win")) {
                    dockerHost = "npipe:////./pipe/docker_engine";
                } else {
                    dockerHost = "unix:///var/run/docker.sock";
                }
            }
            log.info("Using Docker host: {}", dockerHost);

            DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                    .withDockerHost(dockerHost)
                    .build();
            DockerHttpClient httpClient = new ZerodepDockerHttpClient.Builder()
                    .dockerHost(config.getDockerHost())
                    .sslConfig(config.getSSLConfig())
                    .maxConnections(100)
                    .connectionTimeout(Duration.ofSeconds(30))
                    .responseTimeout(Duration.ofSeconds(45))
                    .build();

            dockerClient = DockerClientImpl.getInstance(config, httpClient);

            // Check if Docker is available
            dockerClient.pingCmd().exec();
            log.info("Docker client initialized successfully");

            // Check for existing container
            checkExistingContainer();

        } catch (Throwable e) {
            // Catch Throwable to handle NoClassDefFoundError and other errors
            log.warn("Docker not available: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            dockerClient = null;
            status = LocalKafkaStatus.DOCKER_NOT_AVAILABLE;
        }
    }

    private void checkExistingContainer() {
        try {
            List<Container> containers = dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .withNameFilter(List.of(CONTAINER_NAME))
                    .exec();

            if (!containers.isEmpty()) {
                Container container = containers.get(0);
                containerId = container.getId();

                if ("running".equals(container.getState())) {
                    status = LocalKafkaStatus.RUNNING;
                    log.info("Found running local Kafka container: {}", containerId);
                } else {
                    status = LocalKafkaStatus.STOPPED;
                    log.info("Found stopped local Kafka container: {}", containerId);
                }
            } else {
                status = LocalKafkaStatus.NOT_CREATED;
            }
        } catch (Exception e) {
            log.error("Failed to check existing container", e);
            status = LocalKafkaStatus.ERROR;
        }
    }

    /**
     * Retry connecting to Docker. Call this if Docker was started after the app.
     * @return true if Docker is now available
     */
    public boolean retryDockerConnection() {
        log.info("Retrying Docker connection...");
        initializeDockerClient();
        boolean available = isDockerAvailable();
        if (available) {
            log.info("Docker connection established successfully");
            if (statusListener != null) {
                statusListener.accept(status);
            }
        }
        return available;
    }

    /**
     * Checks if Docker is available on this system.
     */
    public boolean isDockerAvailable() {
        if (dockerClient == null) return false;
        try {
            dockerClient.pingCmd().exec();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Gets the current status of the local Kafka server.
     */
    public LocalKafkaStatus getStatus() {
        return status;
    }

    /**
     * Sets a listener to receive status updates.
     */
    public void setStatusListener(Consumer<LocalKafkaStatus> listener) {
        this.statusListener = listener;
    }

    /**
     * Sets a listener to receive log output.
     */
    public void setLogListener(Consumer<String> listener) {
        this.logListener = listener;
    }

    private void updateStatus(LocalKafkaStatus newStatus) {
        this.status = newStatus;
        if (statusListener != null) {
            Platform.runLater(() -> statusListener.accept(newStatus));
        }
    }

    private void logMessage(String message) {
        log.info(message);
        if (logListener != null) {
            Platform.runLater(() -> logListener.accept(message));
        }
    }

    /**
     * Starts the local Kafka server.
     * Creates the container if it doesn't exist, or starts an existing one.
     */
    public CompletableFuture<Boolean> start() {
        return CompletableFuture.supplyAsync(() -> {
            if (!isDockerAvailable()) {
                logMessage("Docker is not available. Please install Docker Desktop.");
                updateStatus(LocalKafkaStatus.DOCKER_NOT_AVAILABLE);
                return false;
            }

            try {
                if (containerId == null) {
                    // Need to create container
                    return createAndStartContainer();
                } else {
                    // Start existing container
                    return startExistingContainer();
                }
            } catch (Exception e) {
                log.error("Failed to start local Kafka", e);
                logMessage("Error: " + e.getMessage());
                updateStatus(LocalKafkaStatus.ERROR);
                return false;
            }
        });
    }

    private boolean createAndStartContainer() {
        updateStatus(LocalKafkaStatus.PULLING_IMAGE);
        logMessage("Pulling Kafka image: " + KAFKA_IMAGE);

        try {
            // Pull the image
            dockerClient.pullImageCmd(KAFKA_IMAGE)
                    .exec(new PullImageResultCallback() {
                        @Override
                        public void onNext(PullResponseItem item) {
                            if (item.getStatus() != null) {
                                logMessage("Pull: " + item.getStatus());
                            }
                        }
                    })
                    .awaitCompletion(5, TimeUnit.MINUTES);

            logMessage("Image pulled successfully");

        } catch (Exception e) {
            // Image might already exist
            log.warn("Pull failed (image may already exist): {}", e.getMessage());
        }

        updateStatus(LocalKafkaStatus.CREATING);
        logMessage("Creating Kafka container...");

        // Create the container with KRaft mode (Apache Kafka official image)
        CreateContainerResponse container = dockerClient.createContainerCmd(KAFKA_IMAGE)
                .withName(CONTAINER_NAME)
                .withEnv(
                        // Apache Kafka native image uses KAFKA_* env vars that map to server.properties
                        "KAFKA_NODE_ID=1",
                        "KAFKA_PROCESS_ROLES=broker,controller",
                        "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
                        "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092",
                        "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                        "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
                        "KAFKA_NUM_PARTITIONS=3",
                        "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk"
                )
                .withHostConfig(HostConfig.newHostConfig()
                        .withPortBindings(
                                PortBinding.parse(KAFKA_PORT + ":9092"),
                                PortBinding.parse(CONTROLLER_PORT + ":9093")
                        )
                        .withRestartPolicy(RestartPolicy.noRestart())
                )
                .withExposedPorts(
                        ExposedPort.tcp(9092),
                        ExposedPort.tcp(9093)
                )
                .withLabels(java.util.Map.of(
                        "com.streamlens", "true",
                        "com.streamlens.service", "kafka"
                ))
                .exec();

        containerId = container.getId();
        logMessage("Container created: " + containerId.substring(0, 12));

        return startExistingContainer();
    }

    private boolean startExistingContainer() {
        updateStatus(LocalKafkaStatus.STARTING);
        logMessage("Starting Kafka container...");

        try {
            dockerClient.startContainerCmd(containerId).exec();
        } catch (NotModifiedException e) {
            // Already running
            log.info("Container already running");
        }

        // Wait for Kafka to be ready
        logMessage("Waiting for Kafka to be ready...");
        boolean ready = waitForKafkaReady(60);

        if (ready) {
            updateStatus(LocalKafkaStatus.RUNNING);
            logMessage("Local Kafka is running on localhost:" + KAFKA_PORT);

            // Create or update the local connection profile
            ensureLocalConnectionProfile();

            return true;
        } else {
            logMessage("Kafka failed to start within timeout");
            updateStatus(LocalKafkaStatus.ERROR);
            return false;
        }
    }

    private boolean waitForKafkaReady(int timeoutSeconds) {
        long start = System.currentTimeMillis();
        long timeout = timeoutSeconds * 1000L;

        while (System.currentTimeMillis() - start < timeout) {
            try {
                InspectContainerResponse inspect = dockerClient.inspectContainerCmd(containerId).exec();
                if (Boolean.TRUE.equals(inspect.getState().getRunning())) {
                    // Try to connect to Kafka
                    try (var socket = new java.net.Socket()) {
                        socket.connect(new java.net.InetSocketAddress("localhost", KAFKA_PORT), 1000);
                        // Give Kafka a moment to fully initialize
                        Thread.sleep(2000);
                        return true;
                    } catch (IOException e) {
                        // Not ready yet
                    }
                }
                Thread.sleep(1000);
            } catch (Exception e) {
                log.debug("Waiting for Kafka: {}", e.getMessage());
            }
        }
        return false;
    }

    private void ensureLocalConnectionProfile() {
        try {
            // Check if local connection already exists
            List<ConnectionProfile> profiles = connectionRepository.findAll();
            boolean exists = profiles.stream()
                    .anyMatch(p -> LOCAL_CONNECTION_NAME.equals(p.getName()));

            if (!exists) {
                // Create local connection profile
                KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig();
                kafkaConfig.setBootstrapServers("localhost:" + KAFKA_PORT);
                kafkaConfig.setAuthenticationType(KafkaConnectionConfig.AuthenticationType.NONE);

                ConnectionProfile profile = new ConnectionProfile();
                profile.setId(UUID.randomUUID().toString());
                profile.setName(LOCAL_CONNECTION_NAME);
                profile.setType(ConnectionProfile.ConnectionType.KAFKA);
                profile.setConfig(kafkaConfig);

                connectionRepository.save(profile);
                logMessage("Created 'Local Kafka' connection profile");
            }
        } catch (Exception e) {
            log.warn("Failed to create local connection profile", e);
        }
    }

    /**
     * Stops the local Kafka server.
     */
    public CompletableFuture<Boolean> stop() {
        return CompletableFuture.supplyAsync(() -> {
            if (containerId == null) {
                logMessage("No local Kafka container to stop");
                return true;
            }

            try {
                updateStatus(LocalKafkaStatus.STOPPING);
                logMessage("Stopping Kafka container...");

                dockerClient.stopContainerCmd(containerId)
                        .withTimeout(30)
                        .exec();

                updateStatus(LocalKafkaStatus.STOPPED);
                logMessage("Local Kafka stopped");
                return true;

            } catch (NotModifiedException e) {
                // Already stopped
                updateStatus(LocalKafkaStatus.STOPPED);
                return true;
            } catch (Exception e) {
                log.error("Failed to stop local Kafka", e);
                logMessage("Error stopping: " + e.getMessage());
                updateStatus(LocalKafkaStatus.ERROR);
                return false;
            }
        });
    }

    /**
     * Removes the local Kafka container and its data.
     */
    public CompletableFuture<Boolean> remove() {
        return CompletableFuture.supplyAsync(() -> {
            if (containerId == null) {
                logMessage("No local Kafka container to remove");
                return true;
            }

            try {
                // Stop first if running
                if (status == LocalKafkaStatus.RUNNING) {
                    stop().get(30, TimeUnit.SECONDS);
                }

                updateStatus(LocalKafkaStatus.REMOVING);
                logMessage("Removing Kafka container...");

                dockerClient.removeContainerCmd(containerId)
                        .withRemoveVolumes(true)
                        .exec();

                containerId = null;
                updateStatus(LocalKafkaStatus.NOT_CREATED);
                logMessage("Local Kafka container removed");
                return true;

            } catch (NotFoundException e) {
                containerId = null;
                updateStatus(LocalKafkaStatus.NOT_CREATED);
                return true;
            } catch (Exception e) {
                log.error("Failed to remove local Kafka", e);
                logMessage("Error removing: " + e.getMessage());
                updateStatus(LocalKafkaStatus.ERROR);
                return false;
            }
        });
    }

    /**
     * Gets the logs from the Kafka container.
     */
    public void streamLogs(Consumer<String> logConsumer) {
        if (containerId == null) return;

        try {
            dockerClient.logContainerCmd(containerId)
                    .withStdOut(true)
                    .withStdErr(true)
                    .withFollowStream(true)
                    .withTailAll()
                    .exec(new ResultCallback.Adapter<Frame>() {
                        @Override
                        public void onNext(Frame frame) {
                            String log = new String(frame.getPayload()).trim();
                            if (!log.isEmpty()) {
                                Platform.runLater(() -> logConsumer.accept(log));
                            }
                        }
                    });
        } catch (Exception e) {
            log.error("Failed to stream logs", e);
        }
    }

    /**
     * Gets container resource usage stats.
     */
    public ContainerStats getStats() {
        if (containerId == null || status != LocalKafkaStatus.RUNNING) {
            return null;
        }

        try {
            final Statistics[] statsHolder = new Statistics[1];
            ResultCallback.Adapter<Statistics> callback = new ResultCallback.Adapter<>() {
                @Override
                public void onNext(Statistics statistics) {
                    statsHolder[0] = statistics;
                    try { close(); } catch (IOException ignored) {}
                }
            };

            dockerClient.statsCmd(containerId).exec(callback);
            callback.awaitCompletion(5, TimeUnit.SECONDS);

            Statistics stats = statsHolder[0];
            if (stats != null && stats.getMemoryStats() != null) {
                return new ContainerStats(
                        stats.getMemoryStats().getUsage(),
                        stats.getMemoryStats().getLimit(),
                        calculateCpuPercent(stats)
                );
            }
        } catch (Exception e) {
            log.debug("Failed to get stats: {}", e.getMessage());
        }
        return null;
    }

    private double calculateCpuPercent(Statistics stats) {
        try {
            long cpuDelta = stats.getCpuStats().getCpuUsage().getTotalUsage()
                    - stats.getPreCpuStats().getCpuUsage().getTotalUsage();
            long systemDelta = stats.getCpuStats().getSystemCpuUsage()
                    - stats.getPreCpuStats().getSystemCpuUsage();
            int cpuCount = stats.getCpuStats().getCpuUsage().getPercpuUsage().size();

            if (systemDelta > 0 && cpuDelta > 0) {
                return (double) cpuDelta / systemDelta * cpuCount * 100.0;
            }
        } catch (Exception ignored) {}
        return 0.0;
    }

    /**
     * Gets the bootstrap servers for the local Kafka instance.
     */
    public String getBootstrapServers() {
        return "localhost:" + KAFKA_PORT;
    }

    @PreDestroy
    public void cleanup() {
        if (dockerClient != null) {
            try {
                ((Closeable) dockerClient).close();
            } catch (Exception ignored) {}
        }
    }

    // ============== Data Classes ==============

    public enum LocalKafkaStatus {
        UNKNOWN("Unknown"),
        DOCKER_NOT_AVAILABLE("Docker Not Available"),
        NOT_CREATED("Not Created"),
        PULLING_IMAGE("Pulling Image..."),
        CREATING("Creating..."),
        STARTING("Starting..."),
        RUNNING("Running"),
        STOPPING("Stopping..."),
        STOPPED("Stopped"),
        REMOVING("Removing..."),
        ERROR("Error");

        private final String displayName;

        LocalKafkaStatus(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }
    }

    public record ContainerStats(Long memoryUsage, Long memoryLimit, Double cpuPercent) {
        public String getMemoryUsageFormatted() {
            if (memoryUsage == null) return "N/A";
            return String.format("%.1f MB", memoryUsage / 1024.0 / 1024.0);
        }

        public String getMemoryLimitFormatted() {
            if (memoryLimit == null) return "N/A";
            return String.format("%.1f MB", memoryLimit / 1024.0 / 1024.0);
        }

        public String getCpuPercentFormatted() {
            if (cpuPercent == null) return "N/A";
            return String.format("%.1f%%", cpuPercent);
        }
    }
}
