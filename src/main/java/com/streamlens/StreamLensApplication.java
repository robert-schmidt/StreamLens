package com.streamlens;

import com.streamlens.controller.MainController;
import com.streamlens.service.kafka.LocalKafkaService;
import javafx.application.Application;
import javafx.application.HostServices;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

/**
 * StreamLens - Event Streaming Message Browser and Inspector
 *
 * Main entry point that integrates JavaFX with Spring Boot.
 * Pure JavaFX implementation (no AWT/Swing).
 */
public class StreamLensApplication extends Application {

    private ConfigurableApplicationContext springContext;
    private static HostServices hostServices;

    @Override
    public void init() {
        // Initialize Spring context before JavaFX starts
        springContext = new SpringApplicationBuilder(SpringBootApp.class)
                .headless(false)  // Required for GUI applications
                .run();
    }

    @Override
    public void start(Stage primaryStage) throws Exception {
        hostServices = getHostServices();

        // Load FXML with Spring bean factory for dependency injection
        FXMLLoader loader = new FXMLLoader(getClass().getResource("/fxml/main.fxml"));
        loader.setControllerFactory(springContext::getBean);

        Parent root = loader.load();

        // Pass host services to the controller for opening URLs
        MainController controller = loader.getController();
        controller.setHostServices(hostServices);

        Scene scene = new Scene(root, 1200, 800);

        // Load stylesheet
        String css = getClass().getResource("/css/streamlens.css").toExternalForm();
        scene.getStylesheets().add(css);

        primaryStage.setTitle("StreamLens");
        primaryStage.setScene(scene);
        primaryStage.setMinWidth(900);
        primaryStage.setMinHeight(600);
        primaryStage.show();

        // Handle window close
        primaryStage.setOnCloseRequest(event -> {
            stop();
            Platform.exit();
            System.exit(0);
        });
    }

    @Override
    public void stop() {
        if (springContext != null) {
            // Stop local Kafka if running
            try {
                LocalKafkaService localKafkaService = springContext.getBean(LocalKafkaService.class);
                if (localKafkaService.isDockerAvailable() &&
                    localKafkaService.getStatus() == LocalKafkaService.LocalKafkaStatus.RUNNING) {
                    System.out.println("Stopping local Kafka server...");
                    localKafkaService.stop();
                }
            } catch (Exception e) {
                System.err.println("Error stopping local Kafka: " + e.getMessage());
            }
            springContext.close();
        }
    }

    public static HostServices getAppHostServices() {
        return hostServices;
    }

    public static void main(String[] args) {
        launch(args);
    }

    /**
     * Inner Spring Boot configuration class.
     */
    @SpringBootApplication
    @ComponentScan(basePackages = "com.streamlens")
    public static class SpringBootApp {
    }
}
