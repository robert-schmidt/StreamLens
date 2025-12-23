package com.streamlens.controller;

import javafx.application.HostServices;
import javafx.fxml.FXML;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Controller for the About dialog.
 * Displays application information, build details, and useful links.
 */
@Component
public class AboutDialogController {

    private static final String APP_VERSION = "1.0.0";
    private static final String WEBSITE_URL = "https://robertschmidt.dev";
    private static final String GITHUB_URL = "https://github.com/robert-schmidt/StreamLens";
    private static final String COFFEE_URL = "https://buymeacoffee.com/robbschmidt";
    private static final String ISSUES_URL = "https://github.com/robert-schmidt/StreamLens/issues";
    private static final String PR_URL = "https://github.com/robert-schmidt/StreamLens/pulls";

    @FXML private Label versionLabel;
    @FXML private Label buildVersionLabel;
    @FXML private Label buildDateLabel;
    @FXML private Label javaVersionLabel;
    @FXML private Label javafxVersionLabel;
    @FXML private Label copyrightLabel;
    @FXML private Hyperlink websiteLink;
    @FXML private Hyperlink githubLink;
    @FXML private Hyperlink coffeeLink;

    private HostServices hostServices;

    @FXML
    public void initialize() {
        // Set version info
        versionLabel.setText("Version " + APP_VERSION);
        buildVersionLabel.setText(APP_VERSION);

        // Build date - use current date as build date
        buildDateLabel.setText(LocalDate.now().format(DateTimeFormatter.ofPattern("MMMM d, yyyy")));

        // Java version info
        javaVersionLabel.setText(System.getProperty("java.version") + " (" + System.getProperty("java.vendor") + ")");
        javafxVersionLabel.setText(System.getProperty("javafx.version", "21"));

        // Copyright year
        int currentYear = LocalDate.now().getYear();
        copyrightLabel.setText("Copyright " + currentYear + " Robert Schmidt");
    }

    /**
     * Set the host services for opening URLs in the browser.
     */
    public void setHostServices(HostServices hostServices) {
        this.hostServices = hostServices;
    }

    @FXML
    private void handleWebsiteLink() {
        openUrl(WEBSITE_URL);
    }

    @FXML
    private void handleGitHubLink() {
        openUrl(GITHUB_URL);
    }

    @FXML
    private void handleCoffeeLink() {
        openUrl(COFFEE_URL);
    }

    @FXML
    private void handleReportIssue() {
        openUrl(ISSUES_URL + "/new");
    }

    @FXML
    private void handleOpenPR() {
        openUrl(PR_URL);
    }

    private void openUrl(String url) {
        if (hostServices != null) {
            hostServices.showDocument(url);
        } else {
            // Fallback: try to open using Desktop
            try {
                if (java.awt.Desktop.isDesktopSupported()) {
                    java.awt.Desktop.getDesktop().browse(new java.net.URI(url));
                }
            } catch (Exception e) {
                // Log error but don't crash
                System.err.println("Failed to open URL: " + url);
            }
        }
    }
}
