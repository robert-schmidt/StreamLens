package com.streamlens.controller;

import com.streamlens.model.filter.FilterPreset;
import com.streamlens.service.filter.FilterService;
import com.streamlens.service.storage.FilterPresetRepository;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ResourceBundle;

/**
 * Controller for the filter dialog.
 */
@Component
public class FilterDialogController implements Initializable {

    private final FilterService filterService;
    private final FilterPresetRepository filterPresetRepository;

    // Preset
    @FXML private ComboBox<FilterPreset> presetComboBox;
    @FXML private Button savePresetButton;
    @FXML private Button deletePresetButton;

    // Quick Search
    @FXML private CheckBox quickSearchEnabledCheckBox;
    @FXML private TextField quickSearchField;

    // Time Range
    @FXML private CheckBox timeRangeEnabledCheckBox;
    @FXML private DatePicker startDatePicker;
    @FXML private TextField startTimeField;
    @FXML private DatePicker endDatePicker;
    @FXML private TextField endTimeField;

    // Key Filter
    @FXML private CheckBox keyFilterEnabledCheckBox;
    @FXML private TextField keyPatternField;
    @FXML private CheckBox keyRegexCheckBox;

    // JSONPath Filter
    @FXML private CheckBox jsonPathEnabledCheckBox;
    @FXML private TextField jsonPathExpressionField;
    @FXML private TextField jsonPathValueField;

    // Validation
    @FXML private Label validationLabel;

    private final ObservableList<FilterPreset> presets = FXCollections.observableArrayList();
    private FilterPreset currentPreset;

    public FilterDialogController(FilterService filterService, FilterPresetRepository filterPresetRepository) {
        this.filterService = filterService;
        this.filterPresetRepository = filterPresetRepository;
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        presetComboBox.setItems(presets);
        presetComboBox.setConverter(new javafx.util.StringConverter<>() {
            @Override
            public String toString(FilterPreset preset) {
                return preset != null ? preset.getName() : "";
            }

            @Override
            public FilterPreset fromString(String string) {
                return presets.stream()
                        .filter(p -> p.getName().equals(string))
                        .findFirst()
                        .orElse(null);
            }
        });

        presetComboBox.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {
                loadPreset(newVal);
            }
        });

        // Bind enable states
        quickSearchField.disableProperty().bind(quickSearchEnabledCheckBox.selectedProperty().not());

        startDatePicker.disableProperty().bind(timeRangeEnabledCheckBox.selectedProperty().not());
        startTimeField.disableProperty().bind(timeRangeEnabledCheckBox.selectedProperty().not());
        endDatePicker.disableProperty().bind(timeRangeEnabledCheckBox.selectedProperty().not());
        endTimeField.disableProperty().bind(timeRangeEnabledCheckBox.selectedProperty().not());

        keyPatternField.disableProperty().bind(keyFilterEnabledCheckBox.selectedProperty().not());
        keyRegexCheckBox.disableProperty().bind(keyFilterEnabledCheckBox.selectedProperty().not());

        jsonPathExpressionField.disableProperty().bind(jsonPathEnabledCheckBox.selectedProperty().not());
        jsonPathValueField.disableProperty().bind(jsonPathEnabledCheckBox.selectedProperty().not());

        // Validation on change
        jsonPathExpressionField.textProperty().addListener((obs, oldVal, newVal) -> validateJsonPath());
        keyPatternField.textProperty().addListener((obs, oldVal, newVal) -> validateKeyPattern());

        // Load presets from database
        loadPresetsFromDatabase();
    }

    private void loadPresetsFromDatabase() {
        presets.clear();
        presets.addAll(filterPresetRepository.findAll());
    }

    private void loadPreset(FilterPreset preset) {
        currentPreset = preset;

        // Quick search
        if (preset.getQuickSearch() != null && !preset.getQuickSearch().isEmpty()) {
            quickSearchEnabledCheckBox.setSelected(true);
            quickSearchField.setText(preset.getQuickSearch());
        }

        // Time range
        if (preset.getStartTime() != null) {
            timeRangeEnabledCheckBox.setSelected(true);
            LocalDateTime startLdt = LocalDateTime.ofInstant(preset.getStartTime(), ZoneId.systemDefault());
            startDatePicker.setValue(startLdt.toLocalDate());
            startTimeField.setText(startLdt.toLocalTime().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        }
        if (preset.getEndTime() != null) {
            LocalDateTime endLdt = LocalDateTime.ofInstant(preset.getEndTime(), ZoneId.systemDefault());
            endDatePicker.setValue(endLdt.toLocalDate());
            endTimeField.setText(endLdt.toLocalTime().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        }

        // Key filter
        if (preset.getKeyPattern() != null && !preset.getKeyPattern().isEmpty()) {
            keyFilterEnabledCheckBox.setSelected(true);
            keyPatternField.setText(preset.getKeyPattern());
            keyRegexCheckBox.setSelected(preset.isKeyPatternIsRegex());
        }

        // JSONPath filter
        if (preset.getJsonPathExpression() != null && !preset.getJsonPathExpression().isEmpty()) {
            jsonPathEnabledCheckBox.setSelected(true);
            jsonPathExpressionField.setText(preset.getJsonPathExpression());
            jsonPathValueField.setText(preset.getJsonPathValue());
        }
    }

    @FXML
    private void handleSavePreset() {
        TextInputDialog dialog = new TextInputDialog(currentPreset != null ? currentPreset.getName() : "");
        dialog.setTitle("Save Filter Preset");
        dialog.setHeaderText("Enter a name for this filter preset:");
        dialog.setContentText("Name:");

        dialog.showAndWait().ifPresent(name -> {
            if (!name.trim().isEmpty()) {
                FilterPreset preset = buildPreset();
                preset.setName(name.trim());

                // Save to database
                filterPresetRepository.saveOrUpdate(preset);

                // Reload presets and select the saved one
                loadPresetsFromDatabase();
                presets.stream()
                        .filter(p -> p.getId().equals(preset.getId()))
                        .findFirst()
                        .ifPresent(p -> presetComboBox.getSelectionModel().select(p));
            }
        });
    }

    @FXML
    private void handleDeletePreset() {
        FilterPreset selected = presetComboBox.getSelectionModel().getSelectedItem();
        if (selected != null) {
            // Delete from database
            filterPresetRepository.delete(selected.getId());

            // Reload presets and clear form
            loadPresetsFromDatabase();
            clearForm();
        }
    }

    private void clearForm() {
        currentPreset = null;
        quickSearchEnabledCheckBox.setSelected(false);
        quickSearchField.clear();
        timeRangeEnabledCheckBox.setSelected(false);
        startDatePicker.setValue(null);
        startTimeField.clear();
        endDatePicker.setValue(null);
        endTimeField.clear();
        keyFilterEnabledCheckBox.setSelected(false);
        keyPatternField.clear();
        keyRegexCheckBox.setSelected(false);
        jsonPathEnabledCheckBox.setSelected(false);
        jsonPathExpressionField.clear();
        jsonPathValueField.clear();
        validationLabel.setText("");
    }

    /**
     * Clears all filters (called from Clear All button).
     */
    public void clearAll() {
        clearForm();
        presetComboBox.getSelectionModel().clearSelection();
    }

    private void validateJsonPath() {
        if (!jsonPathEnabledCheckBox.isSelected()) {
            validationLabel.setText("");
            return;
        }
        String expr = jsonPathExpressionField.getText();
        String error = filterService.validateJsonPath(expr);
        if (error != null) {
            validationLabel.setText(error);
            validationLabel.setStyle("-fx-text-fill: #f44336;");
        } else {
            validationLabel.setText("JSONPath is valid");
            validationLabel.setStyle("-fx-text-fill: #4caf50;");
        }
    }

    private void validateKeyPattern() {
        if (!keyFilterEnabledCheckBox.isSelected() || !keyRegexCheckBox.isSelected()) {
            return;
        }
        String pattern = keyPatternField.getText();
        String error = filterService.validateRegex(pattern);
        if (error != null) {
            validationLabel.setText(error);
            validationLabel.setStyle("-fx-text-fill: #f44336;");
        } else {
            validationLabel.setText("");
        }
    }

    /**
     * Builds a FilterPreset from the current form values.
     */
    public FilterPreset buildPreset() {
        FilterPreset preset = currentPreset != null ? currentPreset : new FilterPreset();

        // Quick search
        if (quickSearchEnabledCheckBox.isSelected() && !quickSearchField.getText().trim().isEmpty()) {
            preset.setQuickSearch(quickSearchField.getText().trim());
        } else {
            preset.setQuickSearch(null);
        }

        // Time range
        if (timeRangeEnabledCheckBox.isSelected()) {
            preset.setStartTime(parseDateTime(startDatePicker.getValue(), startTimeField.getText()));
            preset.setEndTime(parseDateTime(endDatePicker.getValue(), endTimeField.getText()));
        } else {
            preset.setStartTime(null);
            preset.setEndTime(null);
        }

        // Key filter
        if (keyFilterEnabledCheckBox.isSelected()) {
            preset.setKeyPattern(keyPatternField.getText());
            preset.setKeyPatternIsRegex(keyRegexCheckBox.isSelected());
        } else {
            preset.setKeyPattern(null);
            preset.setKeyPatternIsRegex(false);
        }

        // JSONPath filter
        if (jsonPathEnabledCheckBox.isSelected()) {
            preset.setJsonPathExpression(jsonPathExpressionField.getText());
            preset.setJsonPathValue(jsonPathValueField.getText());
        } else {
            preset.setJsonPathExpression(null);
            preset.setJsonPathValue(null);
        }

        return preset;
    }

    private Instant parseDateTime(LocalDate date, String timeStr) {
        if (date == null) return null;

        LocalTime time = LocalTime.MIDNIGHT;
        if (timeStr != null && !timeStr.trim().isEmpty()) {
            try {
                time = LocalTime.parse(timeStr.trim(), DateTimeFormatter.ofPattern("HH:mm:ss"));
            } catch (DateTimeParseException e) {
                try {
                    time = LocalTime.parse(timeStr.trim(), DateTimeFormatter.ofPattern("HH:mm"));
                } catch (DateTimeParseException e2) {
                    // Use midnight
                }
            }
        }

        return LocalDateTime.of(date, time).atZone(ZoneId.systemDefault()).toInstant();
    }

    /**
     * Validates the form and returns error message, or null if valid.
     */
    public String validate() {
        if (keyFilterEnabledCheckBox.isSelected() && keyRegexCheckBox.isSelected()) {
            String error = filterService.validateRegex(keyPatternField.getText());
            if (error != null) return error;
        }

        if (jsonPathEnabledCheckBox.isSelected()) {
            String error = filterService.validateJsonPath(jsonPathExpressionField.getText());
            if (error != null) return error;
        }

        return null;
    }

    /**
     * Sets the list of available presets.
     */
    public void setPresets(java.util.List<FilterPreset> presetList) {
        presets.clear();
        presets.addAll(presetList);
    }

    /**
     * Sets the current filter preset to edit.
     */
    public void setCurrentPreset(FilterPreset preset) {
        if (preset != null) {
            loadPreset(preset);
        } else {
            clearForm();
        }
    }
}
