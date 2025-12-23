package com.streamlens.model.filter;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents a saved filter configuration preset.
 */
public class FilterPreset {

    private String id;
    private String name;

    // Quick search (searches both key and value)
    private String quickSearch;

    // Time range filter
    private Instant startTime;
    private Instant endTime;

    // Key filter
    private String keyPattern;
    private boolean keyPatternIsRegex;

    // Body filter (JSONPath)
    private String jsonPathExpression;
    private String jsonPathValue;

    // Metadata
    private Instant createdAt;
    private Instant updatedAt;

    public FilterPreset() {
        this.id = UUID.randomUUID().toString();
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public FilterPreset(String name) {
        this();
        this.name = name;
    }

    /**
     * Checks if any filter is active.
     */
    public boolean hasActiveFilters() {
        return hasQuickSearch() || hasTimeRangeFilter() || hasKeyFilter() || hasJsonPathFilter();
    }

    /**
     * Checks if quick search is configured.
     */
    public boolean hasQuickSearch() {
        return quickSearch != null && !quickSearch.isEmpty();
    }

    /**
     * Checks if time range filter is configured.
     */
    public boolean hasTimeRangeFilter() {
        return startTime != null || endTime != null;
    }

    /**
     * Checks if key filter is configured.
     */
    public boolean hasKeyFilter() {
        return keyPattern != null && !keyPattern.isEmpty();
    }

    /**
     * Checks if JSONPath filter is configured.
     */
    public boolean hasJsonPathFilter() {
        return jsonPathExpression != null && !jsonPathExpression.isEmpty();
    }

    // Getters and Setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQuickSearch() {
        return quickSearch;
    }

    public void setQuickSearch(String quickSearch) {
        this.quickSearch = quickSearch;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    public String getKeyPattern() {
        return keyPattern;
    }

    public void setKeyPattern(String keyPattern) {
        this.keyPattern = keyPattern;
    }

    public boolean isKeyPatternIsRegex() {
        return keyPatternIsRegex;
    }

    public void setKeyPatternIsRegex(boolean keyPatternIsRegex) {
        this.keyPatternIsRegex = keyPatternIsRegex;
    }

    public String getJsonPathExpression() {
        return jsonPathExpression;
    }

    public void setJsonPathExpression(String jsonPathExpression) {
        this.jsonPathExpression = jsonPathExpression;
    }

    public String getJsonPathValue() {
        return jsonPathValue;
    }

    public void setJsonPathValue(String jsonPathValue) {
        this.jsonPathValue = jsonPathValue;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public String toString() {
        return name;
    }
}
