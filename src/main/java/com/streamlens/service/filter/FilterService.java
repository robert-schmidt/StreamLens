package com.streamlens.service.filter;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.streamlens.model.filter.FilterPreset;
import com.streamlens.model.message.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Service for filtering messages based on various criteria.
 */
@Service
public class FilterService {

    private static final Logger log = LoggerFactory.getLogger(FilterService.class);

    /**
     * Filters a list of messages based on a filter preset.
     */
    public List<StreamMessage> filter(List<StreamMessage> messages, FilterPreset preset) {
        if (preset == null || !preset.hasActiveFilters()) {
            return messages;
        }

        return messages.stream()
                .filter(msg -> matchesFilter(msg, preset))
                .collect(Collectors.toList());
    }

    /**
     * Checks if a single message matches the filter criteria.
     */
    public boolean matchesFilter(StreamMessage message, FilterPreset preset) {
        if (preset == null || !preset.hasActiveFilters()) {
            return true;
        }

        // Quick search (searches both key and value)
        if (preset.hasQuickSearch()) {
            if (!matchesQuickSearch(message, preset.getQuickSearch())) {
                return false;
            }
        }

        // Time range filter
        if (preset.hasTimeRangeFilter()) {
            if (!matchesTimeRange(message, preset.getStartTime(), preset.getEndTime())) {
                return false;
            }
        }

        // Key filter
        if (preset.hasKeyFilter()) {
            if (!matchesKeyPattern(message, preset.getKeyPattern(), preset.isKeyPatternIsRegex())) {
                return false;
            }
        }

        // JSONPath filter
        if (preset.hasJsonPathFilter()) {
            if (!matchesJsonPath(message, preset.getJsonPathExpression(), preset.getJsonPathValue())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if message key or value contains the quick search term (case-insensitive).
     */
    private boolean matchesQuickSearch(StreamMessage message, String searchTerm) {
        if (searchTerm == null || searchTerm.isEmpty()) {
            return true;
        }

        String lowerSearch = searchTerm.toLowerCase();

        // Check key
        String key = message.getKey();
        if (key != null && key.toLowerCase().contains(lowerSearch)) {
            return true;
        }

        // Check value
        String value = message.getValueAsString();
        if (value != null && value.toLowerCase().contains(lowerSearch)) {
            return true;
        }

        return false;
    }

    /**
     * Checks if message timestamp is within the specified time range.
     */
    private boolean matchesTimeRange(StreamMessage message, Instant startTime, Instant endTime) {
        Instant msgTime = message.getTimestamp();
        if (msgTime == null) {
            return false;
        }

        if (startTime != null && msgTime.isBefore(startTime)) {
            return false;
        }

        if (endTime != null && msgTime.isAfter(endTime)) {
            return false;
        }

        return true;
    }

    /**
     * Checks if message key matches the pattern.
     */
    private boolean matchesKeyPattern(StreamMessage message, String pattern, boolean isRegex) {
        String key = message.getKey();
        if (key == null) {
            return pattern == null || pattern.isEmpty();
        }

        if (isRegex) {
            try {
                Pattern regex = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
                return regex.matcher(key).find();
            } catch (PatternSyntaxException e) {
                log.warn("Invalid regex pattern: {}", pattern);
                return false;
            }
        } else {
            // Simple contains match
            return key.toLowerCase().contains(pattern.toLowerCase());
        }
    }

    /**
     * Checks if message body matches the JSONPath expression.
     */
    private boolean matchesJsonPath(StreamMessage message, String jsonPathExpression, String expectedValue) {
        String body = message.getValueAsString();
        if (body == null || body.isEmpty()) {
            return false;
        }

        try {
            Object result = JsonPath.read(body, jsonPathExpression);

            if (expectedValue == null || expectedValue.isEmpty()) {
                // Just check if path exists and has a value
                return result != null;
            }

            // Compare the result to the expected value
            String resultStr = result != null ? result.toString() : "";
            return resultStr.equals(expectedValue) ||
                   resultStr.toLowerCase().contains(expectedValue.toLowerCase());

        } catch (PathNotFoundException e) {
            // Path doesn't exist in this message
            return false;
        } catch (Exception e) {
            log.debug("JSONPath evaluation failed for expression '{}': {}", jsonPathExpression, e.getMessage());
            return false;
        }
    }

    /**
     * Validates a JSONPath expression.
     */
    public String validateJsonPath(String expression) {
        if (expression == null || expression.isEmpty()) {
            return null;
        }

        try {
            JsonPath.compile(expression);
            return null; // Valid
        } catch (Exception e) {
            return "Invalid JSONPath: " + e.getMessage();
        }
    }

    /**
     * Validates a regex pattern.
     */
    public String validateRegex(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return null;
        }

        try {
            Pattern.compile(pattern);
            return null; // Valid
        } catch (PatternSyntaxException e) {
            return "Invalid regex: " + e.getMessage();
        }
    }
}
