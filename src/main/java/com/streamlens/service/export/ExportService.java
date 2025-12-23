package com.streamlens.service.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streamlens.model.message.MessageHeader;
import com.streamlens.model.message.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Service for exporting messages to various formats.
 */
@Service
public class ExportService {

    private static final Logger log = LoggerFactory.getLogger(ExportService.class);
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZone(ZoneId.systemDefault());

    private final ObjectMapper objectMapper;

    public ExportService() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Exports messages to JSON format.
     */
    public void exportToJson(List<StreamMessage> messages, File file, Consumer<Integer> progressCallback)
            throws IOException {
        log.info("Exporting {} messages to JSON: {}", messages.size(), file.getAbsolutePath());

        List<Map<String, Object>> exportData = messages.stream()
                .map(this::messageToMap)
                .toList();

        objectMapper.writeValue(file, exportData);

        if (progressCallback != null) {
            progressCallback.accept(messages.size());
        }

        log.info("Export complete: {} messages", messages.size());
    }

    /**
     * Exports messages to NDJSON (Newline Delimited JSON) format.
     */
    public void exportToNdjson(List<StreamMessage> messages, File file, Consumer<Integer> progressCallback)
            throws IOException {
        log.info("Exporting {} messages to NDJSON: {}", messages.size(), file.getAbsolutePath());

        ObjectMapper lineMapper = new ObjectMapper();
        lineMapper.registerModule(new JavaTimeModule());
        lineMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, StandardCharsets.UTF_8))) {
            int count = 0;
            for (StreamMessage message : messages) {
                Map<String, Object> map = messageToMap(message);
                String line = lineMapper.writeValueAsString(map);
                writer.write(line);
                writer.newLine();

                count++;
                if (progressCallback != null && count % 100 == 0) {
                    progressCallback.accept(count);
                }
            }

            if (progressCallback != null) {
                progressCallback.accept(count);
            }
        }

        log.info("Export complete: {} messages", messages.size());
    }

    /**
     * Exports messages to CSV format.
     */
    public void exportToCsv(List<StreamMessage> messages, File file, Consumer<Integer> progressCallback)
            throws IOException {
        log.info("Exporting {} messages to CSV: {}", messages.size(), file.getAbsolutePath());

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, StandardCharsets.UTF_8))) {
            // Header
            writer.write("timestamp,topic,partition,offset,key,value,headers");
            writer.newLine();

            int count = 0;
            for (StreamMessage message : messages) {
                StringBuilder line = new StringBuilder();

                // Timestamp
                line.append(formatCsvValue(formatTimestamp(message.getTimestamp()))).append(",");

                // Topic
                line.append(formatCsvValue(message.getTopic())).append(",");

                // Partition
                line.append(message.getPartition() != null ? message.getPartition() : "").append(",");

                // Offset
                line.append(message.getOffset() != null ? message.getOffset() : "").append(",");

                // Key
                line.append(formatCsvValue(message.getKey())).append(",");

                // Value
                line.append(formatCsvValue(message.getValueAsString())).append(",");

                // Headers
                line.append(formatCsvValue(formatHeaders(message.getHeaders())));

                writer.write(line.toString());
                writer.newLine();

                count++;
                if (progressCallback != null && count % 100 == 0) {
                    progressCallback.accept(count);
                }
            }

            if (progressCallback != null) {
                progressCallback.accept(count);
            }
        }

        log.info("Export complete: {} messages", messages.size());
    }

    private Map<String, Object> messageToMap(StreamMessage message) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", message.getId());
        map.put("timestamp", formatTimestamp(message.getTimestamp()));
        map.put("topic", message.getTopic());
        map.put("partition", message.getPartition());
        map.put("offset", message.getOffset());
        map.put("key", message.getKey());
        map.put("value", message.getValueAsString());

        if (message.getHeaders() != null && !message.getHeaders().isEmpty()) {
            Map<String, String> headersMap = new LinkedHashMap<>();
            for (MessageHeader header : message.getHeaders()) {
                headersMap.put(header.getKey(), header.getValue());
            }
            map.put("headers", headersMap);
        }

        map.put("starred", message.isStarred());
        map.put("flagged", message.isFlagged());

        return map;
    }

    private String formatTimestamp(Instant instant) {
        if (instant == null) return "";
        return TIMESTAMP_FORMAT.format(instant);
    }

    private String formatHeaders(List<MessageHeader> headers) {
        if (headers == null || headers.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < headers.size(); i++) {
            if (i > 0) sb.append("; ");
            MessageHeader header = headers.get(i);
            sb.append(header.getKey()).append("=").append(header.getValue());
        }
        return sb.toString();
    }

    private String formatCsvValue(String value) {
        if (value == null) return "";

        // Escape quotes and wrap in quotes if contains special characters
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    /**
     * Export format enumeration.
     */
    public enum ExportFormat {
        JSON("JSON", ".json"),
        NDJSON("NDJSON", ".ndjson"),
        CSV("CSV", ".csv");

        private final String displayName;
        private final String extension;

        ExportFormat(String displayName, String extension) {
            this.displayName = displayName;
            this.extension = extension;
        }

        public String getDisplayName() {
            return displayName;
        }

        public String getExtension() {
            return extension;
        }

        @Override
        public String toString() {
            return displayName;
        }
    }
}
