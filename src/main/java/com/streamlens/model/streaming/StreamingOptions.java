package com.streamlens.model.streaming;

import java.time.Instant;

/**
 * Options for starting a streaming session.
 * Configures offset position, consumer group, and other streaming behaviors.
 */
public class StreamingOptions {

    private StartPosition startPosition = StartPosition.LATEST;
    private Long specificOffset;
    private Instant specificTimestamp;
    private String consumerGroup;
    private boolean commitOffsets = false;
    private int maxMessages = 0; // 0 = unlimited

    public StreamingOptions() {}

    public static StreamingOptions latest() {
        return new StreamingOptions();
    }

    public static StreamingOptions earliest() {
        StreamingOptions opts = new StreamingOptions();
        opts.setStartPosition(StartPosition.EARLIEST);
        return opts;
    }

    public static StreamingOptions fromOffset(long offset) {
        StreamingOptions opts = new StreamingOptions();
        opts.setStartPosition(StartPosition.FROM_OFFSET);
        opts.setSpecificOffset(offset);
        return opts;
    }

    public static StreamingOptions fromTimestamp(Instant timestamp) {
        StreamingOptions opts = new StreamingOptions();
        opts.setStartPosition(StartPosition.FROM_TIMESTAMP);
        opts.setSpecificTimestamp(timestamp);
        return opts;
    }

    // Getters and Setters

    public StartPosition getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(StartPosition startPosition) {
        this.startPosition = startPosition;
    }

    public Long getSpecificOffset() {
        return specificOffset;
    }

    public void setSpecificOffset(Long specificOffset) {
        this.specificOffset = specificOffset;
    }

    public Instant getSpecificTimestamp() {
        return specificTimestamp;
    }

    public void setSpecificTimestamp(Instant specificTimestamp) {
        this.specificTimestamp = specificTimestamp;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public boolean isCommitOffsets() {
        return commitOffsets;
    }

    public void setCommitOffsets(boolean commitOffsets) {
        this.commitOffsets = commitOffsets;
    }

    public int getMaxMessages() {
        return maxMessages;
    }

    public void setMaxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
    }

    /**
     * Start position options for streaming.
     */
    public enum StartPosition {
        /**
         * Start from the latest offset (new messages only).
         */
        LATEST("Latest (new messages only)"),

        /**
         * Start from the earliest offset (beginning of topic).
         */
        EARLIEST("Earliest (from beginning)"),

        /**
         * Start from a specific offset.
         */
        FROM_OFFSET("From specific offset"),

        /**
         * Start from a specific timestamp.
         */
        FROM_TIMESTAMP("From timestamp");

        private final String displayName;

        StartPosition(String displayName) {
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String toString() {
            return displayName;
        }
    }
}
