package com.streamlens.model.message;

/**
 * Represents a message header (Kafka header or Event Hubs property).
 */
public class MessageHeader {

    private String key;
    private String value;

    public MessageHeader() {
    }

    public MessageHeader(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }
}
