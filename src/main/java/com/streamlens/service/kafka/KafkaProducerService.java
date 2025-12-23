package com.streamlens.service.kafka;

import com.streamlens.model.connection.KafkaConnectionConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service for producing messages to Kafka topics.
 * Supports single message and mass message production with variable substitution.
 */
@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    // Variable pattern: {{variableName}} or {{variableName:param1:param2}}
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{\\{([^}]+)\\}\\}");

    /**
     * Sends a single message to a topic.
     */
    public SendResult sendMessage(KafkaConnectionConfig config, String topic, Integer partition,
                                  String key, String value, Map<String, String> headers) {
        Map<String, Object> props = config.toKafkaProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : null;

            ProducerRecord<String, byte[]> record;
            if (partition != null) {
                record = new ProducerRecord<>(topic, partition, key, valueBytes);
            } else {
                record = new ProducerRecord<>(topic, key, valueBytes);
            }

            // Add headers
            if (headers != null) {
                for (Map.Entry<String, String> header : headers.entrySet()) {
                    record.headers().add(new RecordHeader(
                            header.getKey(),
                            header.getValue().getBytes(StandardCharsets.UTF_8)
                    ));
                }
            }

            RecordMetadata metadata = producer.send(record).get();
            log.info("Sent message to {}:{} offset {}", topic, metadata.partition(), metadata.offset());

            return new SendResult(true, metadata.partition(), metadata.offset(), null);
        } catch (Exception e) {
            log.error("Failed to send message to {}", topic, e);
            return new SendResult(false, -1, -1, e.getMessage());
        }
    }

    /**
     * Sends multiple messages with variable substitution.
     *
     * @param config         Kafka connection config
     * @param topic          Target topic
     * @param partition      Target partition (null for automatic)
     * @param keyTemplate    Key template with variables (can be null)
     * @param valueTemplate  Value template with variables
     * @param headers        Headers to add to each message
     * @param messageCount   Total number of messages to send
     * @param delayMs        Delay between messages in milliseconds (0 for no delay)
     * @param progressCallback Callback for progress updates
     * @return Result with success/failure counts
     */
    public CompletableFuture<MassProduceResult> sendMassMessages(
            KafkaConnectionConfig config,
            String topic,
            Integer partition,
            String keyTemplate,
            String valueTemplate,
            Map<String, String> headers,
            int messageCount,
            int delayMs,
            Consumer<MassProduceProgress> progressCallback) {

        return CompletableFuture.supplyAsync(() -> {
            Map<String, Object> props = config.toKafkaProperties();
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);
            AtomicLong startTime = new AtomicLong(System.currentTimeMillis());

            VariableContext varContext = new VariableContext();

            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
                for (int i = 0; i < messageCount; i++) {
                    varContext.setIndex(i);

                    String key = keyTemplate != null ? substituteVariables(keyTemplate, varContext) : null;
                    String value = substituteVariables(valueTemplate, varContext);
                    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

                    ProducerRecord<String, byte[]> record;
                    if (partition != null) {
                        record = new ProducerRecord<>(topic, partition, key, valueBytes);
                    } else {
                        record = new ProducerRecord<>(topic, key, valueBytes);
                    }

                    // Add headers
                    if (headers != null) {
                        for (Map.Entry<String, String> header : headers.entrySet()) {
                            String headerValue = substituteVariables(header.getValue(), varContext);
                            record.headers().add(new RecordHeader(
                                    header.getKey(),
                                    headerValue.getBytes(StandardCharsets.UTF_8)
                            ));
                        }
                    }

                    final int currentIndex = i;
                    producer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            successCount.incrementAndGet();
                        } else {
                            failCount.incrementAndGet();
                            log.warn("Failed to send message {}: {}", currentIndex, exception.getMessage());
                        }
                    });

                    // Progress callback
                    if (progressCallback != null && (i % 100 == 0 || i == messageCount - 1)) {
                        long elapsed = System.currentTimeMillis() - startTime.get();
                        double rate = elapsed > 0 ? (i + 1) * 1000.0 / elapsed : 0;
                        progressCallback.accept(new MassProduceProgress(
                                i + 1, messageCount, successCount.get(), failCount.get(), rate
                        ));
                    }

                    // Delay if specified
                    if (delayMs > 0 && i < messageCount - 1) {
                        Thread.sleep(delayMs);
                    }
                }

                // Flush remaining messages
                producer.flush();

                long totalTime = System.currentTimeMillis() - startTime.get();
                double avgRate = totalTime > 0 ? messageCount * 1000.0 / totalTime : 0;

                log.info("Mass produce completed: {} sent, {} failed, {:.1f} msg/s",
                        successCount.get(), failCount.get(), avgRate);

                return new MassProduceResult(successCount.get(), failCount.get(), totalTime, avgRate);

            } catch (Exception e) {
                log.error("Mass produce failed", e);
                return new MassProduceResult(successCount.get(), failCount.get(), 0, 0);
            }
        });
    }

    /**
     * Substitutes variables in a template string.
     *
     * Supported variables:
     * - {{index}} or {{i}} - Current message index (0-based)
     * - {{index1}} - Current message index (1-based)
     * - {{uuid}} - Random UUID
     * - {{timestamp}} - Current timestamp (ISO format)
     * - {{timestamp_ms}} - Current timestamp in milliseconds
     * - {{date}} - Current date (yyyy-MM-dd)
     * - {{datetime}} - Current datetime (yyyy-MM-dd HH:mm:ss)
     * - {{random}} - Random integer
     * - {{random:min:max}} - Random integer in range [min, max]
     * - {{seq:name}} - Named sequence (increments each call)
     * - {{seq:name:start}} - Named sequence starting at 'start'
     */
    public String substituteVariables(String template, VariableContext context) {
        if (template == null) return null;

        Matcher matcher = VARIABLE_PATTERN.matcher(template);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String variable = matcher.group(1);
            String replacement = evaluateVariable(variable, context);
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);

        return result.toString();
    }

    private String evaluateVariable(String variable, VariableContext context) {
        String[] parts = variable.split(":");
        String varName = parts[0].toLowerCase().trim();

        switch (varName) {
            case "index":
            case "i":
                return String.valueOf(context.getIndex());

            case "index1":
                return String.valueOf(context.getIndex() + 1);

            case "uuid":
                return UUID.randomUUID().toString();

            case "timestamp":
                return Instant.now().toString();

            case "timestamp_ms":
                return String.valueOf(System.currentTimeMillis());

            case "date":
                return LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE);

            case "datetime":
                return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            case "random":
                if (parts.length >= 3) {
                    int min = Integer.parseInt(parts[1].trim());
                    int max = Integer.parseInt(parts[2].trim());
                    return String.valueOf(context.getRandom().nextInt(max - min + 1) + min);
                }
                return String.valueOf(context.getRandom().nextInt(Integer.MAX_VALUE));

            case "seq":
                String seqName = parts.length > 1 ? parts[1].trim() : "default";
                int start = parts.length > 2 ? Integer.parseInt(parts[2].trim()) : 0;
                return String.valueOf(context.getSequence(seqName, start));

            case "string":
                // {{string:length}} - random alphanumeric string
                int length = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 10;
                return randomString(length, context.getRandom());

            case "choice":
                // {{choice:option1:option2:option3}} - random choice from options
                if (parts.length > 1) {
                    int idx = context.getRandom().nextInt(parts.length - 1) + 1;
                    return parts[idx].trim();
                }
                return "";

            default:
                return "{{" + variable + "}}"; // Unknown variable, keep as-is
        }
    }

    private String randomString(int length, Random random) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

    // ============== Data Classes ==============

    public static class VariableContext {
        private int index = 0;
        private final Random random = new Random();
        private final Map<String, AtomicInteger> sequences = new HashMap<>();

        public int getIndex() { return index; }
        public void setIndex(int index) { this.index = index; }
        public Random getRandom() { return random; }

        public int getSequence(String name, int start) {
            return sequences.computeIfAbsent(name, k -> new AtomicInteger(start - 1))
                    .incrementAndGet();
        }
    }

    public static class SendResult {
        private final boolean success;
        private final int partition;
        private final long offset;
        private final String error;

        public SendResult(boolean success, int partition, long offset, String error) {
            this.success = success;
            this.partition = partition;
            this.offset = offset;
            this.error = error;
        }

        public boolean isSuccess() { return success; }
        public int getPartition() { return partition; }
        public long getOffset() { return offset; }
        public String getError() { return error; }
    }

    public static class MassProduceProgress {
        private final int sent;
        private final int total;
        private final int success;
        private final int failed;
        private final double rate;

        public MassProduceProgress(int sent, int total, int success, int failed, double rate) {
            this.sent = sent;
            this.total = total;
            this.success = success;
            this.failed = failed;
            this.rate = rate;
        }

        public int getSent() { return sent; }
        public int getTotal() { return total; }
        public int getSuccess() { return success; }
        public int getFailed() { return failed; }
        public double getRate() { return rate; }
        public double getPercentage() { return total > 0 ? sent * 100.0 / total : 0; }
    }

    public static class MassProduceResult {
        private final int successCount;
        private final int failCount;
        private final long totalTimeMs;
        private final double averageRate;

        public MassProduceResult(int successCount, int failCount, long totalTimeMs, double averageRate) {
            this.successCount = successCount;
            this.failCount = failCount;
            this.totalTimeMs = totalTimeMs;
            this.averageRate = averageRate;
        }

        public int getSuccessCount() { return successCount; }
        public int getFailCount() { return failCount; }
        public long getTotalTimeMs() { return totalTimeMs; }
        public double getAverageRate() { return averageRate; }
        public int getTotalCount() { return successCount + failCount; }
    }
}
