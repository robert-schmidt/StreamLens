package com.streamlens.service.kafka;

import com.streamlens.model.connection.KafkaConnectionConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Service for Kafka administrative operations.
 * Provides consumer group management, topic metadata, and offset operations.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);
    private static final int TIMEOUT_SECONDS = 30;

    /**
     * Lists all topics with their partition counts.
     */
    public Map<String, Integer> listTopicsWithPartitions(KafkaConnectionConfig config)
            throws ExecutionException, InterruptedException, TimeoutException {

        Map<String, Integer> result = new LinkedHashMap<>();

        try (AdminClient adminClient = createAdminClient(config)) {
            Set<String> topicNames = adminClient.listTopics()
                    .names().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (!topicNames.isEmpty()) {
                DescribeTopicsResult descResult = adminClient.describeTopics(topicNames);
                Map<String, TopicDescription> descriptions = descResult.allTopicNames()
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

                for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                    result.put(entry.getKey(), entry.getValue().partitions().size());
                }
            }
        }

        return result;
    }

    /**
     * Creates a new topic.
     */
    public void createTopic(KafkaConnectionConfig config, String topicName, int partitions, short replicationFactor)
            throws ExecutionException, InterruptedException, TimeoutException {

        try (AdminClient adminClient = createAdminClient(config)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            adminClient.createTopics(Collections.singletonList(newTopic))
                    .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Created topic: {} with {} partitions", topicName, partitions);
        }
    }

    /**
     * Deletes a topic.
     */
    public void deleteTopic(KafkaConnectionConfig config, String topicName)
            throws ExecutionException, InterruptedException, TimeoutException {

        try (AdminClient adminClient = createAdminClient(config)) {
            adminClient.deleteTopics(Collections.singletonList(topicName))
                    .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Deleted topic: {}", topicName);
        }
    }

    /**
     * Adds partitions to an existing topic.
     */
    public void addPartitions(KafkaConnectionConfig config, String topicName, int newTotalPartitions)
            throws ExecutionException, InterruptedException, TimeoutException {

        try (AdminClient adminClient = createAdminClient(config)) {
            Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
            newPartitionsMap.put(topicName, NewPartitions.increaseTo(newTotalPartitions));

            adminClient.createPartitions(newPartitionsMap)
                    .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Increased partitions for topic {} to {}", topicName, newTotalPartitions);
        }
    }

    /**
     * Gets detailed information about a topic.
     */
    public TopicInfo getTopicInfo(KafkaConnectionConfig config, String topic)
            throws ExecutionException, InterruptedException, TimeoutException {

        try (AdminClient adminClient = createAdminClient(config)) {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topic));
            TopicDescription desc = result.topicNameValues().get(topic)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            TopicInfo info = new TopicInfo();
            info.setName(desc.name());
            info.setPartitionCount(desc.partitions().size());
            info.setInternal(desc.isInternal());

            // Get partition details
            List<PartitionInfo> partitions = new ArrayList<>();
            for (var partition : desc.partitions()) {
                PartitionInfo pInfo = new PartitionInfo();
                pInfo.setPartition(partition.partition());
                pInfo.setLeader(partition.leader() != null ? partition.leader().id() : -1);
                pInfo.setReplicas(partition.replicas().stream().map(n -> n.id()).collect(Collectors.toList()));
                pInfo.setIsr(partition.isr().stream().map(n -> n.id()).collect(Collectors.toList()));
                partitions.add(pInfo);
            }
            info.setPartitions(partitions);

            // Get topic configuration
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            DescribeConfigsResult configResult = adminClient.describeConfigs(Collections.singletonList(configResource));
            Config topicConfig = configResult.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS).get(configResource);

            Map<String, String> configMap = new LinkedHashMap<>();
            for (ConfigEntry entry : topicConfig.entries()) {
                if (!entry.isDefault()) {
                    configMap.put(entry.name(), entry.value());
                }
            }
            info.setConfig(configMap);

            return info;
        }
    }

    /**
     * Gets the beginning and end offsets for a topic's partitions.
     */
    public Map<Integer, OffsetRange> getTopicOffsets(KafkaConnectionConfig config, String topic)
            throws ExecutionException, InterruptedException, TimeoutException {

        Map<Integer, OffsetRange> offsets = new LinkedHashMap<>();

        try (KafkaConsumer<String, byte[]> consumer = createConsumer(config)) {
            List<org.apache.kafka.common.PartitionInfo> partitions = consumer.partitionsFor(topic);

            List<TopicPartition> tps = partitions.stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toList());

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(tps);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);

            for (TopicPartition tp : tps) {
                OffsetRange range = new OffsetRange();
                range.setBeginning(beginningOffsets.getOrDefault(tp, 0L));
                range.setEnd(endOffsets.getOrDefault(tp, 0L));
                offsets.put(tp.partition(), range);
            }
        }

        return offsets;
    }

    /**
     * Lists all consumer groups with details.
     */
    public List<ConsumerGroupInfo> listConsumerGroups(KafkaConnectionConfig config)
            throws ExecutionException, InterruptedException, TimeoutException {

        List<ConsumerGroupInfo> groups = new ArrayList<>();

        try (AdminClient adminClient = createAdminClient(config)) {
            Collection<ConsumerGroupListing> listings = adminClient.listConsumerGroups()
                    .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (!listings.isEmpty()) {
                List<String> groupIds = listings.stream()
                        .map(ConsumerGroupListing::groupId)
                        .collect(Collectors.toList());

                // Get detailed descriptions
                Map<String, ConsumerGroupDescription> descriptions = adminClient
                        .describeConsumerGroups(groupIds)
                        .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

                for (ConsumerGroupListing listing : listings) {
                    ConsumerGroupInfo info = new ConsumerGroupInfo();
                    info.setGroupId(listing.groupId());
                    info.setSimple(listing.isSimpleConsumerGroup());

                    ConsumerGroupDescription desc = descriptions.get(listing.groupId());
                    if (desc != null) {
                        info.setState(desc.state().toString());
                        info.setMemberCount(desc.members().size());
                    }

                    groups.add(info);
                }
            }
        }

        return groups;
    }

    /**
     * Gets the committed offsets for a consumer group on a topic.
     */
    public Map<Integer, Long> getConsumerGroupOffsets(KafkaConnectionConfig config, String groupId, String topic)
            throws ExecutionException, InterruptedException, TimeoutException {

        Map<Integer, Long> offsets = new LinkedHashMap<>();

        try (AdminClient adminClient = createAdminClient(config)) {
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> groupOffsets = result.partitionsToOffsetAndMetadata()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : groupOffsets.entrySet()) {
                if (entry.getKey().topic().equals(topic)) {
                    offsets.put(entry.getKey().partition(), entry.getValue().offset());
                }
            }
        }

        return offsets;
    }

    /**
     * Gets consumer group lag for a topic.
     */
    public Map<Integer, LagInfo> getConsumerGroupLag(KafkaConnectionConfig config, String groupId, String topic)
            throws ExecutionException, InterruptedException, TimeoutException {

        Map<Integer, LagInfo> lagInfoMap = new LinkedHashMap<>();

        Map<Integer, Long> committedOffsets = getConsumerGroupOffsets(config, groupId, topic);
        Map<Integer, OffsetRange> topicOffsets = getTopicOffsets(config, topic);

        for (Map.Entry<Integer, OffsetRange> entry : topicOffsets.entrySet()) {
            int partition = entry.getKey();
            OffsetRange range = entry.getValue();

            LagInfo lag = new LagInfo();
            lag.setPartition(partition);
            lag.setEarliestOffset(range.getBeginning());
            lag.setLatestOffset(range.getEnd());
            lag.setCommittedOffset(committedOffsets.getOrDefault(partition, -1L));

            if (lag.getCommittedOffset() >= 0) {
                lag.setLag(range.getEnd() - lag.getCommittedOffset());
            } else {
                lag.setLag(-1); // Unknown
            }

            lagInfoMap.put(partition, lag);
        }

        return lagInfoMap;
    }

    /**
     * Resets consumer group offsets to the beginning.
     */
    public void resetOffsetsToEarliest(KafkaConnectionConfig config, String groupId, String topic)
            throws ExecutionException, InterruptedException, TimeoutException {

        try (AdminClient adminClient = createAdminClient(config);
             KafkaConsumer<String, byte[]> consumer = createConsumer(config)) {

            List<TopicPartition> tps = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toList());

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(tps);

            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
                offsetsToCommit.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
            }

            adminClient.alterConsumerGroupOffsets(groupId, offsetsToCommit)
                    .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.info("Reset offsets for group {} on topic {} to earliest", groupId, topic);
        }
    }

    /**
     * Resets consumer group offsets to the end.
     */
    public void resetOffsetsToLatest(KafkaConnectionConfig config, String groupId, String topic)
            throws ExecutionException, InterruptedException, TimeoutException {

        try (AdminClient adminClient = createAdminClient(config);
             KafkaConsumer<String, byte[]> consumer = createConsumer(config)) {

            List<TopicPartition> tps = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toList());

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);

            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
                offsetsToCommit.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
            }

            adminClient.alterConsumerGroupOffsets(groupId, offsetsToCommit)
                    .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.info("Reset offsets for group {} on topic {} to latest", groupId, topic);
        }
    }

    /**
     * Resets consumer group offsets to a specific offset.
     */
    public void resetOffsetsToSpecific(KafkaConnectionConfig config, String groupId, String topic,
                                       int partition, long offset)
            throws ExecutionException, InterruptedException, TimeoutException {

        try (AdminClient adminClient = createAdminClient(config)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            offsetsToCommit.put(tp, new OffsetAndMetadata(offset));

            adminClient.alterConsumerGroupOffsets(groupId, offsetsToCommit)
                    .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            log.info("Reset offset for group {} on {}:{} to {}", groupId, topic, partition, offset);
        }
    }

    /**
     * Resets consumer group offsets to a timestamp.
     */
    public void resetOffsetsToTimestamp(KafkaConnectionConfig config, String groupId, String topic, Instant timestamp)
            throws ExecutionException, InterruptedException, TimeoutException {

        try (AdminClient adminClient = createAdminClient(config);
             KafkaConsumer<String, byte[]> consumer = createConsumer(config)) {

            List<TopicPartition> tps = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toList());

            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (TopicPartition tp : tps) {
                timestampsToSearch.put(tp, timestamp.toEpochMilli());
            }

            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);

            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
                if (entry.getValue() != null) {
                    offsetsToCommit.put(entry.getKey(), new OffsetAndMetadata(entry.getValue().offset()));
                }
            }

            if (!offsetsToCommit.isEmpty()) {
                adminClient.alterConsumerGroupOffsets(groupId, offsetsToCommit)
                        .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                log.info("Reset offsets for group {} on topic {} to timestamp {}", groupId, topic, timestamp);
            }
        }
    }

    /**
     * Deletes a consumer group.
     */
    public void deleteConsumerGroup(KafkaConnectionConfig config, String groupId)
            throws ExecutionException, InterruptedException, TimeoutException {

        try (AdminClient adminClient = createAdminClient(config)) {
            adminClient.deleteConsumerGroups(Collections.singletonList(groupId))
                    .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Deleted consumer group: {}", groupId);
        }
    }

    /**
     * Gets the offset for a specific timestamp.
     */
    public Map<Integer, Long> getOffsetsForTimestamp(KafkaConnectionConfig config, String topic, Instant timestamp) {
        Map<Integer, Long> result = new LinkedHashMap<>();

        try (KafkaConsumer<String, byte[]> consumer = createConsumer(config)) {
            List<TopicPartition> tps = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toList());

            Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            for (TopicPartition tp : tps) {
                timestampsToSearch.put(tp, timestamp.toEpochMilli());
            }

            Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);

            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
                if (entry.getValue() != null) {
                    result.put(entry.getKey().partition(), entry.getValue().offset());
                }
            }
        }

        return result;
    }

    private AdminClient createAdminClient(KafkaConnectionConfig config) {
        Map<String, Object> props = config.toKafkaProperties();
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_SECONDS * 1000);
        return AdminClient.create(props);
    }

    private KafkaConsumer<String, byte[]> createConsumer(KafkaConnectionConfig config) {
        Map<String, Object> props = config.toKafkaProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "streamlens-admin-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    // ============== Data Classes ==============

    public static class TopicInfo {
        private String name;
        private int partitionCount;
        private boolean internal;
        private List<PartitionInfo> partitions;
        private Map<String, String> config;

        // Getters and setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getPartitionCount() { return partitionCount; }
        public void setPartitionCount(int partitionCount) { this.partitionCount = partitionCount; }
        public boolean isInternal() { return internal; }
        public void setInternal(boolean internal) { this.internal = internal; }
        public List<PartitionInfo> getPartitions() { return partitions; }
        public void setPartitions(List<PartitionInfo> partitions) { this.partitions = partitions; }
        public Map<String, String> getConfig() { return config; }
        public void setConfig(Map<String, String> config) { this.config = config; }
    }

    public static class PartitionInfo {
        private int partition;
        private int leader;
        private List<Integer> replicas;
        private List<Integer> isr;

        public int getPartition() { return partition; }
        public void setPartition(int partition) { this.partition = partition; }
        public int getLeader() { return leader; }
        public void setLeader(int leader) { this.leader = leader; }
        public List<Integer> getReplicas() { return replicas; }
        public void setReplicas(List<Integer> replicas) { this.replicas = replicas; }
        public List<Integer> getIsr() { return isr; }
        public void setIsr(List<Integer> isr) { this.isr = isr; }
    }

    public static class OffsetRange {
        private long beginning;
        private long end;

        public long getBeginning() { return beginning; }
        public void setBeginning(long beginning) { this.beginning = beginning; }
        public long getEnd() { return end; }
        public void setEnd(long end) { this.end = end; }
        public long getMessageCount() { return end - beginning; }
    }

    public static class ConsumerGroupInfo {
        private String groupId;
        private boolean simple;
        private String state;
        private int memberCount;

        public String getGroupId() { return groupId; }
        public void setGroupId(String groupId) { this.groupId = groupId; }
        public boolean isSimple() { return simple; }
        public void setSimple(boolean simple) { this.simple = simple; }
        public String getState() { return state; }
        public void setState(String state) { this.state = state; }
        public int getMemberCount() { return memberCount; }
        public void setMemberCount(int memberCount) { this.memberCount = memberCount; }
    }

    public static class LagInfo {
        private int partition;
        private long earliestOffset;
        private long latestOffset;
        private long committedOffset;
        private long lag;

        public int getPartition() { return partition; }
        public void setPartition(int partition) { this.partition = partition; }
        public long getEarliestOffset() { return earliestOffset; }
        public void setEarliestOffset(long earliestOffset) { this.earliestOffset = earliestOffset; }
        public long getLatestOffset() { return latestOffset; }
        public void setLatestOffset(long latestOffset) { this.latestOffset = latestOffset; }
        public long getCommittedOffset() { return committedOffset; }
        public void setCommittedOffset(long committedOffset) { this.committedOffset = committedOffset; }
        public long getLag() { return lag; }
        public void setLag(long lag) { this.lag = lag; }
    }
}
