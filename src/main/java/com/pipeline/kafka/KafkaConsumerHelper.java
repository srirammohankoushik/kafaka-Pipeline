package com.pipeline.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * KafkaConsumerHelper: Consumes all records from a Kafka topic and writes them
 * to a local CSV file for downstream sorting.
 *
 * Uses consumer group protocol with auto-commit so offsets are visible
 * in Offset Explorer for monitoring.
 *
 * Optimizations:
 *   - Large max.poll.records (10000) to reduce poll overhead
 *   - 1MB fetch sizes to maximize network throughput per request
 *   - 256KB write buffer to minimize disk I/O syscalls
 *   - Auto-commit enabled for offset tracking in Offset Explorer
 */
public class KafkaConsumerHelper {

    private static final int WRITE_BUF = 256 * 1024;
    private static final int PROGRESS = 5_000_000;

    /**
     * Consume all records from the given topic and write them to outFile.
     * Uses the specified consumer group ID for offset tracking.
     *
     * @param bootstrap Kafka bootstrap servers
     * @param topic     Topic to consume from
     * @param groupId   Consumer group ID (e.g., "pipeline-sorter-group")
     * @param outFile   Output CSV file
     * @return total records consumed
     */
    public static long consumeToFile(String bootstrap, String topic, String groupId, File outFile) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576");       // 1MB min fetch
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "10485760");      // 10MB max fetch
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "5242880"); // 5MB per partition
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");      // 60s session timeout
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "20000");   // 20s heartbeat
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");   // 10 min max poll interval

        AtomicLong total = new AtomicLong(0);
        long startMs = System.currentTimeMillis();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             BufferedWriter writer = new BufferedWriter(new FileWriter(outFile), WRITE_BUF)) {

            consumer.subscribe(Collections.singletonList(topic));

            Map<TopicPartition, Long> endOffsets = null;
            List<TopicPartition> partitions = null;
            int emptyPolls = 0;
            boolean assigned = false;

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));

                if (!assigned && !consumer.assignment().isEmpty()) {
                    assigned = true;
                    partitions = new ArrayList<>(consumer.assignment());
                    endOffsets = consumer.endOffsets(partitions);
                    long totalExpected = endOffsets.values().stream().mapToLong(Long::longValue).sum();
                    System.out.printf("  [%s] consuming from '%s': %d partitions, ~%,d records expected%n",
                            groupId, topic, partitions.size(), totalExpected);
                }

                if (records.isEmpty()) {
                    emptyPolls++;
                    if (assigned && (isFullyConsumed(consumer, endOffsets) || emptyPolls >= 5)) {
                        break;
                    }
                    continue;
                }
                emptyPolls = 0;

                for (ConsumerRecord<String, String> rec : records) {
                    String value = rec.value();
                    if (value != null && !value.isEmpty()) {
                        writer.write(value);
                        writer.newLine();
                        long n = total.incrementAndGet();
                        if (n % PROGRESS == 0) {
                            long expected = (endOffsets != null)
                                    ? endOffsets.values().stream().mapToLong(Long::longValue).sum() : 0;
                            System.out.printf("  [%s] consumed %,d / ~%,d (%.1fs)%n",
                                    groupId, n, expected,
                                    (System.currentTimeMillis() - startMs) / 1000.0);
                        }
                    }
                }
            }

            writer.flush();
        }

        System.out.printf("  [%s] consume done: %,d records in %.2fs%n",
                groupId, total.get(), (System.currentTimeMillis() - startMs) / 1000.0);
        return total.get();
    }

    private static boolean isFullyConsumed(KafkaConsumer<String, String> consumer,
                                           Map<TopicPartition, Long> endOffsets) {
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            long position = consumer.position(entry.getKey());
            if (position < entry.getValue()) {
                return false;
            }
        }
        return true;
    }
}
