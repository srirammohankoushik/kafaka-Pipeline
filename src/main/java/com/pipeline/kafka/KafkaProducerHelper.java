package com.pipeline.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka helper: topic management + CSV-to-Kafka producer with 4 worker threads.
 */
public class KafkaProducerHelper {

    // ── Topic management ──────────────────────────────────────────────

    public static void recreateTopic(String bootstrap, String topic, int partitions) throws Exception {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");

        for (int attempt = 1; attempt <= 10; attempt++) {
            try (AdminClient admin = AdminClient.create(p)) {
                try {
                    admin.deleteTopics(Collections.singleton(topic)).all().get();
                    Thread.sleep(1000);
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) throw e;
                }
                try {
                    admin.createTopics(Collections.singleton(
                            new NewTopic(topic, partitions, (short) 1))).all().get();
                    System.out.printf("  topic '%s' ready (%d partitions)%n", topic, partitions);
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) throw e;
                }
                return;
            } catch (Exception e) {
                if (attempt == 10) throw e;
                System.out.printf("  Kafka not ready (attempt %d/10), retrying...%n", attempt);
                Thread.sleep(3000);
            }
        }
    }

    // ── CSV → Kafka producer ──────────────────────────────────────────

    public static long produceFile(File csvFile, String topic, Properties producerProps) throws Exception {
        AtomicLong sent = new AtomicLong(0);
        AtomicLong failed = new AtomicLong(0);
        BlockingQueue<String> q = new ArrayBlockingQueue<>(50_000);
        String POISON = "__END__";

        ExecutorService workers = Executors.newFixedThreadPool(4);
        for (int w = 0; w < 4; w++) {
            workers.submit(() -> {
                try (KafkaProducer<String, String> prod = new KafkaProducer<>(producerProps)) {
                    int batch = 0;
                    while (true) {
                        String line = q.take();
                        if (line.equals(POISON)) { q.put(POISON); break; }
                        prod.send(new ProducerRecord<>(topic, line), (m, ex) -> {
                            if (ex != null) failed.incrementAndGet();
                            else {
                                long n = sent.incrementAndGet();
                                if (n % 5_000_000 == 0) System.out.printf("  sent %,d messages%n", n);
                            }
                        });
                        if (++batch >= 50_000) { prod.flush(); batch = 0; }
                    }
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
        }

        try (BufferedReader r = new BufferedReader(new FileReader(csvFile), 1024 * 1024)) {
            String line;
            while ((line = r.readLine()) != null) q.put(line.trim());
        }
        q.put(POISON);
        workers.shutdown();
        workers.awaitTermination(1, TimeUnit.HOURS);

        System.out.printf("  produce done: %,d sent, %d failed%n", sent.get(), failed.get());
        if (failed.get() > 0) throw new RuntimeException("Produce failures: " + failed.get());
        return sent.get();
    }
}
