package com.pipeline;

import com.pipeline.generator.CsvGenerator;
import com.pipeline.kafka.KafkaConsumerHelper;
import com.pipeline.kafka.KafkaProducerHelper;
import com.pipeline.model.Record;
import com.pipeline.sorter.FileSorter;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * Kafka Pipeline v2 — Full end-to-end data pipeline:
 *
 *   Step 1: Generate 50M random CSV records → data.csv
 *   Step 2: Produce all records to Kafka 'source' topic
 *   Step 3: Consume from Kafka 'source' topic → consumed.csv
 *   Step 4: Sort consumed data by id/name/continent → publish to 3 output topics
 *
 * Key design decisions:
 *   - Reads from Kafka 'source' (as required) — consumed once, sorted 3 ways
 *   - Continent uses bucket sort (O(N), 6 distinct values)
 *   - id/name use external merge sort (O(N log N), 250K chunks, tiered merge)
 *   - id + name sorts run in PARALLEL to minimize wall-clock time
 *   - Tiered merge (max 50-way) prevents OOM from too many open file handles
 *
 * Resource constraints: 2GB memory, 4 cores.
 * Target: ~10-15 minutes for 50M records.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // ── Config
        long count = 50_000_000;
        String bootstrap = "localhost:29092";
        String dataDir = "data";

        // Simple arg parsing
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--count" -> count = Long.parseLong(args[i + 1]);
                case "--bootstrap" -> bootstrap = args[i + 1];
                case "--dataDir" -> dataDir = args[i + 1];
            }
        }

        final String finalDataDir = dataDir;
        final Properties finalSortProps;

        File dir = new File(dataDir);
        if (!dir.exists()) dir.mkdirs();
        File csvFile = new File(dir, "data.csv");

        // Kafka producer properties (high-throughput tuning)
        Properties prodProps = new Properties();
        prodProps.put("bootstrap.servers", bootstrap);
        prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("acks", "1");
        prodProps.put("compression.type", "lz4");
        prodProps.put("linger.ms", "100");
        prodProps.put("batch.size", "262144");
        prodProps.put("buffer.memory", "67108864");
        prodProps.put("max.in.flight.requests.per.connection", "5");
        prodProps.put("request.timeout.ms", "60000");
        prodProps.put("delivery.timeout.ms", "120000");

        // Sorter producer: tuned for 3 parallel producers within memory budget
        Properties sortProdProps = new Properties();
        sortProdProps.putAll(prodProps);
        sortProdProps.put("buffer.memory", "20971520");  // 20MB each (3 × 20 = 60MB total)
        sortProdProps.put("batch.size", "524288");        // 512KB batches 
        sortProdProps.put("linger.ms", "200");            //fill batches
        finalSortProps = sortProdProps;

        long wallStart = System.currentTimeMillis();
        long genMs = 0, prodMs = 0, consumeMs = 0, sortMs = 0;

        // ── Step 1: Generate CSV 
        System.out.printf("%n=== Step 1: Generate %,d records ===%n", count);
        long t0 = System.currentTimeMillis();
        CsvGenerator.generate(csvFile, count);
        genMs = System.currentTimeMillis() - t0;

        // ── Step 2: Produce to Kafka 'source' topic 
        System.out.printf("%n=== Step 2: Produce to Kafka 'source' ===%n");
        t0 = System.currentTimeMillis();
        KafkaProducerHelper.recreateTopic(bootstrap, "source", 3);
        KafkaProducerHelper.produceFile(csvFile, "source", prodProps);
        prodMs = System.currentTimeMillis() - t0;

        // ── Step 3: Consume from Kafka 'source' topic 
        System.out.printf("%n=== Step 3: Consume from Kafka 'source' ===%n");
        t0 = System.currentTimeMillis();

        File consumedFile = new File(dir, "consumed.csv");
        final String fb = bootstrap;
        long consumed = KafkaConsumerHelper.consumeToFile(
                fb, "source", "pipeline-sorter-group", consumedFile);

        consumeMs = System.currentTimeMillis() - t0;
        System.out.printf("  consumed %,d records from Kafka%n", consumed);

        // ── Step 4: Sort consumed data & publish to output topics 
        System.out.printf("%n=== Step 4: Sort (continent first, then id+name parallel) ===%n");
        t0 = System.currentTimeMillis();

        // Create output topics
        for (String topic : new String[]{"id", "name", "continent"}) {
            KafkaProducerHelper.recreateTopic(bootstrap, topic, 1);
        }

        // Phase A: Continent sort FIRST (bucket sort, reads file alone — no disk contention)
        long cCont = new FileSorter("continent", Comparator.comparing((Record r) -> r.continent),
                cloneProps(finalSortProps), finalDataDir).sort(consumedFile);

        // Phase B: ID + Name sorts in parallel (2 readers — manageable contention)
        ExecutorService exec = Executors.newFixedThreadPool(2);

        Future<Long> fId = exec.submit(() ->
                new FileSorter("id", Comparator.comparingInt(r -> r.id),
                        cloneProps(finalSortProps), finalDataDir).sort(consumedFile));

        Future<Long> fName = exec.submit(() ->
                new FileSorter("name", Comparator.comparing(r -> r.name),
                        cloneProps(finalSortProps), finalDataDir).sort(consumedFile));

        long cId = fId.get();
        long cName = fName.get();
        exec.shutdown();

        sortMs = System.currentTimeMillis() - t0;

        // Clean up consumed file
        consumedFile.delete();

        // ── Summary 
        long totalMs = System.currentTimeMillis() - wallStart;
        System.out.printf("%n===========================================%n");
        System.out.printf(" Pipeline Summary%n");
        System.out.printf("===========================================%n");
        System.out.printf("  Records:    %,d%n", count);
        System.out.printf("  Consumed:   %,d%n", consumed);
        System.out.printf("  Sorted:     id=%,d  name=%,d  continent=%,d%n", cId, cName, cCont);
        System.out.printf("  -----------------------------------------%n");
        System.out.printf("  Step 1 - CSV generation:   %8.2fs%n", genMs / 1000.0);
        System.out.printf("  Step 2 - Kafka produce:    %8.2fs%n", prodMs / 1000.0);
        System.out.printf("  Step 3 - Kafka consume:    %8.2fs%n", consumeMs / 1000.0);
        System.out.printf("  Step 4 - Sort & publish:   %8.2fs%n", sortMs / 1000.0);
        System.out.printf("  -----------------------------------------%n");
        System.out.printf("  TOTAL wall-clock:          %8.2fs%n", totalMs / 1000.0);
        System.out.printf("===========================================%n");
    }

    private static Properties cloneProps(Properties base) {
        Properties p = new Properties();
        p.putAll(base);
        return p;
    }
}
