package com.pipeline.sorter;

import com.pipeline.model.Record;
import org.apache.kafka.clients.producer.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FileSorter: Reads a CSV file, sorts by a given key, produces sorted output to Kafka.
 *
 * Algorithm selection (automatic via cardinality probe):
 *   - Bucket sort for low-cardinality keys (continent: 6 distinct values) → O(N)
 *   - External merge sort for high-cardinality keys (id, name) → O(N log N)
 *     with tiered k-way merge (max 100-way) to avoid opening too many files.
 */
public class FileSorter {
    private static final int CHUNK_SIZE = 500_000;           // 500K records per chunk (balanced for 768MB heap)
    private static final int CARDINALITY_THRESHOLD = 10;
    private static final int PROGRESS = 5_000_000;
    private static final int WRITE_BUF = 512 * 1024;        // 512KB write buffer
    private static final int READ_BUF = 2 * 1024 * 1024;    // 2MB read buffer
    private static final int MERGE_READ_BUF = 64 * 1024;    // 64KB per merge reader
    private static final int MAX_FAN_IN = 100;               // 50M/500K = 100 chunks — single merge pass

    private final String key;
    private final Comparator<Record> cmp;
    private final Properties producerProps;
    private final String outputTopic;
    private final File tmpDir;

    public FileSorter(String key, Comparator<Record> cmp,
                      Properties producerProps, String dataDir) throws IOException {
        this.key = key;
        this.cmp = cmp;
        this.producerProps = producerProps;
        this.outputTopic = key;
        File base = new File(dataDir, "tmp");
        this.tmpDir = new File(base, "sort-" + key + "-" + System.nanoTime());
        if (!tmpDir.mkdirs()) throw new IOException("Cannot create: " + tmpDir);
    }

    /**
     *entry: read CSV file, sort, produce to Kafka output topic.
     * Returns number of records produced.
     */
    public long sort(File csvFile) throws IOException {
        long t0 = System.currentTimeMillis();
        System.out.printf("[%s] starting sort from %s%n", key, csvFile.getName());

        try {
            // Cardinality probe: read first CHUNK_SIZE records
            List<Record> probe = new ArrayList<>(CHUNK_SIZE);
            Set<String> distinct = new HashSet<>();
            try (BufferedReader r = new BufferedReader(new FileReader(csvFile), READ_BUF)) {
                String line;
                while ((line = r.readLine()) != null && probe.size() < CHUNK_SIZE) {
                    Record rec = Record.fromCsv(line);
                    if (rec != null) { probe.add(rec); distinct.add(keyOf(rec)); }
                }
            }

            boolean lowCard = distinct.size() < CARDINALITY_THRESHOLD;
            System.out.printf("[%s] probe: %d distinct in %d -> %s%n",
                    key, distinct.size(), probe.size(), lowCard ? "BUCKET" : "MERGE");

            long result = lowCard
                    ? bucketSort(csvFile, probe)
                    : mergeSort(csvFile, probe);

            System.out.printf("[%s] done: %,d records in %.2fs%n%n",
                    key, result, (System.currentTimeMillis() - t0) / 1000.0);
            return result;
        } finally {
            deleteDir(tmpDir);
        }
    }

    // ─── Bucket sort (continent─) 

    private long bucketSort(File csvFile, List<Record> probe) throws IOException {
        long t1 = System.currentTimeMillis();
        Map<String, PrintWriter> buckets = new HashMap<>();
        long count = 0;

        for (Record rec : probe) {
            getBucket(buckets, keyOf(rec)).println(rec.toCsv());
            count++;
        }

        try (BufferedReader r = new BufferedReader(new FileReader(csvFile), READ_BUF)) {
            String line;
            long skip = probe.size();
            while ((line = r.readLine()) != null) {
                if (skip > 0) { skip--; continue; }
                Record rec = Record.fromCsv(line);
                if (rec != null) {
                    getBucket(buckets, keyOf(rec)).println(rec.toCsv());
                    count++;
                    if (count % PROGRESS == 0)
                        System.out.printf("  [%s] bucketed %,d%n", key, count);
                }
            }
        }
        buckets.values().forEach(PrintWriter::close);
        System.out.printf("[%s] buckets: %,d records -> %d buckets in %.1fs%n",
                key, count, buckets.size(), (System.currentTimeMillis() - t1) / 1000.0);

        List<String> sortedKeys = new ArrayList<>(buckets.keySet());
        Collections.sort(sortedKeys);
        return streamToKafka(sortedKeys);
    }

    private long streamToKafka(List<String> sortedKeys) throws IOException {
        long t2 = System.currentTimeMillis();
        AtomicLong sent = new AtomicLong(0);
        AtomicLong fail = new AtomicLong(0);

        try (KafkaProducer<String, String> prod = new KafkaProducer<>(producerProps)) {
            for (String bk : sortedKeys) {
                File f = new File(tmpDir, "bucket_" + bk.replace(" ", "_") + ".csv");
                try (BufferedReader br = new BufferedReader(new FileReader(f), WRITE_BUF)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        prod.send(new ProducerRecord<>(outputTopic, line), (m, ex) -> {
                            if (ex != null) fail.incrementAndGet();
                            else { long n = sent.incrementAndGet(); if (n % PROGRESS == 0) System.out.printf("  [%s] produced %,d%n", key, n); }
                        });
                    }
                }
            }
            prod.flush();
        }
        System.out.printf("[%s] stream: %,d sent in %.1fs%n",
                key, sent.get(), (System.currentTimeMillis() - t2) / 1000.0);
        return sent.get();
    }

    // ─── External merge sort (id, name) 

    private long mergeSort(File csvFile, List<Record> probe) throws IOException {
        long t1 = System.currentTimeMillis();
        List<File> chunks = new ArrayList<>();
        List<Record> buf = new ArrayList<>(probe);
        long total = 0;

        if (buf.size() >= CHUNK_SIZE) {
            chunks.add(writeChunk(buf, chunks.size()));
            total += buf.size();
            buf = new ArrayList<>(CHUNK_SIZE);
        }

        try (BufferedReader r = new BufferedReader(new FileReader(csvFile), READ_BUF)) {
            String line;
            long skip = probe.size();
            while ((line = r.readLine()) != null) {
                if (skip > 0) { skip--; continue; }
                Record rec = Record.fromCsv(line);
                if (rec != null) {
                    buf.add(rec);
                    if (buf.size() >= CHUNK_SIZE) {
                        chunks.add(writeChunk(buf, chunks.size()));
                        total += buf.size();
                        buf = new ArrayList<>(CHUNK_SIZE);
                        if (total % PROGRESS < CHUNK_SIZE)
                            System.out.printf("  [%s] chunked %,d records, %d chunks%n", key, total, chunks.size());
                    }
                }
            }
        }
		if (!buf.isEmpty()) {
			chunks.add(writeChunk(buf, chunks.size()));
			total += buf.size();
		}

        System.out.printf("[%s] chunks: %,d records -> %d chunks in %.1fs%n",
                key, total, chunks.size(), (System.currentTimeMillis() - t1) / 1000.0);

        long t2 = System.currentTimeMillis();
        List<File> current = new ArrayList<>(chunks);
        int round = 0;

        while (current.size() > MAX_FAN_IN) {
            round++;
            List<File> next = new ArrayList<>();
            System.out.printf("[%s] merge round %d: %d chunks (fan-in %d)%n",
                    key, round, current.size(), MAX_FAN_IN);
            for (int i = 0; i < current.size(); i += MAX_FAN_IN) {
                int end = Math.min(i + MAX_FAN_IN, current.size());
                List<File> batch = current.subList(i, end);
                next.add(mergeToFile(batch, round, next.size()));
                for (File f : batch) f.delete();
            }
            current = next;
        }

        System.out.printf("[%s] final merge: %d chunks -> Kafka '%s'%n",
                key, current.size(), outputTopic);
        long merged = mergeToKafka(current);
        System.out.printf("[%s] merge+produce: %,d records in %.1fs%n",
                key, merged, (System.currentTimeMillis() - t2) / 1000.0);
        for (File f : current) f.delete();
        return merged;
    }

    private File writeChunk(List<Record> buf, int idx) throws IOException {
        Record[] arr = buf.toArray(new Record[0]);
        Arrays.parallelSort(arr, cmp);
        File f = new File(tmpDir, String.format("chunk-%05d.tmp", idx));
        try (PrintWriter w = new PrintWriter(new BufferedWriter(new FileWriter(f), WRITE_BUF))) {
            for (Record r : arr) w.println(r.toCsv());
        }
        return f;
    }

    private File mergeToFile(List<File> chunks, int round, int idx) throws IOException {
        PriorityQueue<ChunkReader> pq = new PriorityQueue<>(
                (a, b) -> cmp.compare(a.current, b.current));
        List<ChunkReader> readers = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            ChunkReader cr = new ChunkReader(chunks.get(i));
            if (cr.current != null) { pq.offer(cr); readers.add(cr); }
        }

        File out = new File(tmpDir, String.format("merge-r%d-%05d.tmp", round, idx));
        long n = 0;
        try (PrintWriter w = new PrintWriter(new BufferedWriter(new FileWriter(out), WRITE_BUF))) {
            while (!pq.isEmpty()) {
                ChunkReader cr = pq.poll();
                w.println(cr.current.toCsv());
                n++;
                if (cr.advance()) pq.offer(cr);
            }
        }
        for (ChunkReader cr : readers) cr.close();
        System.out.printf("  [%s] merged %d -> %s (%,d records)%n",
                key, chunks.size(), out.getName(), n);
        return out;
    }

    private long mergeToKafka(List<File> chunks) throws IOException {
        PriorityQueue<ChunkReader> pq = new PriorityQueue<>(
                (a, b) -> cmp.compare(a.current, b.current));
        List<ChunkReader> readers = new ArrayList<>();
        for (int i = 0; i < chunks.size(); i++) {
            ChunkReader cr = new ChunkReader(chunks.get(i));
            if (cr.current != null) { pq.offer(cr); readers.add(cr); }
        }

        AtomicLong sent = new AtomicLong(0);
        AtomicLong fail = new AtomicLong(0);

        try (KafkaProducer<String, String> prod = new KafkaProducer<>(producerProps)) {
            while (!pq.isEmpty()) {
                ChunkReader cr = pq.poll();
                prod.send(new ProducerRecord<>(outputTopic, cr.current.toCsv()), (m, ex) -> {
                    if (ex != null) fail.incrementAndGet();
                    else { long n = sent.incrementAndGet(); if (n % PROGRESS == 0) System.out.printf("  [%s] produced %,d%n", key, n); }
                });
                if (cr.advance()) pq.offer(cr);
            }
            prod.flush();
        }
        for (ChunkReader cr : readers) cr.close();
        if (fail.get() > 0) System.err.printf("[%s] WARNING: %d failures%n", key, fail.get());
        return sent.get();
    }

    // ─── Helpers 

    private String keyOf(Record rec) {
        return switch (key) {
            case "id" -> String.valueOf(rec.id);
            case "name" -> rec.name;
            case "continent" -> rec.continent;
            default -> "";
        };
    }

    private PrintWriter getBucket(Map<String, PrintWriter> map, String k) {
        return map.computeIfAbsent(k, x -> {
            try {
                return new PrintWriter(new BufferedWriter(
                        new FileWriter(new File(tmpDir, "bucket_" + x.replace(" ", "_") + ".csv")), 64 * 1024));
            } catch (IOException e) { throw new RuntimeException(e); }
        });
    }

    private void deleteDir(File dir) {
        if (!dir.exists()) return;
        File[] files = dir.listFiles();
		if (files != null)
			for (File f : files) {
				if (f.isDirectory())
					deleteDir(f);
				else
					f.delete();
			}
        dir.delete();
    }

    private class ChunkReader implements AutoCloseable {
        private final BufferedReader reader;
        Record current;
        ChunkReader(File f) throws IOException {
            reader = new BufferedReader(new FileReader(f), MERGE_READ_BUF);
            advance();
        }
        boolean advance() throws IOException {
            String line = reader.readLine();
            current = (line != null) ? Record.fromCsv(line) : null;
            return current != null;
        }
        @Override public void close() throws IOException { reader.close(); }
    }
}
