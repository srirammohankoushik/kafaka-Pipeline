package com.pipeline.generator;

import java.io.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates random CSV records matching the pipeline schema.
 * Uses ThreadLocalRandom for speed and 4MB write buffer.
 */
public class CsvGenerator {
    private static final String[] CONTINENTS = {
            "North America", "Asia", "South America", "Europe", "Africa", "Australia"
    };
    private static final String ALPHA =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String ADDR_CHARS =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";

    public static void generate(File outFile, long count) throws IOException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        try (BufferedWriter w = new BufferedWriter(new FileWriter(outFile), 4 * 1024 * 1024)) {
            StringBuilder sb = new StringBuilder(128);
            long start = System.currentTimeMillis();
            for (long i = 0; i < count; i++) {
                sb.setLength(0);
                sb.append(rnd.nextInt(Integer.MAX_VALUE)).append(',');
                appendRandom(sb, rnd, ALPHA, 10 + rnd.nextInt(6));
                sb.append(',');
                sb.append((char) ('0' + rnd.nextInt(10))).append(' ');
                appendRandom(sb, rnd, ADDR_CHARS, 13 + rnd.nextInt(6));
                sb.append(',');
                sb.append(CONTINENTS[rnd.nextInt(CONTINENTS.length)]);
                w.write(sb.toString());
                w.newLine();
                if ((i + 1) % 5_000_000 == 0) {
                    System.out.printf("  generated %,d / %,d (%.1fs)%n",
                            i + 1, count, (System.currentTimeMillis() - start) / 1000.0);
                }
            }
            w.flush();
            System.out.printf("  CSV done: %,d records in %.2fs%n",
                    count, (System.currentTimeMillis() - start) / 1000.0);
        }
    }

    private static void appendRandom(StringBuilder sb, ThreadLocalRandom rnd, String cs, int len) {
        for (int i = 0; i < len; i++) sb.append(cs.charAt(rnd.nextInt(cs.length())));
    }
}
