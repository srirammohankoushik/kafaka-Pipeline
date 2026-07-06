package com.pipeline.generator;

import com.pipeline.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CsvGenerator — validates schema constraints on generated data.
 */
class CsvGeneratorTest {

    private static final Set<String> VALID_CONTINENTS = Set.of(
            "North America", "Asia", "South America", "Europe", "Africa", "Australia"
    );

    @TempDir
    Path tempDir;

    // ── Schema validation ───────────────────────────────────────────

    @Test
    @DisplayName("Generates exact number of requested records")
    void generatesCorrectCount() throws IOException {
        File out = tempDir.resolve("test.csv").toFile();
        CsvGenerator.generate(out, 100);

        long lines = countLines(out);
        assertEquals(100, lines);
    }

    @Test
    @DisplayName("Each record has exactly 4 comma-separated fields")
    void eachRecordHasFourFields() throws IOException {
        File out = tempDir.resolve("test.csv").toFile();
        CsvGenerator.generate(out, 50);

        try (BufferedReader r = new BufferedReader(new FileReader(out))) {
            String line;
            int lineNum = 0;
            while ((line = r.readLine()) != null) {
                lineNum++;
                Record rec = Record.fromCsv(line);
                assertNotNull(rec, "Failed to parse line " + lineNum + ": " + line);
            }
        }
    }

    @Test
    @DisplayName("ID is within valid int32 range (0 to Integer.MAX_VALUE-1)")
    void idWithinRange() throws IOException {
        File out = tempDir.resolve("test.csv").toFile();
        CsvGenerator.generate(out, 200);

        try (BufferedReader r = new BufferedReader(new FileReader(out))) {
            String line;
            while ((line = r.readLine()) != null) {
                Record rec = Record.fromCsv(line);
                assertNotNull(rec);
                assertTrue(rec.id >= 0, "ID should be non-negative: " + rec.id);
                assertTrue(rec.id < Integer.MAX_VALUE, "ID should be < MAX_VALUE: " + rec.id);
            }
        }
    }

    @Test
    @DisplayName("Name length is between 10 and 15 characters")
    void nameLength() throws IOException {
        File out = tempDir.resolve("test.csv").toFile();
        CsvGenerator.generate(out, 200);

        try (BufferedReader r = new BufferedReader(new FileReader(out))) {
            String line;
            while ((line = r.readLine()) != null) {
                Record rec = Record.fromCsv(line);
                assertNotNull(rec);
                int len = rec.name.length();
                assertTrue(len >= 10 && len <= 15,
                        "Name length should be 10-15, got " + len + ": " + rec.name);
            }
        }
    }

    @Test
    @DisplayName("Name contains only English alphabetic characters")
    void nameOnlyAlpha() throws IOException {
        File out = tempDir.resolve("test.csv").toFile();
        CsvGenerator.generate(out, 200);

        try (BufferedReader r = new BufferedReader(new FileReader(out))) {
            String line;
            while ((line = r.readLine()) != null) {
                Record rec = Record.fromCsv(line);
                assertNotNull(rec);
                assertTrue(rec.name.matches("[a-zA-Z]+"),
                        "Name should be English chars only: " + rec.name);
            }
        }
    }

    @Test
    @DisplayName("Address length is between 15 and 20 characters")
    void addressLength() throws IOException {
        File out = tempDir.resolve("test.csv").toFile();
        CsvGenerator.generate(out, 200);

        try (BufferedReader r = new BufferedReader(new FileReader(out))) {
            String line;
            while ((line = r.readLine()) != null) {
                Record rec = Record.fromCsv(line);
                assertNotNull(rec);
                int len = rec.address.length();
                assertTrue(len >= 15 && len <= 20,
                        "Address length should be 15-20, got " + len + ": " + rec.address);
            }
        }
    }

    @Test
    @DisplayName("Address contains mix of letters, digits, and spaces")
    void addressContainsMix() throws IOException {
        File out = tempDir.resolve("test.csv").toFile();
        CsvGenerator.generate(out, 200);

        boolean hasDigit = false, hasAlpha = false, hasSpace = false;
        try (BufferedReader r = new BufferedReader(new FileReader(out))) {
            String line;
            while ((line = r.readLine()) != null) {
                Record rec = Record.fromCsv(line);
                assertNotNull(rec);
                for (char c : rec.address.toCharArray()) {
                    if (Character.isDigit(c)) hasDigit = true;
                    if (Character.isLetter(c)) hasAlpha = true;
                    if (c == ' ') hasSpace = true;
                }
                assertTrue(rec.address.matches("[a-zA-Z0-9 ]+"),
                        "Address should be alphanumeric+space: " + rec.address);
            }
        }
        assertTrue(hasDigit, "At least one address should contain a digit");
        assertTrue(hasAlpha, "At least one address should contain a letter");
        assertTrue(hasSpace, "At least one address should contain a space");
    }

    @Test
    @DisplayName("Continent is one of the 6 valid values")
    void continentIsValid() throws IOException {
        File out = tempDir.resolve("test.csv").toFile();
        CsvGenerator.generate(out, 200);

        try (BufferedReader r = new BufferedReader(new FileReader(out))) {
            String line;
            while ((line = r.readLine()) != null) {
                Record rec = Record.fromCsv(line);
                assertNotNull(rec);
                assertTrue(VALID_CONTINENTS.contains(rec.continent),
                        "Invalid continent: " + rec.continent);
            }
        }
    }

    @Test
    @DisplayName("All 6 continents appear in a large enough sample")
    void allContinentsRepresented() throws IOException {
        File out = tempDir.resolve("test.csv").toFile();
        CsvGenerator.generate(out, 1000);

        Set<String> seen = new HashSet<>();
        try (BufferedReader r = new BufferedReader(new FileReader(out))) {
            String line;
            while ((line = r.readLine()) != null) {
                Record rec = Record.fromCsv(line);
                if (rec != null) seen.add(rec.continent);
            }
        }
        assertEquals(VALID_CONTINENTS, seen, "All 6 continents should appear in 1000 records");
    }

    @Test
    @DisplayName("Zero records produces empty file")
    void zeroRecords() throws IOException {
        File out = tempDir.resolve("test.csv").toFile();
        CsvGenerator.generate(out, 0);
        assertEquals(0, countLines(out));
    }

    // ── Helper

    private long countLines(File f) throws IOException {
        long count = 0;
        try (BufferedReader r = new BufferedReader(new FileReader(f))) {
            while (r.readLine() != null) count++;
        }
        return count;
    }
}
