package com.pipeline.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Record CSV parsing and serialization.
 */
class RecordTest {

    // ── fromCsv() — valid input 

    @Test
    @DisplayName("Parse valid CSV line with all 4 fields")
    void parsesValidCsvLine() {
        Record r = Record.fromCsv("21,axxxxxxxxx,12 abc dfsf LdUE,Asia");
        assertNotNull(r);
        assertEquals(21, r.id);
        assertEquals("axxxxxxxxx", r.name);
        assertEquals("12 abc dfsf LdUE", r.address);
        assertEquals("Asia", r.continent);
    }

    @Test
    @DisplayName("Parse CSV with large id near int32 max")
    void parsesLargeId() {
        Record r = Record.fromCsv("2147483646,testname1234,5 abc def ghijk,Europe");
        assertNotNull(r);
        assertEquals(2147483646, r.id);
    }

    @Test
    @DisplayName("Parse CSV with zero id")
    void parsesZeroId() {
        Record r = Record.fromCsv("0,abcdefghij,1 abcdefghijklmn,Africa");
        assertNotNull(r);
        assertEquals(0, r.id);
    }

    @Test
    @DisplayName("Parse CSV with continent containing space")
    void parsesContinentWithSpace() {
        Record r = Record.fromCsv("42,nameabcdefg,9 addr space test,North America");
        assertNotNull(r);
        assertEquals("North America", r.continent);
    }

    @Test
    @DisplayName("Parse CSV with all 6 continent values")
    void parsesAllContinents() {
        String[] continents = {"North America", "Asia", "South America", "Europe", "Africa", "Australia"};
        for (String c : continents) {
            Record r = Record.fromCsv("1,abcdefghij,1 abcdefghijklmn," + c);
            assertNotNull(r, "Failed to parse continent: " + c);
            assertEquals(c, r.continent);
        }
    }

    // ── fromCsv() — invalid input

    @Test
    @DisplayName("Return null for null input")
    void returnsNullForNull() {
        assertNull(Record.fromCsv(null));
    }

    @Test
    @DisplayName("Return null for empty string")
    void returnsNullForEmpty() {
        assertNull(Record.fromCsv(""));
    }

    @Test
    @DisplayName("Return null for missing commas")
    void returnsNullForMissingCommas() {
        assertNull(Record.fromCsv("no commas here"));
    }

    @Test
    @DisplayName("Return null for only 2 commas (3 fields instead of 4)")
    void returnsNullForTooFewFields() {
        assertNull(Record.fromCsv("1,name,address"));
    }

    @Test
    @DisplayName("Return null for non-numeric id")
    void returnsNullForBadId() {
        assertNull(Record.fromCsv("abc,name,address,Asia"));
    }

    // ── toCsv() — serialization 

    @Test
    @DisplayName("Serialize record to CSV format")
    void serializesToCsv() {
        Record r = new Record(21, "axxxxxxxxx", "12 abc dfsf LdUE", "Asia");
        assertEquals("21,axxxxxxxxx,12 abc dfsf LdUE,Asia", r.toCsv());
    }

    @Test
    @DisplayName("Round-trip: fromCsv -> toCsv produces identical output")
    void roundTrip() {
        String original = "99,testnamexyz,7 hello world abcd,South America";
        Record r = Record.fromCsv(original);
        assertNotNull(r);
        assertEquals(original, r.toCsv());
    }

    @Test
    @DisplayName("Round-trip with zero id")
    void roundTripZeroId() {
        String original = "0,abcdefghij,1 abcdefghijklmn,Africa";
        assertEquals(original, Record.fromCsv(original).toCsv());
    }

    @Test
    @DisplayName("Round-trip with large id")
    void roundTripLargeId() {
        String original = "2147483646,nameabcdefg,9 addr space test,Europe";
        assertEquals(original, Record.fromCsv(original).toCsv());
    }
}
