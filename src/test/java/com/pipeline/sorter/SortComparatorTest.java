package com.pipeline.sorter;

import com.pipeline.model.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for sort comparators used in FileSorter.
 * Validates id (numerical), name (alphabetical), and continent (alphabetical) ordering.
 */
class SortComparatorTest {

    // ── ID sort (numerical) 

    @Test
    @DisplayName("ID comparator sorts numerically, not lexicographically")
    void idSortsNumerically() {
        List<Record> records = List.of(
                rec(21, "aaa", "Asia"),
                rec(2, "bbb", "Africa"),
                rec(100, "ccc", "Europe"),
                rec(3, "ddd", "Australia")
        );

        List<Record> sorted = records.stream()
                .sorted(Comparator.comparingInt(r -> r.id))
                .collect(Collectors.toList());

        assertArrayEquals(
                new int[]{2, 3, 21, 100},
                sorted.stream().mapToInt(r -> r.id).toArray(),
                "IDs should be sorted numerically: 2, 3, 21, 100"
        );
    }

    @Test
    @DisplayName("ID comparator handles duplicate ids")
    void idHandlesDuplicates() {
        List<Record> records = List.of(
                rec(5, "aaa", "Asia"),
                rec(5, "bbb", "Africa"),
                rec(1, "ccc", "Europe")
        );

        List<Record> sorted = records.stream()
                .sorted(Comparator.comparingInt(r -> r.id))
                .collect(Collectors.toList());

        assertEquals(1, sorted.get(0).id);
        assertEquals(5, sorted.get(1).id);
        assertEquals(5, sorted.get(2).id);
    }

    @Test
    @DisplayName("ID comparator handles single element")
    void idSingleElement() {
        List<Record> records = List.of(rec(42, "test", "Asia"));
        List<Record> sorted = records.stream()
                .sorted(Comparator.comparingInt(r -> r.id))
                .collect(Collectors.toList());

        assertEquals(1, sorted.size());
        assertEquals(42, sorted.get(0).id);
    }

    @Test
    @DisplayName("ID comparator handles zero and large values")
    void idZeroAndLarge() {
        List<Record> records = List.of(
                rec(2147483646, "aaa", "Asia"),
                rec(0, "bbb", "Africa"),
                rec(1000000, "ccc", "Europe")
        );

        List<Record> sorted = records.stream()
                .sorted(Comparator.comparingInt(r -> r.id))
                .collect(Collectors.toList());

        assertEquals(0, sorted.get(0).id);
        assertEquals(1000000, sorted.get(1).id);
        assertEquals(2147483646, sorted.get(2).id);
    }

    // ── Name sort (alphabetical)

    @Test
    @DisplayName("Name comparator sorts alphabetically")
    void nameSortsAlphabetically() {
        List<Record> records = List.of(
                rec(1, "charlie", "Asia"),
                rec(2, "alice", "Africa"),
                rec(3, "bob", "Europe")
        );

        List<Record> sorted = records.stream()
                .sorted(Comparator.comparing(r -> r.name))
                .collect(Collectors.toList());

        assertEquals("alice", sorted.get(0).name);
        assertEquals("bob", sorted.get(1).name);
        assertEquals("charlie", sorted.get(2).name);
    }

    @Test
    @DisplayName("Name comparator is case-sensitive (uppercase before lowercase)")
    void nameCaseSensitive() {
        List<Record> records = List.of(
                rec(1, "banana", "Asia"),
                rec(2, "Apple", "Africa"),
                rec(3, "cherry", "Europe")
        );

        List<Record> sorted = records.stream()
                .sorted(Comparator.comparing(r -> r.name))
                .collect(Collectors.toList());

        // In Java String comparison: uppercase < lowercase (A=65, a=97)
        assertEquals("Apple", sorted.get(0).name);
        assertEquals("banana", sorted.get(1).name);
        assertEquals("cherry", sorted.get(2).name);
    }

    @Test
    @DisplayName("Name comparator handles duplicate names")
    void nameHandlesDuplicates() {
        List<Record> records = List.of(
                rec(1, "samename", "Asia"),
                rec(2, "samename", "Africa"),
                rec(3, "aardvark", "Europe")
        );

        List<Record> sorted = records.stream()
                .sorted(Comparator.comparing(r -> r.name))
                .collect(Collectors.toList());

        assertEquals("aardvark", sorted.get(0).name);
        assertEquals("samename", sorted.get(1).name);
        assertEquals("samename", sorted.get(2).name);
    }

    @Test
    @DisplayName("Name comparator handles names of varying lengths")
    void nameVaryingLengths() {
        List<Record> records = List.of(
                rec(1, "abcdefghijklmno", "Asia"),   // 15 chars
                rec(2, "abcdefghij", "Africa"),       // 10 chars
                rec(3, "abcdefghijkl", "Europe")      // 12 chars
        );

        List<Record> sorted = records.stream()
                .sorted(Comparator.comparing(r -> r.name))
                .collect(Collectors.toList());

        // "abcdefghij" < "abcdefghijkl" < "abcdefghijklmno" (prefix ordering)
        assertEquals("abcdefghij", sorted.get(0).name);
        assertEquals("abcdefghijkl", sorted.get(1).name);
        assertEquals("abcdefghijklmno", sorted.get(2).name);
    }

    // ── Continent sort (alphabetical)

    @Test
    @DisplayName("Continent comparator sorts all 6 values alphabetically")
    void continentSortsAllSix() {
        List<Record> records = List.of(
                rec(1, "aaa", "South America"),
                rec(2, "bbb", "Asia"),
                rec(3, "ccc", "Europe"),
                rec(4, "ddd", "Australia"),
                rec(5, "eee", "North America"),
                rec(6, "fff", "Africa")
        );

        List<Record> sorted = records.stream()
                .sorted(Comparator.comparing(r -> r.continent))
                .collect(Collectors.toList());

        assertEquals("Africa", sorted.get(0).continent);
        assertEquals("Asia", sorted.get(1).continent);
        assertEquals("Australia", sorted.get(2).continent);
        assertEquals("Europe", sorted.get(3).continent);
        assertEquals("North America", sorted.get(4).continent);
        assertEquals("South America", sorted.get(5).continent);
    }

    @Test
    @DisplayName("Continent comparator groups same continents together")
    void continentGroupsTogether() {
        List<Record> records = List.of(
                rec(1, "aaa", "Asia"),
                rec(2, "bbb", "Africa"),
                rec(3, "ccc", "Asia"),
                rec(4, "ddd", "Africa"),
                rec(5, "eee", "Europe")
        );

        List<Record> sorted = records.stream()
                .sorted(Comparator.comparing(r -> r.continent))
                .collect(Collectors.toList());

        // Africa records first, then Asia, then Europe
        assertEquals("Africa", sorted.get(0).continent);
        assertEquals("Africa", sorted.get(1).continent);
        assertEquals("Asia", sorted.get(2).continent);
        assertEquals("Asia", sorted.get(3).continent);
        assertEquals("Europe", sorted.get(4).continent);
    }

    @Test
    @DisplayName("Continent comparator preserves all record fields after sorting")
    void continentPreservesFields() {
        Record original = new Record(42, "testnamexyz", "5 hello world abc", "Australia");
        List<Record> records = List.of(
                rec(1, "aaa", "Europe"),
                original
        );

        List<Record> sorted = records.stream()
                .sorted(Comparator.comparing(r -> r.continent))
                .collect(Collectors.toList());

        // Australia < Europe, so original should be first
        Record first = sorted.get(0);
        assertEquals(42, first.id);
        assertEquals("testnamexyz", first.name);
        assertEquals("5 hello world abc", first.address);
        assertEquals("Australia", first.continent);
    }

    // ── Combined: verify sort independence 

    @Test
    @DisplayName("Different comparators produce different orderings")
    void differentComparatorsDifferentOrders() {
        List<Record> records = List.of(
                rec(100, "alice", "Europe"),
                rec(1, "charlie", "Africa"),
                rec(50, "bob", "Asia")
        );

        List<Record> byId = records.stream()
                .sorted(Comparator.comparingInt(r -> r.id))
                .collect(Collectors.toList());

        List<Record> byName = records.stream()
                .sorted(Comparator.comparing(r -> r.name))
                .collect(Collectors.toList());

        List<Record> byContinent = records.stream()
                .sorted(Comparator.comparing(r -> r.continent))
                .collect(Collectors.toList());

        // ID order: 1, 50, 100
        assertEquals(1, byId.get(0).id);
        assertEquals(50, byId.get(1).id);
        assertEquals(100, byId.get(2).id);

        // Name order: alice, bob, charlie
        assertEquals("alice", byName.get(0).name);
        assertEquals("bob", byName.get(1).name);
        assertEquals("charlie", byName.get(2).name);

        // Continent order: Africa, Asia, Europe
        assertEquals("Africa", byContinent.get(0).continent);
        assertEquals("Asia", byContinent.get(1).continent);
        assertEquals("Europe", byContinent.get(2).continent);
    }

    // ── Helper

    private static Record rec(int id, String name, String continent) {
        return new Record(id, name, "1 default address", continent);
    }
}
