package com.pipeline.model;

/**
 * CSV record: id (int32) | name (string) | address (string) | continent (string)
 */
public class Record {
    public final int id;
    public final String name;
    public final String address;
    public final String continent;

    public Record(int id, String name, String address, String continent) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.continent = continent;
    }

    /** Fast CSV parse — manual indexOf, no regex. */
    public static Record fromCsv(String line) {
        if (line == null || line.isEmpty()) return null;
        try {
            int c1 = line.indexOf(',');
            if (c1 < 0) return null;
            int c2 = line.indexOf(',', c1 + 1);
            if (c2 < 0) return null;
            int c3 = line.indexOf(',', c2 + 1);
            if (c3 < 0) return null;
            return new Record(
                    Integer.parseInt(line, 0, c1, 10),
                    line.substring(c1 + 1, c2),
                    line.substring(c2 + 1, c3),
                    line.substring(c3 + 1));
        } catch (Exception e) {
            return null;
        }
    }

    /** Fast CSV serialize — StringBuilder, no String.format. */
    public String toCsv() {
        return new StringBuilder(64)
                .append(id).append(',')
                .append(name).append(',')
                .append(address).append(',')
                .append(continent)
                .toString();
    }
}
