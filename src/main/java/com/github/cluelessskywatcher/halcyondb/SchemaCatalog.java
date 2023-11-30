package com.github.cluelessskywatcher.halcyondb;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.github.cluelessskywatcher.halcyondb.data.DataType;
import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;
import com.github.cluelessskywatcher.halcyondb.storage.file.DatabaseFile;
import com.github.cluelessskywatcher.halcyondb.storage.file.QuickFile;

public class SchemaCatalog {
    private Map<String, DatabaseFile> namesToFiles;

    private Map<Integer, String> idsToPrimaryKeys;

    private Map<Integer, String> idsToNames;
    private Map<String, Integer> namesToIds;

    public SchemaCatalog() {
        this.namesToFiles = new HashMap<>();
        this.idsToPrimaryKeys = new HashMap<>();
        this.idsToNames = new HashMap<>();
        this.namesToIds = new HashMap<>();
    }

    public int getTableId(String name) {
        if (!namesToIds.containsKey(name)) {
            throw new NoSuchElementException(String.format("Key not found: %s", name));
        }
        return namesToIds.get(name);
    }

    public void addTable(DatabaseFile file, String name, String primaryKey) {
        int tableId = file.getId();

        namesToFiles.put(name, file);
        idsToNames.put(tableId, name);
        namesToIds.put(name, tableId);
        idsToPrimaryKeys.put(tableId, primaryKey);
    }

    public void addTable(DatabaseFile file, String name) {
        addTable(file, name, "");
    }

    public String getTableName(int tableId) {
        if (!idsToNames.containsKey(tableId)) {
            throw new NoSuchElementException(String.format("Key not found: %d", tableId));
        }
        return idsToNames.get(tableId);
    }

    public String getPrimaryKey(int tableId) {
        if (!idsToPrimaryKeys.containsKey(tableId)) {
            throw new NoSuchElementException(String.format("Key not found: %d", tableId));
        }
        return idsToPrimaryKeys.get(tableId);
    }

    public DatabaseFile getDatabaseFile(String name) {
        return namesToFiles.get(name);
    }

    public TupleMetadata getTupleMetadata(String name) {
        if (!namesToFiles.containsKey(name)) {
            throw new NoSuchElementException(String.format("Metadata does not exist for %s", name));
        }
        return namesToFiles.get(name).getTupleMetadata();
    }

    public TupleMetadata getTupleMetadataFromId(int id) {
        if (!idsToNames.containsKey(id)) {
            throw new NoSuchElementException(String.format("Metadata does not exist for %d", id));
        }
        return getTupleMetadata(idsToNames.get(id));
    }

    public void reset() {
        namesToFiles.clear();
        idsToNames.clear();
        namesToIds.clear();
        idsToPrimaryKeys.clear();
    }

    /*
     * Reads a catalog from file. A catalog file will have rows of the form
     * "<Object Type>,<Object Name>,<Object Field Count>,<Field List>"
     */
    public void loadFromFile(String fileName) {
        try {
            Reader in = new FileReader(String.format("src/files/%s", fileName));
            String[] headers = { "schemaType", "tableName", "primaryKey", "fields" };
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setSkipHeaderRecord(false)
                    .setHeader(headers)
                    .build();

            Iterable<CSVRecord> records = csvFormat.parse(in);

            for (CSVRecord record : records) {
                try {
                    processCatalogRow(record);
                }
                catch (NoSuchElementException e) {
                    continue;
                }                
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processCatalogRow(CSVRecord row) {
        String tableName = row.get("tableName");
        String primaryKey = row.get("primaryKey");
        String[] fields = row.get("fields").split(",");

        DataType[] types = new DataType[fields.length];
        String[] fieldNames = new String[fields.length];

        for (int i = 0; i < fields.length; i++) {
            String[] field = fields[i].trim().split(" ");
            fieldNames[i] = field[0];
            if (field[1].equals("integer")) {
                types[i] = DataType.INTEGER;
            } else if (field[1].equals("string")) {
                types[i] = DataType.STRING;
            }
        }

        if (!primaryKey.equals("") && !Set.copyOf(Arrays.asList(fieldNames)).contains(primaryKey)) {
            throw new NoSuchElementException(String.format("Primary key %s should be one of the fields", primaryKey));
        }

        TupleMetadata metadata = new TupleMetadata(types, fieldNames);
        DatabaseFile newFile = new QuickFile(metadata);
        addTable(newFile, tableName, primaryKey);
    }

    public boolean hasTable(String name) {
        return namesToIds.containsKey(name) && namesToFiles.containsKey(name);
    }
}
