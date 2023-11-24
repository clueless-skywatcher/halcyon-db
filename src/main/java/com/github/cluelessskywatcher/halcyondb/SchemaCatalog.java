package com.github.cluelessskywatcher.halcyondb;

import java.util.HashMap;
import java.util.Map;

import com.github.cluelessskywatcher.halcyondb.storage.DatabaseFile;

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
        return namesToIds.get(name);
    }

    public String getTableName(int tableId) {
        return idsToNames.get(tableId);
    }

    public String getPrimaryKey(int tableId) {
        return idsToPrimaryKeys.get(tableId);
    }

    public DatabaseFile getDatabaseFile(String name) {
        return namesToFiles.get(name);
    }
}
