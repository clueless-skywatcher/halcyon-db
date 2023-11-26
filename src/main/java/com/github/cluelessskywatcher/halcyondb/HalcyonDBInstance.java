package com.github.cluelessskywatcher.halcyondb;

import com.github.cluelessskywatcher.halcyondb.storage.DatabaseBufferPool;

public class HalcyonDBInstance {
    private static HalcyonDBInstance instance = new HalcyonDBInstance();
    private SchemaCatalog catalog;
    private DatabaseBufferPool bufferPool;

    private HalcyonDBInstance() {
        this.catalog = new SchemaCatalog();
        this.bufferPool = new DatabaseBufferPool();
    }

    public static SchemaCatalog getCatalog() {
        return instance.catalog;
    }

    public static DatabaseBufferPool getBufferPool() {
        return instance.bufferPool;
    }

    public static void reset() {
        instance.catalog = new SchemaCatalog();
        instance.bufferPool = new DatabaseBufferPool();
    }
}
