package com.github.cluelessskywatcher.halcyondb.storage;

import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;

public class QuickFile implements DatabaseFile {
    private int id;
    private TupleMetadata metadata;

    public QuickFile(int id, TupleMetadata metadata) {
        this.id = id;
        this.metadata = metadata;
    }

    public QuickFile(TupleMetadata metadata) {
        this.id = (int) System.currentTimeMillis();
        this.metadata = metadata;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public TupleMetadata getTupleMetadata() {
        return metadata;
    }
    
}
