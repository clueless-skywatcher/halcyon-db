package com.github.cluelessskywatcher.halcyondb.storage;

import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;

public interface DatabaseFile {
    public int getId();

    public TupleMetadata getTupleMetadata();
}
