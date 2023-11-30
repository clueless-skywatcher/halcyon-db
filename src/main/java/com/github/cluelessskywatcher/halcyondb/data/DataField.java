package com.github.cluelessskywatcher.halcyondb.data;

import java.io.DataOutputStream;
import java.io.IOException;

public interface DataField {
    boolean compare(QueryPredicate.Op op, DataField other);

    DataType getType();

    void serialize(DataOutputStream dos) throws IOException;
}
