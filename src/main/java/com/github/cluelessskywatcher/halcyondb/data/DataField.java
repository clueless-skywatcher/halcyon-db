package com.github.cluelessskywatcher.halcyondb.data;

public interface DataField {
    boolean compare(QueryPredicate.Op op, DataField other);

    DataType getType();
}
