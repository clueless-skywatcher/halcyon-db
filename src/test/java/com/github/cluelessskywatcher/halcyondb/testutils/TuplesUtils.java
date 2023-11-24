package com.github.cluelessskywatcher.halcyondb.testutils;

import com.github.cluelessskywatcher.halcyondb.data.DataType;
import com.github.cluelessskywatcher.halcyondb.data.IntegerField;
import com.github.cluelessskywatcher.halcyondb.data.Tuple;
import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;

public class TuplesUtils {
    public static Tuple createSampleTuple(int numFields) {
        TupleMetadata metadata = TupleMetadataUtils.createSampleTupleMetadata(numFields, DataType.INTEGER);
        Tuple newTuple = new Tuple(metadata);
        for (int i = 0; i < metadata.getNumFields(); i++) {
            newTuple.setFieldAt(i, new IntegerField(5));
        }
        return newTuple;
    }
}
