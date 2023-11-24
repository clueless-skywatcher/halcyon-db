package com.github.cluelessskywatcher.halcyondb.testutils;

import java.util.Arrays;

import com.github.cluelessskywatcher.halcyondb.data.DataType;
import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;

public class TupleMetadataUtils {
    public static TupleMetadata createSampleTupleMetadata(int numFields, DataType type) {
        DataType[] types = new DataType[numFields];
        String[] fieldNames = new String[numFields];

        Arrays.fill(types, type);
        
        for (int i = 0; i < numFields; i++) {
            fieldNames[i] = String.format("field%d", i);
        }

        return new TupleMetadata(types, fieldNames);
    }
}
