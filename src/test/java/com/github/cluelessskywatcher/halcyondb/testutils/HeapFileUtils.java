package com.github.cluelessskywatcher.halcyondb.testutils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.github.cluelessskywatcher.halcyondb.data.DataType;
import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;
import com.github.cluelessskywatcher.halcyondb.storage.DatabaseBufferPool;
import com.github.cluelessskywatcher.halcyondb.storage.file.HeapFile;
import com.github.cluelessskywatcher.halcyondb.storage.file.HeapFileGenerator;

public class HeapFileUtils {
    public static HeapFile generateHeapFile(int rows, int columns, String fileName) throws IOException {
        Random random = new Random();
        Object[][] tuples = new Object[rows][columns];
        for (int i = 0; i < rows; i++){
            for (int j = 0; j < columns; j++) {
                tuples[i][j] = random.nextInt(10000);
            }
        }
        TupleMetadata metadata = TupleMetadataUtils.createSampleTupleMetadata(rows, DataType.INTEGER);
        return HeapFileGenerator.generateHeapFileFromList(tuples, fileName, DatabaseBufferPool.PAGE_SIZE, rows, metadata);
    }

    public static HeapFile generateHeapFileFromList(Object[][] rows, String fileName) throws IOException {
        TupleMetadata metadata = TupleMetadataUtils.createSampleTupleMetadata(rows[0].length, DataType.INTEGER);

        return HeapFileGenerator.generateHeapFileFromList(rows, fileName, DatabaseBufferPool.PAGE_SIZE, rows[0].length, metadata);
    }
}
