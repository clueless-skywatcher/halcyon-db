package com.github.cluelessskywatcher.halcyondb.datatests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.github.cluelessskywatcher.halcyondb.data.DataType;
import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;
import com.github.cluelessskywatcher.halcyondb.testutils.TupleMetadataUtils;

public class TupleMetadataTest {
    @Test
    public void testTupleMetadataCreate() {
        TupleMetadata sampleMetadata = TupleMetadataUtils.createSampleTupleMetadata(5, DataType.INTEGER);
        assertEquals(sampleMetadata.getNumFields(), 5);
        for (int i = 0; i < sampleMetadata.getNumFields(); i++) {
            assertEquals(sampleMetadata.getTypeAt(i), DataType.INTEGER);
            assertEquals(sampleMetadata.getFieldNameAt(i), String.format("field%d", i));
        }
    }

    @Test
    public void testSize() {
        TupleMetadata sampleMetadata = TupleMetadataUtils.createSampleTupleMetadata(5, DataType.INTEGER);
        assertEquals(sampleMetadata.getTotalSize(), 20);

        TupleMetadata sampleMetadata2 = TupleMetadataUtils.createSampleTupleMetadata(5, DataType.STRING);
        assertEquals(sampleMetadata2.getTotalSize(), 259 * 5);
    }

    @Test
    public void testTupleMetadataEquality() {
        TupleMetadata sample1 = TupleMetadataUtils.createSampleTupleMetadata(5, DataType.INTEGER);
        TupleMetadata sample2 = TupleMetadataUtils.createSampleTupleMetadata(5, DataType.INTEGER);
        TupleMetadata sample3 = TupleMetadataUtils.createSampleTupleMetadata(6, DataType.INTEGER);
        TupleMetadata sample4 = TupleMetadataUtils.createSampleTupleMetadata(5, DataType.STRING);

        assertTrue(sample1.equals(sample2));
        assertFalse(sample1.equals(sample3));
        assertFalse(sample1.equals(sample4));
    }

    @Test
    public void testTupleMetadataCombine() {
        TupleMetadata sample1 = TupleMetadataUtils.createSampleTupleMetadata(5, DataType.INTEGER);
        
        DataType[] types = new DataType[5];
        Arrays.fill(types, DataType.INTEGER);
        String[] names = { "field2_1", "field2_2", "field2_3", "field2_4", "field2_5" };
        TupleMetadata sample2 = new TupleMetadata(types, names);

        TupleMetadata combined = TupleMetadata.combine(sample1, sample2);

        assertEquals(combined.getNumFields(), 10);
    }
}
