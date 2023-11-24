package com.github.cluelessskywatcher.halcyondb.datatests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.github.cluelessskywatcher.halcyondb.data.DataType;
import com.github.cluelessskywatcher.halcyondb.data.Tuple;
import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;
import com.github.cluelessskywatcher.halcyondb.testutils.TupleMetadataUtils;
import com.github.cluelessskywatcher.halcyondb.testutils.TuplesUtils;

public class TuplesTest {
    @Test
    public void testCreateTuple() {
        Tuple sample1 = TuplesUtils.createSampleTuple(6);
        TupleMetadata metadata1 = TupleMetadataUtils.createSampleTupleMetadata(6, DataType.INTEGER);
        assertTrue(metadata1.equals(sample1.getMetadata()));
        assertEquals(sample1.getNumFields(), 6);
        assertTrue(sample1.toString().equals("5, 5, 5, 5, 5, 5"));
    }

    @Test
    public void testCompareTuple() {
        Tuple sample1 = TuplesUtils.createSampleTuple(6);
        Tuple sample2 = TuplesUtils.createSampleTuple(6);
        Tuple sample3 = TuplesUtils.createSampleTuple(7);

        assertTrue(sample1.equals(sample2));
        assertFalse(sample1.equals(sample3));
    }
}
