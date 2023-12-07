package com.github.cluelessskywatcher.halcyondb.storagetests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.github.cluelessskywatcher.halcyondb.HalcyonDBInstance;
import com.github.cluelessskywatcher.halcyondb.storage.file.HeapFile;
import com.github.cluelessskywatcher.halcyondb.storage.page.HeapPage;
import com.github.cluelessskywatcher.halcyondb.storage.page.HeapPageIdentifier;
import com.github.cluelessskywatcher.halcyondb.testutils.HeapFileUtils;
@TestInstance(Lifecycle.PER_CLASS)
public class HeapFileReadTest {
    private HeapFile heapFileRandom;
    private HeapFile heapFileFromList;

    @BeforeAll
    public void setUp() throws IOException {
        heapFileRandom = HeapFileUtils.generateHeapFile(100, 100, "heapFile1.dat");
        
        Object[][] rows = {
            {10, 11, 12, 13, 14, 15, 16},
            {20, 21, 22, 23, 24, 25, 26},
            {30, 31, 32, 33, 34, 35, 36},
            {-10, -11, -12, -13, -14, -15, -16},
            {40, 41, 42, 43, 44, 45, 46},
            {10, 11, 12, 13, 14, 15, 16},
            {20, 21, 22, 23, 24, 25, 26},
            {30, 31, 32, 33, 34, 35, 36},
            {-10, -11, -12, -13, -14, -15, -16},
            {40, 41, 42, 43, 44, 45, 46}
        };
        heapFileFromList = HeapFileUtils.generateHeapFileFromList(rows, "heapFile2.dat");

        HalcyonDBInstance.getCatalog().addTable(heapFileFromList, "table_list");
        HalcyonDBInstance.getCatalog().addTable(heapFileRandom, "table_random");
    }

    @Test
    public void testHeapFilePageCount() {
        Assertions.assertEquals(heapFileRandom.getPageCount(), 10);
        Assertions.assertEquals(heapFileFromList.getPageCount(), 1);
        Assertions.assertEquals(heapFileRandom.getTupleMetadata().getNumFields(), 100);
        Assertions.assertEquals(heapFileFromList.getTupleMetadata().getNumFields(), 7);
    }

    @Test
    public void testReadHeapFileList() {
        HeapPage page = heapFileFromList.readPage(
            new HeapPageIdentifier(0, heapFileFromList.getId())
        );
        System.out.println(page.getTupleAt(0));
    }
}
