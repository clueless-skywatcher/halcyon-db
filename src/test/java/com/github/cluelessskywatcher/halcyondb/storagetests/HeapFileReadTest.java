package com.github.cluelessskywatcher.halcyondb.storagetests;

import java.io.IOException;

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
        heapFileRandom = HeapFileUtils.generateHeapFile(10, 10, "heapFile1.dat");
        
        Object[][] rows = {
            {29, 29, 98, 7, 23, 84, 4, 77, 20, 17},
            {66, 74, 51, 28, 97, 72, 41, 86, 87, 100},
            {17, 24, 81, 19, 89, 89, 78, 69, 35, 81},
            {1, 42, 85, 96, 42, 30, 35, 59, 70, 10},
            {55, 67, 43, 24, 45, 91, 86, 6, 23, 63}
        };
        heapFileFromList = HeapFileUtils.generateHeapFileFromList(rows, "heapFile2.dat");

        HalcyonDBInstance.getCatalog().addTable(heapFileFromList, "table_list");
        HalcyonDBInstance.getCatalog().addTable(heapFileRandom, "table_random");
    }

    @Test
    public void testHeapFilePageCount() {
        Assertions.assertEquals(heapFileRandom.getPageCount(), 1);
        Assertions.assertEquals(heapFileFromList.getPageCount(), 1);
        Assertions.assertEquals(heapFileRandom.getTupleMetadata().getNumFields(), 10);
        Assertions.assertEquals(heapFileFromList.getTupleMetadata().getNumFields(), 10);
    }

    @Test
    public void testReadHeapFileList() {
        for (int i = 0; i < heapFileFromList.getPageCount(); i++) {
            HeapPage page = heapFileFromList.readPage(
                new HeapPageIdentifier(i, heapFileFromList.getId())
            );
            System.out.println(page.getTupleCount());
            for (int j = 0; j < page.getTupleCount(); j++) {
                System.out.println(page.getTupleAt(j));
            }
        }
    }

    @Test
    public void testReadHeapFileRandom() {
        for (int i = 0; i < heapFileRandom.getPageCount(); i++) {
            HeapPage page = heapFileRandom.readPage(
                new HeapPageIdentifier(i, heapFileRandom.getId())
            );
            System.out.println(page.getTupleCount());
            for (int j = 0; j < page.getTupleCount(); j++) {
                System.out.println(page.getTupleAt(j));
            }
        }
    }
}
