package com.github.cluelessskywatcher.halcyondb.datatests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.github.cluelessskywatcher.halcyondb.HalcyonDBInstance;
import com.github.cluelessskywatcher.halcyondb.exceptions.BufferIsFullException;
import com.github.cluelessskywatcher.halcyondb.storage.DatabaseBufferPool;
import com.github.cluelessskywatcher.halcyondb.storage.page.PageBase;
import com.github.cluelessskywatcher.halcyondb.storage.page.QuickPage;

public class BufferPoolTest {
    @BeforeAll
    public static void setUp() throws BufferIsFullException {
        DatabaseBufferPool pool = HalcyonDBInstance.getBufferPool();
        for (int i = 0; i < DatabaseBufferPool.DEFAULT_PAGES; i++) {
            pool.getPage(i);
        }
    }

    @Test
    public void testGetPage() throws BufferIsFullException {
        DatabaseBufferPool pool = HalcyonDBInstance.getBufferPool();
        for (int i = 0; i < 50; i++) {
            PageBase page = pool.getPage(i);
            assertEquals(page, new QuickPage(i));
            byte[] pageData = page.getData();
            for (int j = 0; j < pageData.length; j++) {
                assertEquals(pageData[j], j + page.getId().getPageNo());
            }
        }
        assertThrows(BufferIsFullException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                pool.getPage(51);
            }
        });
    }
}
