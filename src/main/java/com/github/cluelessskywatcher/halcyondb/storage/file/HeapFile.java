package com.github.cluelessskywatcher.halcyondb.storage.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.NoSuchElementException;

import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;
import com.github.cluelessskywatcher.halcyondb.storage.DatabaseBufferPool;
import com.github.cluelessskywatcher.halcyondb.storage.page.HeapPage;
import com.github.cluelessskywatcher.halcyondb.storage.page.HeapPageIdentifier;
import com.github.cluelessskywatcher.halcyondb.storage.page.PageBase;
import com.github.cluelessskywatcher.halcyondb.storage.page.PageIdentifier;

public class HeapFile implements DatabaseFile {
    private File file;
    private TupleMetadata metadata;
    private int pageCount;
    private int id;
    
    public HeapFile(File file, TupleMetadata metadata) {
        this.file = file;
        this.metadata = metadata;
        this.id = file.getAbsolutePath().hashCode();
        this.pageCount = (int) file.length() / DatabaseBufferPool.PAGE_SIZE;
    }

    @Override
    public int getId() {
        return this.id;
    }

    @Override
    public TupleMetadata getTupleMetadata() {
        return this.metadata;
    }

    public int getPageCount() {
        return this.pageCount;
    }

    public HeapPage readPage(HeapPageIdentifier pageId) {
        if (pageId.getPageNo() > pageCount) {
            throw new NoSuchElementException("Invalid read page with number: " + pageId.getPageNo());
        }

        try {
            if (pageId.getPageNo() == pageCount) {
                pageCount++;
                return new HeapPage(pageId, HeapPage.createEmptyPage());
            }
            byte[] data = new byte[DatabaseBufferPool.PAGE_SIZE];
            RandomAccessFile raf = new RandomAccessFile(file.getAbsolutePath().toString(), "r");
            raf.seek(DatabaseBufferPool.PAGE_SIZE * pageId.getPageNo());
            raf.read(data);
            raf.close();
            return new HeapPage(pageId, data);                      
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void writePage(PageBase page) throws IOException {
        PageIdentifier pageId = page.getId();
        long pageOffset = pageId.getPageNo() * DatabaseBufferPool.PAGE_SIZE;

        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        raf.seek(pageOffset);
        raf.write(page.getData());
        raf.close();
    }
}
