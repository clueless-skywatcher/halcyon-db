package com.github.cluelessskywatcher.halcyondb.storage;

import java.util.HashMap;
import java.util.Map;

import com.github.cluelessskywatcher.halcyondb.exceptions.BufferIsFullException;

public class DatabaseBufferPool {
    public static final int PAGE_SIZE = 4096;
    public static final int DEFAULT_PAGES = 50;

    private int numPages;

    private Map<Integer, PageBase> pages;

    public DatabaseBufferPool() {
        this.numPages = DEFAULT_PAGES;
        this.pages = new HashMap<>(numPages);
    }

    public DatabaseBufferPool(int numPages) {
        this.numPages = numPages;
        this.pages = new HashMap<>(numPages);
    }

    /*
     * Retrieve a page if stored in pool. If page is not
     * present, load it from disk, save it in pool and
     * retrieve it.
     */
    public PageBase getPage(int pageId) throws BufferIsFullException {
        if (pages.containsKey(pageId)) {
            return pages.get(pageId);
        }
        if (pages.size() < numPages) {
            loadFromDisk(pageId);
            return pages.get(pageId);
        }
        throw new BufferIsFullException("Buffer is full. Cannot add more pages");
    }

    private void loadFromDisk(int pageId) {
        // TODO: Change this later to actually load from disk and not use QuickPage
        PageBase page = new QuickPage(pageId);
        pages.put(pageId, page);
    }

    public int getNumPages() {
        return this.numPages;
    }
}