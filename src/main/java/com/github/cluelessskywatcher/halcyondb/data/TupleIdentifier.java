package com.github.cluelessskywatcher.halcyondb.data;

import com.github.cluelessskywatcher.halcyondb.storage.page.PageIdentifier;

public class TupleIdentifier {
    private PageIdentifier pageId;
    private int tupleNo;

    public TupleIdentifier(PageIdentifier pageId, int tupleNo) {
        this.pageId = pageId;
        this.tupleNo = tupleNo;
    }

    public PageIdentifier getPageId() {
        return pageId;
    }

    public int getTupleNo() {
        return tupleNo;
    }

    public boolean equals(Object other) {
        if (other instanceof TupleIdentifier) {
            TupleIdentifier id = (TupleIdentifier) other;
            return this.getPageId().equals(id.getPageId()) && this.getTupleNo() == id.getTupleNo();
        }
        return false;
    }

    public int hashCode() {
        return this.getPageId().getPageNo() + getTupleNo();
    }
}
