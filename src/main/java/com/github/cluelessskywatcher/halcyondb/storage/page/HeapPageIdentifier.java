package com.github.cluelessskywatcher.halcyondb.storage.page;

public class HeapPageIdentifier implements PageIdentifier {
    private int pageNo;
    private int tableId;

    public HeapPageIdentifier(int pageNo, int tableId) {
        this.pageNo = pageNo;
        this.tableId = tableId;
    }

    @Override
    public int getPageNo() {
        return this.pageNo;
    }

    @Override
    public int getTableId() {
        return this.tableId;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof HeapPageIdentifier) {
            HeapPageIdentifier id = (HeapPageIdentifier) other;
            return this.getPageNo() == id.getPageNo() && this.getTableId() == id.getTableId();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Integer.parseInt(
            Integer.toString(pageNo) + Integer.toString(tableId)
        );
    }
}
