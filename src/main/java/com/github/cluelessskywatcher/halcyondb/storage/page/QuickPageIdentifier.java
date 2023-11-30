package com.github.cluelessskywatcher.halcyondb.storage.page;

public class QuickPageIdentifier implements PageIdentifier {
    private int pageNo;
    private int tableId;

    public QuickPageIdentifier(int pageNo, int tableId) {
        this.pageNo = pageNo;
        this.tableId = tableId;
    }

    @Override
    public int getPageNo() {
        return pageNo;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof QuickPageIdentifier) {
            QuickPageIdentifier id = (QuickPageIdentifier) other;
            return this.getPageNo() == id.getPageNo() && this.getTableId() == id.getTableId();
        }
        return false;
    }
    
    public int hashCode() {
        return Integer.parseInt(
            Integer.toString(pageNo) + Integer.toString(tableId)
        );
    }
}
