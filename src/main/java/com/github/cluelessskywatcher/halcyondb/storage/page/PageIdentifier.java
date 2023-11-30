package com.github.cluelessskywatcher.halcyondb.storage.page;

public interface PageIdentifier {
    public int getPageNo();

    public int getTableId();

    public boolean equals(Object other);

    public int hashCode();
}
