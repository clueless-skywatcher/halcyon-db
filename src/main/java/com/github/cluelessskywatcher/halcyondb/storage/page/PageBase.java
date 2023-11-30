package com.github.cluelessskywatcher.halcyondb.storage.page;

public interface PageBase {
    public PageIdentifier getId();
    
    public byte[] getData();
}
