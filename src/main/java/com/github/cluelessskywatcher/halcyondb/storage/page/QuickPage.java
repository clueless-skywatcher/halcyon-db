package com.github.cluelessskywatcher.halcyondb.storage.page;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class QuickPage implements PageBase {

    private PageIdentifier id;
    private byte[] data;

    public QuickPage(int id) {
        this.id = new QuickPageIdentifier(id, 1);
        ByteBuffer buffer = ByteBuffer.allocate(10);
        this.data = buffer.array();
        for (int i = 0; i < this.data.length; i++) {
            this.data[i] = (byte) (i + id);
        }
    }

    public QuickPage(int id, int tableId) {
        this.id = new QuickPageIdentifier(id, tableId);
        ByteBuffer buffer = ByteBuffer.allocate(10);
        this.data = buffer.array();
        for (int i = 0; i < this.data.length; i++) {
            this.data[i] = (byte) (i + id);
        }
    }

    @Override
    public PageIdentifier getId() {
        return this.id;
    }

    @Override
    public byte[] getData() {
        return this.data;
    }

    public boolean equals(Object other) {
        if (other instanceof QuickPage) {
            QuickPage qp = (QuickPage) other;
            return Arrays.equals(qp.getData(), getData()) && qp.getId().equals(getId());
        }
        return false;
    }
    
}
