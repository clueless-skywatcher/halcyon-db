package com.github.cluelessskywatcher.halcyondb.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class QuickPage implements PageBase {

    private int id;
    private byte[] data;

    public QuickPage(int id) {
        this.id = id;
        ByteBuffer buffer = ByteBuffer.allocate(10);
        this.data = buffer.array();
        for (int i = 0; i < this.data.length; i++) {
            this.data[i] = (byte) (i + id);
        }
    }

    @Override
    public int getId() {
        return this.id;
    }

    @Override
    public byte[] getData() {
        return this.data;
    }

    public boolean equals(Object other) {
        if (other instanceof QuickPage) {
            QuickPage qp = (QuickPage) other;
            return Arrays.equals(qp.getData(), getData()) && qp.getId() == getId();
        }
        return false;
    }
    
}
