package com.github.cluelessskywatcher.halcyondb.storage.page;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.github.cluelessskywatcher.halcyondb.HalcyonDBInstance;
import com.github.cluelessskywatcher.halcyondb.data.DataField;
import com.github.cluelessskywatcher.halcyondb.data.DataType;
import com.github.cluelessskywatcher.halcyondb.data.Tuple;
import com.github.cluelessskywatcher.halcyondb.data.TupleIdentifier;
import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;
import com.github.cluelessskywatcher.halcyondb.storage.DatabaseBufferPool;

public class HeapPage implements PageBase {
    private byte[] header;
    private TupleMetadata metadata;
    private HeapPageIdentifier id;
    private Tuple[] rows;
    private int numSlots;

    public HeapPage(HeapPageIdentifier id, byte[] data) throws IOException {
        this.id = id;
        this.metadata = HalcyonDBInstance.getCatalog().getTupleMetadataFromId(id.getTableId());
        this.numSlots = getTupleCount();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        this.header = new byte[getHeaderSize()];

        for (int i = 0; i < this.header.length; i++) {
            this.header[i] = dis.readByte();
        }

        try {
            rows = new Tuple[this.numSlots];
            for (int i = 0; i < rows.length; i++) {
                rows[i] = readNextTuple(dis, i);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        dis.close();
    }

    private Tuple readNextTuple(DataInputStream dis, int slotIndex) throws Exception {
        // If the particular slot index is empty, skip to the next
        // tuple
        if (!hasValue(slotIndex)){
            for (int i = 0; i < this.metadata.getTotalSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
        
        Tuple newTuple = new Tuple(metadata);
        TupleIdentifier tupleId = new TupleIdentifier(id, slotIndex);
        newTuple.setIdentifier(tupleId);

        for (int i = 0; i < metadata.getNumFields(); i++) {
            DataType type = metadata.getTypeAt(i);
            DataField field = type.parse(dis);

            newTuple.setFieldAt(i, field);
        }

        return newTuple;
    }

    private boolean hasValue(int slotIndex) {
        return (this.header[slotIndex / 8] & (1 << (slotIndex % 8))) != 0;
    }

    public int getHeaderSize() {
        return (int) Math.ceil(this.numSlots / 8.0);
    }

    public int getTupleCount() {
        return (int) Math.floor(
                (DatabaseBufferPool.PAGE_SIZE * 8.0) / (this.metadata.getTotalSize() * 8.0 + 1.0));
    }

    @Override
    public PageIdentifier getId() {
        return this.id;
    }

    public TupleMetadata getMetadata() {
        return this.metadata;
    }

    public Tuple getTupleAt(int i) {
        if (!hasValue(i)) {
            return null;
        }
        return rows[i];
    }

    @Override
    public byte[] getData() {
        int pageBytes = DatabaseBufferPool.PAGE_SIZE;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(pageBytes);
        DataOutputStream dos = new DataOutputStream(outputStream);
        
        // First we write the header
        for (int i = 0; i < header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Now we start writing the data
        for (int i = 0; i < rows.length; i++) {
            if (!hasValue(i)) {
                for (int j = 0; j < metadata.getTotalSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }
            for (int j = 0; j < metadata.getNumFields(); j++) {
                DataField f = rows[i].getFieldAt(j);
                try {
                    f.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // And then we pad the remainder of the page with zeros
        int padding = DatabaseBufferPool.PAGE_SIZE - (metadata.getTotalSize() * rows.length + header.length);
        while (padding-- > 0) {
            try {
                dos.writeByte(0);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return outputStream.toByteArray();
    }

    public static byte[] createEmptyPage() {
        return new byte[DatabaseBufferPool.PAGE_SIZE];
    }
}
