package com.github.cluelessskywatcher.halcyondb.data;

import java.util.StringJoiner;

public class Tuple {
    private TupleMetadata metadata;
    private DataField[] data;
    private TupleIdentifier tupleId;

    public Tuple(TupleMetadata metadata) {
        this.metadata = metadata;
        this.data = new DataField[metadata.getNumFields()];
    }

    public Tuple(TupleMetadata metadata, DataField[] data) throws Exception {
        this.metadata = metadata;
        if (data.length != metadata.getNumFields()) {
            throw new Exception("Data should have same number of fields as metadata");
        }
        this.data = data;
    }

    public Tuple(TupleMetadata metadata, DataField[] data, TupleIdentifier id) throws Exception {
        this.metadata = metadata;
        if (data.length != metadata.getNumFields()) {
            throw new Exception("Data should have same number of fields as metadata");
        }
        this.data = data;
        this.tupleId = id;
    }

    public int getTotalSize() {
        return metadata.getTotalSize();
    }

    public DataField getFieldAt(int i){
        return data[i];
    }

    public void setIdentifier(TupleIdentifier id) {
        this.tupleId = id;
    }

    public void setFieldAt(int i, DataField data) {
        this.data[i] = data;
    }

    public String toString() {
        StringJoiner joiner = new StringJoiner(", ");
        for (int i = 0; i < data.length; i++) {
            joiner.add(data[i].toString());
        }
        return joiner.toString();
    }

    public TupleMetadata getMetadata() {
        return metadata;
    }

    public int getNumFields() {
        return data.length;
    }

    public boolean equals(Object other) {
        if (other instanceof Tuple) {
            Tuple otherTuple = (Tuple) other;
            if (otherTuple.getNumFields() != getNumFields()) {
                return false;
            }
            for (int i = 0; i < getNumFields(); i++) {
                if (!getFieldAt(i).equals(otherTuple.getFieldAt(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
