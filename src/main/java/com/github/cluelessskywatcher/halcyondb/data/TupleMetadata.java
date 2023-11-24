package com.github.cluelessskywatcher.halcyondb.data;

import java.text.MessageFormat;
import java.util.StringJoiner;

public class TupleMetadata {
    private String[] fieldNames;
    private DataType[] types; 

    public TupleMetadata(DataType[] types, String[] fieldNames) {
        this.types = types;
        this.fieldNames = fieldNames;
    }

    public DataType getTypeAt(int i) {
        return this.types[i];
    }

    public String getFieldNameAt(int i) {
        return this.fieldNames[i];
    }

    public String toString() {
        StringJoiner joiner = new StringJoiner(", ");
        for (int i = 0; i < types.length; i++) {
            joiner.add(
                MessageFormat.format("{0}({1})", 
                fieldNames[i], types[i].toString())
            );
        }
        return joiner.toString();
    }

    public int getNumFields() {
        return types.length;
    }

    public int getTotalSize() {
        int totalSize = 0;
        for (int i = 0; i < types.length; i++) {
            totalSize += types[i].getSize();
        }
        return totalSize;
    }

    public static TupleMetadata combine(TupleMetadata td1, TupleMetadata td2) {
        String[] newFieldNames = new String[td1.getNumFields() + td2.getNumFields()];
        DataType[] newTypes = new DataType[td1.getNumFields() + td2.getNumFields()];

        for (int i = 0; i < td1.getNumFields(); i++) {
            newFieldNames[i] = td1.getFieldNameAt(i);
            newTypes[i] = td1.getTypeAt(i);
        }

        for (int i = 0; i < td2.getNumFields(); i++) {
            newFieldNames[td1.getNumFields() + i] = td2.getFieldNameAt(i);
            newTypes[td1.getNumFields() + i] = td2.getTypeAt(i);
        }

        return new TupleMetadata(newTypes, newFieldNames);
    }

    public boolean equals(Object other) {
        if (other instanceof TupleMetadata) {
            TupleMetadata otherMetadata = (TupleMetadata) other;
            if (otherMetadata.getNumFields() != getNumFields()) {
                return false;
            }
            for (int i = 0; i < getNumFields(); i++) {
                if (!getFieldNameAt(i).equals(otherMetadata.getFieldNameAt(i)) || getTypeAt(i) != otherMetadata.getTypeAt(i)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
