package com.github.cluelessskywatcher.halcyondb.data;

import java.io.DataOutputStream;
import java.io.IOException;

public class StringField implements DataField {
    private String value;
    private int maxSize;

    public StringField(String value, int maxSize) {
        this.maxSize = maxSize;
        if (value.length() > maxSize) {
            this.value = value.substring(0, maxSize);
        }
        else {
            this.value = value;
        }
    }

    @Override
    public boolean compare(QueryPredicate.Op op, DataField other) {
        if (other instanceof StringField) {
            StringField strOther = (StringField) other;
            if (op == QueryPredicate.Op.EQ) {
                return value.compareTo(strOther.value) == 0;
            }
            else if (op == QueryPredicate.Op.GT) {
                return value.compareTo(strOther.value) > 0;
            }
            else if (op == QueryPredicate.Op.GTE) {
                return value.compareTo(strOther.value) >= 0;
            }
            else if (op == QueryPredicate.Op.LIKE) {
                return value.contains(strOther.value);
            }
            else if (op == QueryPredicate.Op.LT) {
                return value.compareTo(strOther.value) < 0;
            }
            else if (op == QueryPredicate.Op.LTE) {
                return value.compareTo(strOther.value) <= 0;
            }
            else {
                return false;
            }
        }
        return false;
    }
    
    public String getValue() {
        return this.value;
    }

    @Override
    public DataType getType() {
        return DataType.STRING;
    }

    public String toString() {
        return value;
    }

    public boolean equals(Object other) {
        if (other instanceof StringField) {
            StringField otherString = (StringField) other;
            return value.equals(otherString.value);
        }
        return false;
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        String s = value;
        int zeroPad = maxSize - s.length();
        if (zeroPad < 0) {
            s = s.substring(0, maxSize);
        }

        dos.writeInt(zeroPad);
        dos.writeBytes(s);
        while (zeroPad-- > 0) {
            dos.write((byte)0);
        }
    }
}
