package com.github.cluelessskywatcher.halcyondb.data;

public class StringField implements DataField {
    private String value;

    public StringField(String value) {
        this.value = value;
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
}
