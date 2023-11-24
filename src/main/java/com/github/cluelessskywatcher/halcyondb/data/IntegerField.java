package com.github.cluelessskywatcher.halcyondb.data;

public class IntegerField implements DataField {
    private int value;

    public IntegerField(int value) {
        this.value = value;
    }

    @Override
    public boolean compare(QueryPredicate.Op op, DataField operand) {
        if (operand instanceof IntegerField) {
            IntegerField intOperand = (IntegerField) operand;
            if (op == QueryPredicate.Op.EQ) {
                return value == intOperand.getValue();
            }
            else if (op == QueryPredicate.Op.GT) {
                return value > intOperand.getValue();
            }
            else if (op == QueryPredicate.Op.GTE) {
                return value >= intOperand.getValue();
            }
            else if (op == QueryPredicate.Op.LIKE) {
                return value == intOperand.getValue();
            }
            else if (op == QueryPredicate.Op.LT) {
                return value < intOperand.getValue();
            }
            else if (op == QueryPredicate.Op.LTE) {
                return value <= intOperand.getValue();
            }
            else {
                return false;
            }
        }
        return false;    
    }

    public int getValue() {
        return this.value;
    }

    public String toString() {
        return Integer.toString(this.value);
    }

    @Override
    public DataType getType() {
        return DataType.INTEGER;
    }

    public boolean equals(Object other) {
        if (other instanceof IntegerField) {
            IntegerField otherInteger = (IntegerField) other;
            return value == otherInteger.value;
        }
        return false;
    }
}
