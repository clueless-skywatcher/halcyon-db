package com.github.cluelessskywatcher.halcyondb.data;

import java.text.MessageFormat;

public class QueryPredicate {
    public enum Op {
        EQ {
            public String toString() {
                return "=";
            }
        },
        GT {
            public String toString() {
                return ">";
            }
        },
        GTE {
            public String toString() {
                return ">=";
            }
        },
        LT {
            public String toString() {
                return "<";
            }
        },
        LTE {
            public String toString() {
                return ">=";
            }
        },
        NE {
            public String toString() {
                return "!=";
            }
        },
        LIKE {
            public String toString() {
                return "~";
            }
        }
    }

    private int fieldIndex;
    private Op op;
    private DataField operand;

    public QueryPredicate(int fieldIndex, Op op, DataField operand) {
        this.fieldIndex = fieldIndex;
        this.op = op;
        this.operand = operand;
    }

    public String toString() {
        return MessageFormat.format(
            "(Field at {0}) {1} {2}",
            fieldIndex, op.toString(), operand.toString()
        );
    }
}
