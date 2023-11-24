package com.github.cluelessskywatcher.halcyondb.data;

public enum DataType {
    INTEGER() {
        @Override
        public int getSize() {
            // TODO Auto-generated method stub
            return 4;
        }
        
    },
    STRING() {
        @Override
        public int getSize() {
            // For strings: We store a header integer which is the 
            // length of the string, then we store the string. So
            // in total we use string length + 4 bytes
            return MAX_STRING_LENGTH + 4;
        }
    };

    public abstract int getSize();

    public static final int MAX_STRING_LENGTH = 255;
}
