package com.github.cluelessskywatcher.halcyondb.data;

import java.io.DataInputStream;
import java.io.IOException;

public enum DataType {
    
    INTEGER() {
        @Override
        public int getSize() {
            return 4;
        }

        @Override 
        public IntegerField parse(DataInputStream dis) throws Exception {
            try {
                return new IntegerField(dis.readInt());
            } catch (IOException e) {
                throw new Exception("Error reading string from data stream");
            }
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

        @Override
        public StringField parse(DataInputStream dis) throws Exception {
            try {
                int strLen = dis.readInt();
                byte[] str = new byte[strLen];
                dis.read(str);
                dis.skipBytes(MAX_STRING_LENGTH - strLen);

                return new StringField(new String(str), MAX_STRING_LENGTH);
            } catch (IOException e) {
                throw new Exception("Error reading integer from data stream");
            }
        }
    };

    public static final int MAX_STRING_LENGTH = 255;

    public abstract int getSize();

    public abstract DataField parse(DataInputStream dis) throws Exception;
}
