package com.github.cluelessskywatcher.halcyondb.storage.file;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.github.cluelessskywatcher.halcyondb.data.DataType;
import com.github.cluelessskywatcher.halcyondb.data.TupleMetadata;
import com.github.cluelessskywatcher.halcyondb.storage.DatabaseBufferPool;

public class HeapFileGenerator {
    public static HeapFile generateHeapFileFromList(Object[][] rows, String outFileName, int pageBytes, int numFields,
            TupleMetadata metadata) throws IOException {

        int recordBytes = 0;

        DataType[] types = new DataType[numFields];
        
        for (int i = 0; i < numFields; i++) {
            types[i] = metadata.getTypeAt(i);
        }

        for (int i = 0; i < numFields; i++) {
            recordBytes += types[i].getSize();
        }

        int tuplesPerPage = (recordBytes * 8) / (DatabaseBufferPool.PAGE_SIZE * 8 + 1);
        int headerBytes = (int) Math.ceil((double)recordBytes / 8);

        int headerBits = headerBytes * 8;

        File outFile = new File(outFileName);
        FileOutputStream outputStream = new FileOutputStream(outFile);
        
        // We need an output stream for writing bytes to header
        ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(headerBytes);
        DataOutputStream headerDOS = new DataOutputStream(headerBAOS);

        // And another output stream for writing the actual data
        ByteArrayOutputStream dataBAOS = new ByteArrayOutputStream(pageBytes);
        DataOutputStream dataDOS = new DataOutputStream(dataBAOS);

        int rowCount = rows.length;
        int colColunt = numFields;

        int currentTuplesInserted = 0;

        for (int i = 0; i < rowCount; i++) {
            if (currentTuplesInserted >= tuplesPerPage || (i == rowCount && currentTuplesInserted > 0)) {
                int idx = 0;
                byte headerByte = 0;

                for (idx = 0; idx < headerBits; idx++){
                    if (idx < tuplesPerPage) {
                        headerByte |= (1 << (idx % 8));
                    }

                    if ((i + 1) % 8 == 0) {
                        headerDOS.writeByte(headerByte);
                        headerByte = 0;
                    }
                }

                if (i % 8 > 0) {
                    headerDOS.writeByte(headerByte);
                }

                for (idx = 0; idx < (pageBytes - (currentTuplesInserted * recordBytes + headerBytes)); idx++) {
                    dataDOS.writeByte(0);
                }

                headerDOS.flush();
                headerBAOS.writeTo(outputStream);
                dataDOS.flush();
                dataBAOS.writeTo(outputStream);

                headerBAOS = new ByteArrayOutputStream(headerBytes);
                headerDOS = new DataOutputStream(headerBAOS);
                dataBAOS = new ByteArrayOutputStream(pageBytes);
                dataDOS = new DataOutputStream(dataBAOS);

                currentTuplesInserted = 0;
            }
            for (int j = 0; j < colColunt; j++){
                Object val = rows[i][j];
                if (types[j] == DataType.INTEGER && val instanceof Integer) {
                    dataDOS.writeInt((int) val);
                }
                else if (types[j] == DataType.STRING && val instanceof String) {
                    String valString = (String) val;
                    valString = valString.trim();
                    int zeroPad = DataType.MAX_STRING_LENGTH - valString.length();
                    if (zeroPad < 0) {
                        valString = valString.substring(0, DataType.MAX_STRING_LENGTH);
                    }
                    // Write the length of the string in the header
                    dataDOS.writeInt(valString.length());
                    // Write the bytes
                    dataDOS.writeBytes(valString);
                    
                    while (zeroPad-- > 0) {
                        dataDOS.write((byte) 0);
                    }
                }
            }
            currentTuplesInserted++;
        }

        

        return new HeapFile(outFile, metadata);
    }
}
