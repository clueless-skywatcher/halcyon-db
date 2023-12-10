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
        // Number of bytes present in a row
        int recordBytes = 0;
        // Array of the types in the metadata
        DataType[] types = new DataType[numFields];
        
        for (int i = 0; i < numFields; i++) {
            types[i] = metadata.getTypeAt(i);
        }

        for (int i = 0; i < numFields; i++) {
            recordBytes += types[i].getSize();
        }
        // Number of tuples that fit in a DatabaseBufferPool.PAGE_SIZE bytes page.
        // We need to note that each tuple takes recordBytes * 8 bits for the data
        // and 1 bit for the header 
        int tuplesPerPage = (DatabaseBufferPool.PAGE_SIZE * 8) / (recordBytes * 8 + 1);

        // Number of bytes present in the header.
        int headerBytes = (int) Math.ceil((double)tuplesPerPage / 8);

        // Number of bits in the header
        int headerBits = headerBytes * 8;

        File outFile = new File(outFileName);
        // Open up an outputStream to write to the file
        FileOutputStream outputStream = new FileOutputStream(outFile);
        
        // We need an output stream for writing bytes to header
        ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(headerBytes);
        DataOutputStream headerDOS = new DataOutputStream(headerBAOS);

        // And another output stream for writing the actual data
        ByteArrayOutputStream dataBAOS = new ByteArrayOutputStream(pageBytes);
        DataOutputStream dataDOS = new DataOutputStream(dataBAOS);

        int rowCount = rows.length;
        int colColunt = numFields;

        // Record how many tuples have been processed so far
        int currentTuplesInserted = 0;

        int i = 0;

        while (true) {
            // If a page is filled up
            if (currentTuplesInserted >= tuplesPerPage || (i == rowCount && currentTuplesInserted > 0)) {
                int idx = 0;
                byte headerByte = 0;
                // Start writing header bytes
                for (idx = 0; idx < headerBits; idx++){
                    if (idx < tuplesPerPage) {
                        headerByte |= (1 << (idx % 8));
                    }
                    // For every 8th bit write the byte to the header data stream
                    if ((idx + 1) % 8 == 0) {
                        headerDOS.writeByte(headerByte);
                        headerByte = 0;
                    }
                }
                if (idx % 8 > 0) {
                    headerDOS.writeByte(headerByte);
                }
                // Fill the rest of the page with 0s
                for (idx = 0; idx < (pageBytes - (currentTuplesInserted * recordBytes + headerBytes)); idx++) {
                    dataDOS.writeByte(0);
                }
                // Flush header and data
                headerDOS.flush();
                headerBAOS.writeTo(outputStream);
                dataDOS.flush();
                dataBAOS.writeTo(outputStream);
                // Reset header and data streams
                headerBAOS = new ByteArrayOutputStream(headerBytes);
                headerDOS = new DataOutputStream(headerBAOS);
                dataBAOS = new ByteArrayOutputStream(pageBytes);
                dataDOS = new DataOutputStream(dataBAOS);
                // Reset page to fill new page
                currentTuplesInserted = 0;
            }
            if (i >= rowCount) {
                break;
            }
            for (int j = 0; j < colColunt; j++){
                Object val = rows[i][j];
                // If the corresponding type is integer, just write it out
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
                    // Pad the rest of the 1024 bytes allotted for a string with 0s
                    while (zeroPad-- > 0) {
                        dataDOS.write((byte) 0);
                    }
                }
            }
            currentTuplesInserted++;
            i++;
        }
        outputStream.flush();
        outputStream.close();
        
        return new HeapFile(outFile, metadata);
    }
}
