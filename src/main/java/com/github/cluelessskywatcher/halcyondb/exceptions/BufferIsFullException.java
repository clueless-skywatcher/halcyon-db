package com.github.cluelessskywatcher.halcyondb.exceptions;

public class BufferIsFullException extends Exception {
    public BufferIsFullException(String message) {
        super(message);
    }

    public BufferIsFullException() {
        super();
    }
}
