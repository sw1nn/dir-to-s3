package com.sw1nn.chunked;

import java.io.IOException;
import java.io.Flushable;

public interface ChunkedOutputStreamDelegate extends AutoCloseable, Flushable {

    public void writeInt(int b) throws IOException;
    public void writeArraySlice(byte[] b, int offset, int len) throws IOException;
    public void writeArray(byte[] b)  throws IOException;

    public void close() throws IOException;
    public void flush() throws IOException;
}
