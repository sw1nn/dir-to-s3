package com.sw1nn.chunked;

import java.nio.ByteBuffer;
import java.io.IOException;

/**
   An output stream that 'chunks' it's output. The intention is that
   the actual writing of the data is performed asynchronously in some way.

   Delegates to an impl of `ChunkedOutputStreamDelegate`

*/
public class ChunkedOutputStream extends java.io.OutputStream
{
    private ChunkedOutputStreamDelegate delegate;

    public ChunkedOutputStream(ChunkedOutputStreamDelegate delegate) throws IOException {
	this.delegate = delegate;
    }

    @Override
    public void write (int b) throws IOException {
	delegate.writeInt(b);
    }

    @Override
    public void write(byte[] b, int offset, int len) throws IOException {
	delegate.writeArraySlice(b,offset,len);
    }

    @Override
    public void write (byte[] b)  throws IOException {
	delegate.writeArray(b);
    }

    @Override
    public void flush() throws IOException {
	delegate.flush();
    }

    @Override
    public void close()  throws IOException{
	delegate.close();
    }
}
