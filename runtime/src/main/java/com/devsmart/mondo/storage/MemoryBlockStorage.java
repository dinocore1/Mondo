package com.devsmart.mondo.storage;


import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class MemoryBlockStorage implements BlockStorage {

    private final int mSize;
    private final byte[] mBuffer;
    private boolean mDirty = false;

    public MemoryBlockStorage(int size) {
        Preconditions.checkArgument(size > 0);
        mSize = size;
        mBuffer = new byte[size];
    }

    @Override
    public int readBlock(long offset, ByteBuffer buffer) throws IOException {
        if(offset < 0 || offset >= mSize) {
            throw new IOException("offset out of bounds: " + offset);
        }

        final long len = Math.min(buffer.remaining(), mSize - offset);
        buffer.put(mBuffer, (int)offset, (int)len);
        return (int) len;
    }

    @Override
    public int writeBlock(long offset, ByteBuffer buffer) throws IOException {
        if(offset < 0 || offset >= mSize) {
            throw new IOException("offset out of bounds: " + offset);
        }

        final int len = (int) Math.min(buffer.remaining(), mSize - offset);
        buffer.get(mBuffer, (int) offset, len);
        mDirty = true;
        return len;
    }

    public boolean isDirty() {
        return mDirty;
    }

    public void clear() {
        Arrays.fill(mBuffer, (byte)0x0);
        mDirty = false;
    }

}
