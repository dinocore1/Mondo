package com.devsmart.mondo.storage;


import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class VirtualFileInputStream extends InputStream {

    private final long mLimit;
    private final VirtualFile mVFile;
    private final ByteBuffer mBuffer;
    private int mPosition;

    public VirtualFileInputStream(VirtualFile vfile){
        mVFile = vfile;
        mLimit = vfile.size();
        mBuffer = ByteBuffer.allocate(4096);
        mPosition = 0;
    }

    @Override
    public int read() throws IOException {
        if(mPosition >= mLimit) {
            return -1;
        }

        mBuffer.clear();
        int bytesRead = mVFile.read(mBuffer, 1, mPosition);
        mPosition += bytesRead;
        if(bytesRead == 1) {
            mBuffer.flip();
            return mBuffer.get();
        } else {
            return -1;
        }
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        if(mPosition >= mLimit) {
            return -1;
        }

        mBuffer.clear();

        len = Math.min(mBuffer.capacity(), len);
        len = (int) Math.min(mLimit - mPosition, len);
        mBuffer.limit(len);

        int bytesRead = mVFile.read(mBuffer, len, mPosition);
        mBuffer.flip();
        mBuffer.get(b, off, bytesRead);
        mPosition += bytesRead;
        return bytesRead;
    }
}
