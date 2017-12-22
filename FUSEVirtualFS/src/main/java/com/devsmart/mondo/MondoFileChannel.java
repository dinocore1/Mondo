package com.devsmart.mondo;


import com.devsmart.mondo.storage.SparseArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;

public class MondoFileChannel implements SeekableByteChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(MondoFileChannel.class);

    public static final int MODE_READ = 0x1;
    public static final int MODE_WRITE = 0x2;

    int mOpenMode;
    final FileChannel mScratchFile;
    private final MondoFileStore mFSStore;

    private long mPosition;
    private long mSize;
    private SparseArray<Long> mBufferIndex = new SparseArray<Long>(10);
    private ByteBuffer mBuffer = ByteBuffer.allocate(MondoFileStore.BUFFER_SIZE);
    private int mBufferNum;
    private boolean mIsOpen;

    MondoFileChannel(int openMode, FileChannel scratchFile, FileMetadata metadata, MondoFileStore store) {
        mOpenMode = openMode;
        mScratchFile = scratchFile;
        mFSStore = store;
        mIsOpen = true;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        if((mOpenMode & MODE_READ) == 0) {
            throw new IOException("no read permission");
        }

        if(byteBuffer.remaining() == 0) {
            return 0;
        }

        if(mBuffer.remaining() == 0) {
            loadBuffer(mBufferNum+1);
        }

        int bytesRead = Math.min(mBuffer.remaining(), byteBuffer.remaining());
        bytesRead = Math.min((int) (mSize - mPosition), bytesRead);
        for(int i=0;i<bytesRead;i++) {
            byteBuffer.put(mBuffer.get());
        }

        mPosition += bytesRead;

        return bytesRead;
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        if((mOpenMode & MODE_WRITE) == 0) {
            throw new IOException("no write permission");
        }

        if(byteBuffer.remaining() == 0) {
            return 0;
        }

        if(mBuffer.remaining() == 0) {
            flushBuffer();
            mBufferNum++;
        }

        int bytesWritten = Math.min(mBuffer.remaining(), byteBuffer.remaining());
        for(int i=0;i<bytesWritten;i++) {
            mBuffer.put(byteBuffer.get());
        }

        mPosition += bytesWritten;
        if(mPosition > mSize) {
            mSize = mPosition;
        }

        return bytesWritten;
    }

    private void flushBuffer() throws IOException {

        Long pos = mBufferIndex.get(mBufferNum);
        if(pos == null) {
            pos = mScratchFile.size();
        }
        mScratchFile.position(pos);

        mBuffer.flip();

        do{
            mScratchFile.write(mBuffer);
        } while(mBuffer.remaining() > 0);

        mBufferIndex.put(mBufferNum, pos);

        mBuffer.clear();
    }

    @Override
    public long position() throws IOException {
        return mPosition;
    }

    @Override
    public SeekableByteChannel position(long l) throws IOException {
        int newBufferIndex = (int) (l / mBuffer.capacity());
        int offset = (int) (l % mBuffer.capacity());

        if(newBufferIndex != mBufferNum) {
            loadBuffer(newBufferIndex);
        }

        mBuffer.position(offset);
        mPosition = l;

        return this;
    }

    private void loadBuffer(int bufferNum) throws IOException {
        Long pos = mBufferIndex.get(bufferNum);
        if(pos != null) {
            mScratchFile.position(pos);

            mBuffer.clear();
            do {
                mScratchFile.read(mBuffer);
            } while(mBuffer.remaining() > 0);
            mBuffer.flip();
            mBufferNum = bufferNum;

        } else {
            //TODO: load from somewhere else?
        }

    }

    @Override
    public long size() throws IOException {
        return mSize;
    }

    @Override
    public SeekableByteChannel truncate(long l) throws IOException {
        LOGGER.trace("truncate()");
        return null;
    }

    @Override
    public boolean isOpen() {
        LOGGER.trace("isOpen()");
        return mIsOpen;
    }

    @Override
    public void close() throws IOException {
        LOGGER.trace("close()");
        mIsOpen = false;
        mFSStore.onFileChannelClose(this);
    }
}
