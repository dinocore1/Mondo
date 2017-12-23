package com.devsmart.mondo;


import com.devsmart.mondo.storage.SparseArray;
import com.google.common.base.Throwables;
import com.google.common.hash.HashCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class MondoFileChannel implements SeekableByteChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(MondoFileChannel.class);

    public static final int MODE_READ = 0x1;
    public static final int MODE_WRITE = 0x2;

    int mOpenMode;
    final FileChannel mScratchFile;
    final FileMetadata mMetadata;
    final MondoFileStore mFSStore;

    private BlockGroup mBlockGroup;
    private long[] mBlockGroupIndex;

    private long mPosition;
    private long mSize;
    private SparseArray<Long> mBufferIndex = new SparseArray<Long>(10);
    private ByteBuffer mBuffer = ByteBuffer.allocate(MondoFileStore.BUFFER_SIZE);
    private boolean mIsBufferDirty;
    private int mBufferNum = -1;
    private boolean mIsOpen;

    MondoFSPath mPath;

    MondoFileChannel(int openMode, FileChannel scratchFile, FileMetadata metadata, MondoFileStore store) {
        mOpenMode = openMode;
        mScratchFile = scratchFile;
        mMetadata = metadata;
        mFSStore = store;
        mSize = metadata.size;
        mBlockGroup = mFSStore.mBlockGroups.get(mMetadata.blockId);
        mIsOpen = true;
    }

    private void readBlockGroup() throws IOException {
        mBlockGroupIndex = new long[mBlockGroup.blocks.size()+1];
        long pos = 0;
        int i = 0;
        for(HashCode hashCode : mBlockGroup.blocks){
            File blockFile = mFSStore.getFileBlock(hashCode);
            if(!blockFile.exists() || !blockFile.isFile()) {
                throw new IOException("missing block: " + hashCode.toString());
            }


            mBlockGroupIndex[i++] = pos;
            pos += blockFile.length();

        }

        mBlockGroupIndex[i] = pos;
    }

    @Override
    public synchronized int read(ByteBuffer byteBuffer) throws IOException {
        try {
            LOGGER.trace("read()");

            if ((mOpenMode & MODE_READ) == 0) {
                throw new IOException("no read permission");
            }

            syncBuffer();

            int bytesRead = Math.min(mBuffer.remaining(), byteBuffer.remaining());
            for(int i=0;i<bytesRead;i++) {
                byteBuffer.put(mBuffer.get());
            }

            mPosition += bytesRead;

            LOGGER.trace("bytesRead: {}", bytesRead);
            return bytesRead;
        } catch (Exception e) {
            LOGGER.error("", e);
            Throwables.propagate(e);
            return -1;
        }
    }

    @Override
    public synchronized int write(ByteBuffer byteBuffer) throws IOException {
        LOGGER.trace("write()");

        if((mOpenMode & MODE_WRITE) == 0) {
            throw new IOException("no write permission");
        }

        syncBuffer();

        int bytesWritten = Math.min(mBuffer.remaining(), byteBuffer.remaining());
        for(int i=0;i<bytesWritten;i++) {
            mBuffer.put(byteBuffer.get());
        }

        mPosition += bytesWritten;
        if(mPosition > mSize) {
            mSize = mPosition;
        }

        mIsBufferDirty = true;

        return bytesWritten;
    }

    private void flushBuffer() throws IOException {
        if(mIsBufferDirty) {
            Long pos = mBufferIndex.get(mBufferNum);
            if (pos == null) {
                pos = mScratchFile.size();
                mBufferIndex.put(mBufferNum, pos);
            }
            mScratchFile.position(pos);

            mBuffer.flip();

            do {
                mScratchFile.write(mBuffer);
            } while (mBuffer.remaining() > 0);

            mBuffer.clear();
            mIsBufferDirty = false;
        }
    }

    @Override
    public synchronized long position() throws IOException {
        LOGGER.trace("position() {}", mPosition);
        return mPosition;
    }

    @Override
    public synchronized SeekableByteChannel position(long l) throws IOException {
        LOGGER.trace("position({})", l);
        mPosition = l;
        return this;
    }

    private void syncBuffer() throws IOException {
        int newBufferIndex = (int) (mPosition / mBuffer.capacity());
        int offset = (int) (mPosition % mBuffer.capacity());

        if(mBufferNum != newBufferIndex) {
            flushBuffer();
            loadBuffer(newBufferIndex);
        }

        mBuffer.position(offset);
    }

    private void loadBuffer(int bufferNum) throws IOException {
        Long pos = mBufferIndex.get(bufferNum);
        if(pos != null) {
            mBuffer.clear();
            mBuffer.limit((int) Math.min(mBuffer.capacity(), mSize - pos));

            do {
                mScratchFile.read(mBuffer, pos);
            } while(mBuffer.remaining() > 0);

            mBuffer.flip();

        } else if(mBlockGroup != null) {
            loadBufferFromBlockGroup(bufferNum);
        }

        mBufferNum = bufferNum;

    }

    private void loadBufferFromBlockGroup(int bufferNum) throws IOException {
        FileChannel fc;
        if(mBlockGroupIndex == null) {
            readBlockGroup();
        }

        long pos = bufferNum * mBuffer.capacity();

        mBuffer.clear();
        mBuffer.limit((int) Math.min(mBuffer.capacity(), mSize - pos));


        while(mBuffer.remaining() > 0) {


            int i = Arrays.binarySearch(mBlockGroupIndex, pos);
            if(i < 0) {
                i = -i - 1;
            }

            i = Math.max(0, i-1);

            File blockFile = mFSStore.getFileBlock(mBlockGroup.blocks.get(i));
            fc = FileChannel.open(blockFile.toPath(), StandardOpenOption.READ);

            long offset = pos - mBlockGroupIndex[i];
            int bytesRead = fc.read(mBuffer, offset);
            fc.close();

            pos += bytesRead;
        }

        mBuffer.flip();

    }

    @Override
    public synchronized long size() throws IOException {
        LOGGER.trace("size() {}", mSize);
        return mSize;
    }

    @Override
    public synchronized SeekableByteChannel truncate(long l) throws IOException {
        LOGGER.trace("truncate()");
        return null;
    }

    @Override
    public synchronized boolean isOpen() {
        LOGGER.trace("isOpen()");
        return mIsOpen;
    }

    @Override
    public synchronized void close() throws IOException {
        LOGGER.trace("close()");
        mIsOpen = false;
        mMetadata.size = mSize;
        mFSStore.onFileChannelClose(this);
    }
}
