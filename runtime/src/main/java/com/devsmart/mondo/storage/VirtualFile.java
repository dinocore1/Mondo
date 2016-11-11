package com.devsmart.mondo.storage;


import com.devsmart.mondo.data.DataStreamBreaker;
import com.devsmart.mondo.data.SecureSegment;
import com.devsmart.mondo.kademlia.ID;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

public class VirtualFile implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualFile.class);

    public static final int BLOCK_SIZE = 4096;



    private final FilesystemStorage mFilesystemStorage;
    private final VirtualFilesystem mVirtualFS;
    public final VirtualFilesystem.Path mPath;
    public final FileMetadata mMetadata;
    private DataFile mDataFile;

    public int mHandle;
    public int mOpenMode;

    private FilebasedBlockStorage mTmpBlockStorage;

    private final ByteBuffer mLoadingByteBuffer = ByteBuffer.allocate(BLOCK_SIZE);

    private boolean loadFromTempBlockStorage(long blockIndex, ByteBuffer buffer) {
        byte[] block = mTmpBlockStorage.get(blockIndex);
        if(block != null) {
            buffer.put(block, 0, BLOCK_SIZE);
            return true;
        } else {
            return false;
        }
    }

    private boolean loadFromLongtermStorage(long blockIndex, ByteBuffer buffer) throws IOException {
        if(mDataFile == null || mDataFile.mParts == null) {
            return false;
        }
        boolean hasData = false;
        long readOffset = blockIndex*BLOCK_SIZE;
        long dataStart = 0;
        int i = 0;
        while(buffer.hasRemaining() && i < mDataFile.mParts.length) {
            FilePart dataPart = mDataFile.mParts[i++];
            final long dataEnd = dataStart + dataPart.getSize();
            if(dataStart <= readOffset && readOffset < dataEnd) {
                FileBlockStorage fbs = new FileBlockStorage(mFilesystemStorage.getFile(dataPart.getID()));
                readOffset += fbs.readBlock(readOffset - dataStart, buffer);
                fbs.close();
                hasData = true;
            }
            dataStart = dataEnd;
        }

        return hasData;
    }

    private final LoadingCache<Long, BlockStorage> mBlockCache = CacheBuilder.newBuilder()
            .removalListener(new RemovalListener<Long, BlockStorage>() {
                @Override
                public void onRemoval(RemovalNotification<Long, BlockStorage> notification) {
                    final long blockIndex = notification.getKey();

                    try {
                        synchronized (mLoadingByteBuffer) {
                            mLoadingByteBuffer.clear();

                            notification.getValue().readBlock(0, mLoadingByteBuffer);
                            assert(mLoadingByteBuffer.limit() == BLOCK_SIZE);
                            assert(mLoadingByteBuffer.hasArray());
                            mTmpBlockStorage.put(blockIndex, mLoadingByteBuffer.array());
                        }
                    } catch (IOException e) {
                        LOGGER.error("", e);
                        Throwables.propagate(e);
                    }

                }
            })
            .maximumSize(50)
            .build(new CacheLoader<Long, BlockStorage>() {
                @Override
                public BlockStorage load(Long key) throws Exception {
                    MemoryBlockStorage retval = new MemoryBlockStorage(BLOCK_SIZE);

                    synchronized (mLoadingByteBuffer) {
                        mLoadingByteBuffer.clear();

                        if(loadFromTempBlockStorage(key, mLoadingByteBuffer) || loadFromLongtermStorage(key, mLoadingByteBuffer)) {
                            mLoadingByteBuffer.flip();
                            retval.writeBlock(0, mLoadingByteBuffer);
                        }

                    }

                    return retval;
                }
            });


    public VirtualFile(VirtualFilesystem virtualFS, FilesystemStorage storage, VirtualFilesystem.Path path, FileMetadata metadata) throws IOException {
        Preconditions.checkArgument(virtualFS != null && storage != null);
        mVirtualFS = virtualFS;
        mFilesystemStorage = storage;
        mPath = path;
        mMetadata = metadata;
        mDataFile = mVirtualFS.mDataObjects.get(metadata.mDataFileId);

        mTmpBlockStorage = new FilebasedBlockStorage(mFilesystemStorage.createTempFile());
    }

    public boolean isDirectory() {
        return (mMetadata.mFlags & FileMetadata.FLAG_DIR) > 0;
    }

    public boolean isFile() {
        return (mMetadata.mFlags & FileMetadata.FLAG_DIR) == 0;
    }

    public String getName() {
        return mPath.getName();
    }

    public long getSize() {
        return mMetadata.getSize();
    }

    public void open(int openMode) {
        mOpenMode = openMode;
    }


    @Override
    public void close() throws IOException {
        fsync();
        mTmpBlockStorage.close();
        mTmpBlockStorage.delete();
    }

    public static long getNumBlocks(long size, long blockSize) {
        return ((blockSize-1) + size) / blockSize;
    }

    public long size() {
        return mMetadata.getSize();
    }

    private class FSyncTask implements DataStreamBreaker.Callback {

        private InputStream mInput;
        private DataStreamBreaker mBreaker;
        private File mCurrentOutputFile;

        public FSyncTask() {
            mInput = createInputStream();
            mBreaker = new DataStreamBreaker(Hashing.sha1(), 16);
            mBreaker.setCallback(this);
        }

        public void run() throws IOException {
            Iterable<FilePart> fileparts = Iterables.transform(mBreaker.getSegments(mInput), new Function<SecureSegment, FilePart>() {
                @Override
                public FilePart apply(SecureSegment input) {
                    return new FilePart((int) input.length, input.secureHash.asBytes());
                }
            });

            DataFile dataFile = new DataFile();
            dataFile.mParts = Iterables.toArray(fileparts, FilePart.class);

            mVirtualFS.mDataObjects.put(mMetadata.mDataFileId, dataFile);
            mDataFile = dataFile;
        }

        @Override
        public OutputStream createOutputStream() {
            try {
                mCurrentOutputFile = mFilesystemStorage.createTempFile();
                return new FileOutputStream(mCurrentOutputFile);
            } catch (IOException e) {
                LOGGER.error("", e);
                Throwables.propagate(e);
                return null;
            }
        }

        @Override
        public void onNewSegment(SecureSegment segment) {
            LOGGER.info("segment {}", segment);

            synchronized (mFilesystemStorage) {
                File dest = mFilesystemStorage.getFile(segment.getID());
                if (!dest.exists()) {
                    dest.getParentFile().mkdirs();
                    mCurrentOutputFile.renameTo(dest);
                    mCurrentOutputFile = null;
                } else {
                    LOGGER.info("segment already exists: {}", segment);
                }
            }

        }
    }

    public synchronized void fsync() throws IOException {
        new FSyncTask().run();

    }

    public InputStream createInputStream() {
        return new VirtualFileInputStream(this);
    }


    public synchronized int write(ByteBuffer buffer, long size, long offset) throws IOException {
        try {
            int bytesWritten = 0;

            while (size > 0) {
                final long blockIndex = offset / BLOCK_SIZE;
                BlockStorage storage = mBlockCache.get(blockIndex);
                long readOffset = offset - (blockIndex * BLOCK_SIZE);
                int wrote = storage.writeBlock(readOffset, buffer);
                bytesWritten += wrote;
                offset += wrote;
                size -= wrote;
            }

            final long newSize = Math.max(mMetadata.mSize, offset + size);
            if(mMetadata.mSize != newSize) {
                mMetadata.mSize = newSize;
                mVirtualFS.updateFileMetadata(mPath, mMetadata);
            }


            return bytesWritten;
        } catch (Exception e) {
            LOGGER.error("error writing to offset {}", offset, e);
            throw new IOException(e);
        }

    }


    public synchronized int read(ByteBuffer buffer, long size, long offset) throws IOException {
        try {
            int bytesRead = 0;

            while (size > 0) {
                final long blockIndex = offset / BLOCK_SIZE;
                BlockStorage storage = mBlockCache.get(blockIndex);
                long readOffset = offset - (blockIndex * BLOCK_SIZE);
                int read = storage.readBlock(readOffset, buffer);
                bytesRead += read;
                offset += read;
                size -= read;
            }


            return bytesRead;
        } catch (Exception e) {
            LOGGER.error("error reading from offset {}", offset, e);
            throw new IOException(e);
        }
    }

    public synchronized void truncate(long size) throws IOException {
        //TODO:
    }

}
