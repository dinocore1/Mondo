package com.devsmart.mondo.storage;


import com.devsmart.IOUtils;
import com.devsmart.mondo.data.*;
import com.devsmart.mondo.kademlia.ID;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;
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

    private final BlockSet<BlockStorage> mBlockSet = new BlockSet<BlockStorage>();
    private final SparseArray<BlockStorage> mBlocks = new SparseArray<BlockStorage>();

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
        long offset = blockIndex * BLOCK_SIZE;
        boolean hasData = false;

        Block<BlockStorage> block;
        while ((block = mBlockSet.getBlockContaining(offset)) != null &&
                block.containsOffset(offset) &&
                mLoadingByteBuffer.hasRemaining()) {
            final long readOffset = offset - block.offset;
            offset += block.continer.readBlock(readOffset, buffer);
            hasData = true;
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

    private final LoadingCache<HashCode, FileBlockStorage> mFileBlockStorageCache = CacheBuilder.newBuilder()
            .removalListener(new RemovalListener<HashCode, FileBlockStorage>() {
                @Override
                public void onRemoval(RemovalNotification<HashCode, FileBlockStorage> notification) {
                    try {
                        notification.getValue().close();
                    } catch (IOException e) {
                        LOGGER.error("error closing file", e);
                    }
                }
            })
            .maximumSize(10)
            .build(new CacheLoader<HashCode, FileBlockStorage>() {
        @Override
        public FileBlockStorage load(HashCode key) throws Exception {
            byte[] keybytes = key.asBytes();
            File f = mFilesystemStorage.getFile(new ID(keybytes, keybytes.length));
            return new FileBlockStorage(f);
        }
    });


    public VirtualFile(VirtualFilesystem virtualFS, FilesystemStorage storage, VirtualFilesystem.Path path, FileMetadata metadata) throws IOException {
        Preconditions.checkArgument(virtualFS != null && storage != null);
        mVirtualFS = virtualFS;
        mFilesystemStorage = storage;
        mPath = path;
        mMetadata = metadata;

        setDataFile(mVirtualFS.mDataObjects.get(metadata.mDataFileId));
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
        return mBlockSet.getLast().end();
    }

    public void setDataFile(DataFile datafile) {
        long offset = 0;
        if(datafile != null && datafile.mParts != null && datafile.mParts.length >= 0) {
            for (int i = 0; i < datafile.mParts.length; i++) {
                FilePart part = datafile.mParts[i];
                final long size = part.getSize();
                Block<BlockStorage> block = new Block<BlockStorage>(offset, (int) size, 0, createBlockStorageWrapper(part.getSha1Checksum()));
                mBlockSet.add(block);
                offset += size;
            }
        }
    }

    private BlockStorage createBlockStorageWrapper(final HashCode sha1Checksum) {
        return new BlockStorage() {
            @Override
            public int readBlock(long offset, ByteBuffer buffer) throws IOException {
                try {
                    FileBlockStorage storage = mFileBlockStorageCache.get(sha1Checksum);
                    return storage.readBlock(offset, buffer);
                } catch (ExecutionException e) {
                    LOGGER.error("", e);
                    throw new IOException(e);
                }
            }

            @Override
            public int writeBlock(long offset, ByteBuffer buffer) throws IOException {
                try {
                    FileBlockStorage storage = mFileBlockStorageCache.get(sha1Checksum);
                    return storage.writeBlock(offset, buffer);
                } catch (ExecutionException e) {
                    LOGGER.error("", e);
                    throw new IOException(e);
                }
            }
        };
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
            mBreaker = new DataStreamBreaker(Hashing.sha1(), 40);
            mBreaker.setCallback(this);
        }

        public void run() throws IOException {
            Iterable<SecureSegment> segments = mBreaker.getSegments(mInput);
            //TODO: update

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

            File dest = mFilesystemStorage.getFile(segment.getID());
            mCurrentOutputFile.renameTo(dest);

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
