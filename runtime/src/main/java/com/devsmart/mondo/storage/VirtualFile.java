package com.devsmart.mondo.storage;


import com.devsmart.mondo.data.Block;
import com.devsmart.mondo.data.BlockSet;
import com.devsmart.mondo.kademlia.ID;
import com.google.common.base.Preconditions;
import com.google.common.cache.*;
import com.google.common.hash.HashCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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

    private final ByteBuffer mLoadingByteBuffer = ByteBuffer.allocate(BLOCK_SIZE);

    private final LoadingCache<Long, BlockStorage> mBlockCache = CacheBuilder.newBuilder()
            .removalListener(new RemovalListener<Long, BlockStorage>() {
                @Override
                public void onRemoval(RemovalNotification<Long, BlockStorage> notification) {

                }
            })
            .maximumSize(50)
            .build(new CacheLoader<Long, BlockStorage>() {
                @Override
                public BlockStorage load(Long key) throws Exception {
                    MemoryBlockStorage retval = new MemoryBlockStorage(BLOCK_SIZE);
                    long offset = key * BLOCK_SIZE;

                    synchronized (mLoadingByteBuffer) {
                        mLoadingByteBuffer.clear();

                        boolean hasData = false;
                        Block<BlockStorage> block;
                        while ((block = mBlockSet.getBlockContaining(offset)) != null &&
                                block.containsOffset(offset) &&
                                mLoadingByteBuffer.remaining() > 0) {
                            final long readOffset = offset - block.offset;
                            offset += block.continer.readBlock(readOffset, mLoadingByteBuffer);
                            hasData = true;
                        }

                        if(hasData) {
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


    public VirtualFile(VirtualFilesystem virtualFS, FilesystemStorage storage, VirtualFilesystem.Path path, FileMetadata metadata) {
        Preconditions.checkArgument(virtualFS != null && storage != null);
        mVirtualFS = virtualFS;
        mFilesystemStorage = storage;
        mPath = path;
        mMetadata = metadata;

        setDataFile(mVirtualFS.mDataObjects.get(metadata.mDataFileId));
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
        //TODO: implement

        fsync();
    }

    public synchronized void fsync() throws IOException {
        //TODO: implement

        mBlockCache.invalidateAll();

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
