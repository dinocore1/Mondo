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

    public int mHandle;
    public int mOpenMode;

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

            @Override
            public void close() throws IOException {

            }
        };
    }

    public void open(int openMode) {
        mOpenMode = openMode;
    }


    @Override
    public void close() throws IOException {
        //TODO: implement
    }

    public synchronized void fsync() throws IOException {
        //TODO: implement

    }


    public synchronized int write(ByteBuffer inputBuffer, long size, long offset) throws IOException {



        //TODO: implement
        return 0;

    }


    public synchronized int read(ByteBuffer buffer, long size, long offset) throws IOException {
        Block<BlockStorage> block = mBlockSet.getBlockContaining(offset);
        if(block != null) {
            BlockStorage storage = block.continer;
            int bytesRead = storage.readBlock(block.secondaryOffset, buffer);
            return bytesRead;
        } else {
            final String msg = String.format("no storage for offset: %d", offset);
            LOGGER.error(msg);
            throw new IOException(msg);
        }
    }

    public synchronized void truncate(long size) throws IOException {
        //TODO:
    }

}
