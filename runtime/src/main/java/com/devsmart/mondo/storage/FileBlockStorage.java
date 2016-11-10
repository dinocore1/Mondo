package com.devsmart.mondo.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;


public class FileBlockStorage implements BlockStorage, Closeable {


    private final File mFile;
    private final FileChannel mChannel;

    public FileBlockStorage(File f) throws IOException {
        mFile = f;
        mChannel = new RandomAccessFile(f, "rw").getChannel();
    }

    @Override
    public int readBlock(long offset, ByteBuffer buffer) throws IOException {
        return mChannel.read(buffer, offset);
    }

    @Override
    public int writeBlock(long offset, ByteBuffer buffer) throws IOException {
        return mChannel.write(buffer, offset);
    }

    @Override
    public void close() throws IOException {
        mChannel.close();
    }
}
