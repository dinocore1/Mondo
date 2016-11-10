package com.devsmart.mondo.storage;


import java.io.IOException;
import java.nio.ByteBuffer;

public interface BlockStorage {

    int readBlock(long offset, ByteBuffer buffer) throws IOException;
    int writeBlock(long offset, ByteBuffer buffer) throws IOException;

}
