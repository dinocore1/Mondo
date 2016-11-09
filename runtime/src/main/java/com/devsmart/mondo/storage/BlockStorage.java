package com.devsmart.mondo.storage;


import java.io.IOException;
import java.nio.ByteBuffer;

public interface BlockStorage {

    void readBlock(long offset, ByteBuffer buffer) throws IOException;
    void writeBlock(long offset, ByteBuffer buffer) throws IOException;

}
