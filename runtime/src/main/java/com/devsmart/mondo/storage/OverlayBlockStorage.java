package com.devsmart.mondo.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;


public class OverlayBlockStorage implements BlockStorage {


    //private ArrayList<BlockIndex> mLayerIndex;
    private ArrayList<BlockStorage> mLayersStorage;

    @Override
    public void readBlock(long offset, ByteBuffer buffer) throws IOException {

    }

    @Override
    public void writeBlock(long offset, ByteBuffer buffer) throws IOException {

    }
}
