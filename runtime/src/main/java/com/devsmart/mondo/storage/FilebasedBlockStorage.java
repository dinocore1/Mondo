package com.devsmart.mondo.storage;


import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.Closeable;
import java.io.File;

public class FilebasedBlockStorage implements Closeable {

    private static final String NAME_BLOCKINDEX = "blockIndex";

    private final File mFile;
    private final DB mDB;
    private final BTreeMap<Long, byte[]> mBlockIndex;

    public FilebasedBlockStorage(File file) {
        mFile = file;
        mDB = DBMaker.fileDB(file)
                .make();

        mBlockIndex = mDB.treeMap(NAME_BLOCKINDEX, Serializer.LONG, Serializer.BYTE_ARRAY)
                .valuesOutsideNodesEnable()
                .createOrOpen();
    }

    @Override
    public void close() {
        mBlockIndex.close();
        mDB.close();
    }

    public void delete() {
        mFile.delete();
    }

    public byte[] get(long index) {
        return mBlockIndex.get(index);
    }

    public void put(long index, byte[] block) {
        mBlockIndex.put(index, block);
    }

    public boolean hasBlock(long blockIndex) {
        return mBlockIndex.containsKey(blockIndex);
    }
}
