package com.devsmart.mondo.storage;


import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;

public class VirtualFile {

    public static final int FLAG_DIR = 0x400;
    public static final int FLAG_EXECUTE = 0x1;
    public static final int FLAG_WRITE = 0x2;
    public static final int FLAG_READ = 0x4;

    String mName;
    int mFlags;
    long mDataFileId;


    public static VirtualFile createDir(String name) {
        VirtualFile retval = new VirtualFile();
        retval.mName = name;
        retval.mFlags = FLAG_DIR | FLAG_READ | FLAG_EXECUTE;
        return retval;
    }

    public static VirtualFile createFile(String name, long datafileId) {
        VirtualFile retval = new VirtualFile();
        retval.mName = name;
        retval.mFlags = FLAG_READ | FLAG_WRITE;
        retval.mDataFileId = datafileId;
        return retval;
    }

    public boolean isDirectory() {
        return (mFlags & FLAG_DIR) > 0;
    }

    public boolean isFile() {
        return (mFlags & FLAG_DIR) == 0;
    }

    public String getName() {
        return mName;
    }



}
