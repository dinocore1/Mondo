package com.devsmart.mondo.storage;


import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;

public class FileMetadata {

    public static final int FLAG_DIR = 0x400;
    public static final int FLAG_EXECUTE = 0x1;
    public static final int FLAG_WRITE = 0x2;
    public static final int FLAG_READ = 0x4;

    public int mFlags;
    public long mDataFileId;
    public long mSize;

    public boolean isDirectory() {
        return (mFlags & FLAG_DIR) > 0;
    }

    public long getSize() {
        return mSize;
    }

    static final GroupSerializerObjectArray<FileMetadata> SERIALIZER = new GroupSerializerObjectArray<FileMetadata>() {

        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull FileMetadata value) throws IOException {
            out.packInt(value.mFlags);
            out.packLong(value.mSize);
            out.packLong(value.mDataFileId);
        }

        @Override
        public FileMetadata deserialize(@NotNull DataInput2 input, int available) throws IOException {
            FileMetadata retval = new FileMetadata();
            retval.mFlags = input.unpackInt();
            retval.mSize = input.unpackLong();
            retval.mDataFileId = input.unpackLong();
            return retval;
        }

        @Override
        public int compare(FileMetadata a, FileMetadata b) {
            return Long.compare(a.mDataFileId, b.mDataFileId);
        }
    };
}
