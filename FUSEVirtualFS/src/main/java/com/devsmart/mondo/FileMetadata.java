package com.devsmart.mondo;

import com.google.common.collect.ComparisonChain;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

public class FileMetadata implements BasicFileAttributes {

    public static final int FLAG_DIR = 0x400;
    public static final int FLAG_EXECUTE = 0x1;
    public static final int FLAG_WRITE = 0x2;
    public static final int FLAG_READ = 0x4;

    long blockId;
    long lastModifiedTime;
    long lastAccessedTime;
    long creationTime;
    int flags;
    long size;

    @Override
    public FileTime lastModifiedTime() {
        return FileTime.fromMillis(lastModifiedTime);
    }

    @Override
    public FileTime lastAccessTime() {
        return FileTime.fromMillis(lastAccessedTime);
    }

    @Override
    public FileTime creationTime() {
        return FileTime.fromMillis(creationTime);
    }

    @Override
    public boolean isRegularFile() {
        return (flags & FLAG_DIR) == 0;
    }

    @Override
    public boolean isDirectory() {
        return (flags & FLAG_DIR) > 0;
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public Object fileKey() {
        return null;
    }

    public static final GroupSerializerObjectArray<FileMetadata> SERIALIZER = new GroupSerializerObjectArray<FileMetadata>() {

        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull FileMetadata value) throws IOException {
            out.packLong(value.blockId);
            out.packLong(value.lastAccessedTime);
            out.packLong(value.lastModifiedTime);
            out.packLong(value.creationTime);
            out.packLong(value.size);
            out.packInt(value.flags);
        }

        @Override
        public FileMetadata deserialize(@NotNull DataInput2 input, int available) throws IOException {
            FileMetadata retval = new FileMetadata();
            retval.blockId = input.unpackLong();
            retval.lastAccessedTime = input.unpackLong();
            retval.lastModifiedTime = input.unpackLong();
            retval.creationTime = input.unpackLong();
            retval.size = input.unpackLong();
            retval.flags = input.unpackInt();
            return retval;
        }

        @Override
        public int compare(FileMetadata a, FileMetadata b) {
            return ComparisonChain.start()
                    .compareTrueFirst(a.isDirectory(), b.isDirectory())
                    .result();
        }
    };
}
