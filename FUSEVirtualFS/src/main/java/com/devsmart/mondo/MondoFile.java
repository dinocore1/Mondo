package com.devsmart.mondo;

import com.google.common.collect.ComparisonChain;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

public class MondoFile implements BasicFileAttributes {

    long lastModifiedTime;
    long lastAccessedTime;
    long creationTime;
    boolean isFile;
    public long size;

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
        return isFile;
    }

    @Override
    public boolean isDirectory() {
        return !isFile;
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

    public static final Serializer<MondoFile> SERIALIZER = new Serializer<MondoFile>() {

        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull MondoFile value) throws IOException {
            out.packLong(value.lastAccessedTime);
            out.packLong(value.lastModifiedTime);
            out.packLong(value.creationTime);
            out.writeBoolean(value.isFile);
        }

        @Override
        public MondoFile deserialize(@NotNull DataInput2 input, int available) throws IOException {
            MondoFile retval = new MondoFile();
            retval.lastAccessedTime = input.unpackLong();
            retval.lastModifiedTime = input.unpackLong();
            retval.creationTime = input.unpackLong();
            retval.isFile = input.readBoolean();
            return retval;
        }

        @Override
        public int compare(MondoFile first, MondoFile second) {
            return ComparisonChain.start()
                    .compareTrueFirst(first.isDirectory(), second.isFile)
                    .result();
        }
    };
}
