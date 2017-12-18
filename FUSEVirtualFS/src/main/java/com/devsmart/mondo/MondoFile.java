package com.devsmart.mondo;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

public class MondoFile implements BasicFileAttributes {

    long lastModifiedTime;
    long lastAccessedTime;
    long creationTime;

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
        return false;
    }

    @Override
    public boolean isDirectory() {
        return false;
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
        return 0;
    }

    @Override
    public Object fileKey() {
        return null;
    }
}
