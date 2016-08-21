package com.devsmart.mondo.data;


import co.paralleluniverse.fuse.DirectoryFiller;
import co.paralleluniverse.fuse.StructFuseFileInfo;
import com.devsmart.mondo.storage.FileMetadata;
import com.devsmart.mondo.storage.VirtualFile;
import com.devsmart.mondo.storage.VirtualFilesystem;
import co.paralleluniverse.fuse.*;
import co.paralleluniverse.fuse.AbstractFuseFilesystem;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class FUSEVirtualFilesystem extends AbstractFuseFilesystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(FUSEVirtualFilesystem.class);


    private final VirtualFilesystem mVirtualFS;

    public FUSEVirtualFilesystem(VirtualFilesystem virtualFilesystem) {
        mVirtualFS = virtualFilesystem;
    }

    @Override
    protected synchronized int readdir(String path, StructFuseFileInfo info, DirectoryFiller filler) {
        LOGGER.info("readdir {}", path);

        if(!"/".equals(path) && !mVirtualFS.pathExists(path)) {
            return -ErrorCodes.ENOENT();
        }

        Iterable<String> files = mVirtualFS.getFilesInDir(path);
        filler.add(files);

        return 0;
    }

    @Override
    protected synchronized int getattr(String path, StructStat stat) {
        LOGGER.info("getattr {}", path);
        if("/".equals(path)) {
            stat.mode(TypeMode.S_IFDIR | 0775);
            return 0;
        }

        FileMetadata file = mVirtualFS.getFile(path);
        if(file == null) {
            return -ErrorCodes.ENOENT();
        } else {
            if(file.isDirectory()) {
                stat.mode(TypeMode.S_IFDIR | 0755);
            } else {
                stat.mode(TypeMode.S_IFREG | 0644);
                stat.size(file.getSize());
            }
            return 0;
        }
    }

    @Override
    protected synchronized int mkdir(String path, long mode) {
        LOGGER.info("mkdir {} {}", path, mode);
        mVirtualFS.mkdir(path);
        return 0;
    }

    @Override
    protected synchronized int rmdir(String path) {
        LOGGER.info("rmdir {}", path);
        mVirtualFS.rmdir(path);
        return 0;
    }

    @Override
    protected synchronized int mknod(String path, long mode, long dev) {
        LOGGER.info("mknod {} {}", path, mode);
        mVirtualFS.mknod(path);
        return 0;
    }

    @Override
    protected synchronized int open(String path, StructFuseFileInfo info) {
        LOGGER.info("open {}", path);
        FileMetadata file = mVirtualFS.getFile(path);
        if(file == null) {
            return -ErrorCodes.ENOENT();
        } else {

        }
        return 0;
    }

    @Override
    protected synchronized int release(String path, StructFuseFileInfo info) {
        LOGGER.info("release {}", path);
        return 0;
    }

    @Override
    protected int write(String path, ByteBuffer buf, long bufSize, long writeOffset, StructFuseFileInfo wrapper) {
        LOGGER.info("write {} {} {}", path, bufSize, writeOffset);
        int bytesWritten = mVirtualFS.write(path, buf, bufSize, writeOffset);
        return bytesWritten;
    }

    @Override
    protected int read(String path, ByteBuffer buffer, long size, long offset, StructFuseFileInfo info) {
        LOGGER.info("read {} {} {}", path, size, offset);

        int bytesRead = mVirtualFS.read(path, buffer, size, offset);
        return bytesRead;
    }

    @Override
    protected synchronized int unlink(String path) {
        LOGGER.info("unlink {}", path);
        FileMetadata file = mVirtualFS.getFile(path);
        if(file == null) {
            return -ErrorCodes.ENOENT();
        } else {
            mVirtualFS.unlink(path);
            return 0;
        }
    }
}
