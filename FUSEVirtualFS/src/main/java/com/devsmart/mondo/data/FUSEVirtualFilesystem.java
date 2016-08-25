package com.devsmart.mondo.data;


import co.paralleluniverse.fuse.*;
import com.devsmart.mondo.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class FUSEVirtualFilesystem extends AbstractFuseFilesystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(FUSEVirtualFilesystem.class);


    private final VirtualFilesystem mVirtualFS;
    private final HashMap<String, VirtualFile> mOpenFiles = new HashMap<String, VirtualFile>();
    private final FileHandlePool mFileHandles = new FileHandlePool(200);
    private final FilesystemStorage mFilesystemStorage;
    private final LinkedList<VirtualFile> mFlushQueue = new LinkedList<VirtualFile>();

    public FUSEVirtualFilesystem(VirtualFilesystem virtualFilesystem, FilesystemStorage storage) {
        mVirtualFS = virtualFilesystem;
        mFilesystemStorage = storage;
    }

    private void addToFlushQueue(VirtualFile vfile) {
        synchronized(mFlushQueue) {
            mFlushQueue.offer(vfile);
            mFlushQueue.notify();
        }
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

        FileMetadata metadata = mVirtualFS.getFile(path);
        if(metadata == null) {
            return -ErrorCodes.ENOENT();
        } else {
            if(metadata.isDirectory()) {
                stat.mode(TypeMode.S_IFDIR | 0755);
            } else {
                stat.mode(TypeMode.S_IFREG | 0644);
                VirtualFile vfile = mOpenFiles.get(path);
                if(vfile != null) {
                    stat.size(vfile.getSize());
                } else {
                    stat.size(metadata.getSize());
                }
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
        FileMetadata metadata = mVirtualFS.getFile(path);
        if(metadata == null || metadata.isDirectory()) {
            return -ErrorCodes.ENOENT();
        } else {
            VirtualFile vfile = mOpenFiles.get(path);
            if(vfile == null) {
                try {
                    vfile = new VirtualFile(mFilesystemStorage);
                    vfile.mMetadata = metadata;
                    vfile.setDataFile(mVirtualFS.mDataObjects.get(vfile.mMetadata.mDataFileId));
                    vfile.mPath = mVirtualFS.mPathPool.borrowObject();
                    vfile.mPath.setFilepath(path);
                    vfile.mHandle = mFileHandles.allocate();
                    info.fh(vfile.mHandle);
                    mOpenFiles.put(path, vfile);
                } catch (Exception e) {
                    LOGGER.error("", e);
                    return -ErrorCodes.ENFILE();
                }
            }
        }
        return 0;
    }

    @Override
    protected synchronized int release(String path, StructFuseFileInfo info) {
        LOGGER.info("release {}", path);

        try {
            VirtualFile vfile = mOpenFiles.get(path);
            addToFlushQueue(vfile);
            return 0;
        } catch (Exception e) {
            LOGGER.error("", e);
        }

        return 0;
    }

    @Override
    protected int write(String path, ByteBuffer buf, long bufSize, long writeOffset, StructFuseFileInfo wrapper) {
        LOGGER.info("write {} {} {}", path, bufSize, writeOffset);
        VirtualFile vfile = mOpenFiles.get(path);
        if(vfile == null) {
            return -ErrorCodes.ENOENT();
        }
        try {
            int bytesWritten = vfile.write(buf, bufSize, writeOffset);
            return bytesWritten;
        } catch (Exception e) {
            LOGGER.error("", e);
            return 0;
        }
    }

    @Override
    protected int read(String path, ByteBuffer buffer, long size, long offset, StructFuseFileInfo info) {
        LOGGER.info("read {} {} {}", path, size, offset);
        VirtualFile vfile = mOpenFiles.get(path);
        if(vfile == null) {
            return -ErrorCodes.ENOENT();
        }
        try {
            int bytesRead = vfile.read(buffer, size, offset);
            return bytesRead;
        } catch (Exception e) {
            LOGGER.error("", e);
            return 0;
        }
    }

    @Override
    protected int truncate(String path, long size) {
        LOGGER.info("truncate {} {}", path, size);
        VirtualFile vfile = mOpenFiles.get(path);
        if(vfile == null) {
            return -ErrorCodes.ENOENT();
        }
        try {
            vfile.truncate(size);
            return 0;
        } catch (IOException e) {
            LOGGER.error("", e);
            return 0;
        }
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
