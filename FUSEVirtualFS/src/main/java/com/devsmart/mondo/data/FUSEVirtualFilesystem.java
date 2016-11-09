package com.devsmart.mondo.data;


import co.paralleluniverse.fuse.*;
import com.devsmart.mondo.storage.*;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class FUSEVirtualFilesystem extends AbstractFuseFilesystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(FUSEVirtualFilesystem.class);


    private final VirtualFilesystem mVirtualFS;
    private final HashMap<Long, VirtualFile> mOpenFiles = new HashMap<Long, VirtualFile>();
    private long mFilehandleId = 0;
    private final FilesystemStorage mFilesystemStorage;
    private final LinkedList<VirtualFile> mFlushQueue = new LinkedList<VirtualFile>();
    private final ScheduledExecutorService mScheduledIO = Executors.newScheduledThreadPool(3);

    public void shutdown() {
        mScheduledIO.shutdown();
    }


    public FUSEVirtualFilesystem(VirtualFilesystem virtualFilesystem, FilesystemStorage storage) {
        mVirtualFS = virtualFilesystem;
        mFilesystemStorage = storage;
    }

    private void addToFlushQueue(VirtualFile vfile) {
        Preconditions.checkArgument(vfile != null);
        synchronized(mFlushQueue) {
            mFlushQueue.offer(vfile);
            mFlushQueue.notify();
        }
    }

    @Override
    protected int readdir(String path, StructFuseFileInfo info, DirectoryFiller filler) {
        LOGGER.info("readdir {}", path);

        Iterable<String> files = mVirtualFS.getFilesInDir(path);
        filler.add(files);

        return 0;
    }

    @Override
    protected int getattr(String path, StructStat stat) {
        LOGGER.info("getattr {}", path);
        FileMetadata metadata;

        if((metadata = mVirtualFS.getFile(path)) != null) {
            if(metadata.isDirectory()) {
                stat.mode(TypeMode.S_IFDIR | 0755);
            } else {
                stat.mode(TypeMode.S_IFREG | 0644);
                stat.size(metadata.getSize());
            }
            return 0;
        } else {
            return -ErrorCodes.ENOENT();
        }
    }

    /*
    @Override
    protected int access(String path, int access) {
        LOGGER.info("access {} {}", path, access);
        if(path.equals("/")) {
            return 0;
        }
        FileMetadata metadata = mVirtualFS.getFile(path);
        if(metadata == null) {
            return -ErrorCodes.ENOENT();
        }

        if((access & FileMetadata.FLAG_EXECUTE) > 0
                && (metadata.mFlags & FileMetadata.FLAG_EXECUTE) == 0) {
            return -ErrorCodes.EACCES();
        }
        if((access & FileMetadata.FLAG_READ) > 0
                && (metadata.mFlags & FileMetadata.FLAG_READ) == 0) {
            return -ErrorCodes.EACCES();
        }
        if((access & FileMetadata.FLAG_WRITE) > 0
                && (metadata.mFlags & FileMetadata.FLAG_WRITE) == 0) {
            return -ErrorCodes.EACCES();
        }
        return 0;
    }
    */

    @Override
    protected int mkdir(String path, long mode) {
        LOGGER.info("mkdir {} {}", path, mode);
        mVirtualFS.mkdir(path);
        return 0;
    }

    @Override
    protected int rmdir(String path) {
        LOGGER.info("rmdir {}", path);
        mVirtualFS.rmdir(path);
        return 0;
    }

    @Override
    protected int mknod(String path, long mode, long dev) {
        LOGGER.info("mknod {} {}", path, mode);
        if(TypeMode.S_ISREG((int) mode) && !TypeMode.S_ISDIR((int) mode)) {
            if(!mVirtualFS.mknod(path)) {
                return -ErrorCodes.EEXIST();
            } else {
                return 0;
            }

        } else {
            return -ErrorCodes.EINVAL();
        }

    }

    private static class ErrorCodeException extends Exception {
        public final int mErrorCode;

        public ErrorCodeException(int errorCode) {
            mErrorCode = errorCode;
        }
    }

    private VirtualFile openVirtualFile(String path) throws ErrorCodeException {
        VirtualFile vfile;

        FileMetadata metadata = mVirtualFS.getFile(path);
        if(metadata == null || metadata.isDirectory()) {
            throw new ErrorCodeException(-ErrorCodes.ENOENT());
        }

        final VirtualFilesystem.Path filePath = mVirtualFS.createPath();
        filePath.setFilepath(path);

        return new VirtualFile(mVirtualFS, mFilesystemStorage, filePath, metadata);
    }

    @Override
    protected int open(String path, StructFuseFileInfo info) {
        LOGGER.info("open {}", path);

        FileMetadata metadata;
        if((metadata = mVirtualFS.getFile(path)) != null) {

            final VirtualFilesystem.Path filePath = mVirtualFS.createPath();
            filePath.setFilepath(path);

            VirtualFile vfile = new VirtualFile(mVirtualFS, mFilesystemStorage, filePath, metadata);
            vfile.open(info.openMode());

            final long fh = mFilehandleId++;
            mOpenFiles.put(fh, vfile);

            info.fh(fh);

            return 0;
        } else {
            return -ErrorCodes.ENOENT();
        }
    }

    @Override
    protected int release(String path, StructFuseFileInfo info) {
        LOGGER.info("release {}", path);

        final long fh = info.fh();
        VirtualFile vfile = mOpenFiles.remove(fh);
        if(vfile != null) {
            try {
                vfile.close();
            } catch (IOException e) {
                LOGGER.error("", e);
            }
        }

        return 0;
    }

    @Override
    protected int write(String path, ByteBuffer buf, long bufSize, long writeOffset, StructFuseFileInfo wrapper) {
        LOGGER.info("write {} {} {}", path, bufSize, writeOffset);

        final long fh = wrapper.fh();
        VirtualFile vfile = mOpenFiles.get(fh);
        if(vfile != null) {
            try {
                int bytesWritten = vfile.write(buf, bufSize, writeOffset);
                return bytesWritten;
            } catch (Exception e) {
                LOGGER.error("", e);
                return -ErrorCodes.EIO();
            }
        } else {
            return -ErrorCodes.EBADF();
        }
    }

    @Override
    protected int read(String path, ByteBuffer buffer, long size, long offset, StructFuseFileInfo info) {
        LOGGER.info("read {} {} {}", path, offset, size);

        final long fh = info.fh();
        VirtualFile vfile = mOpenFiles.get(fh);
        if(vfile != null) {
            try {
                int bytesRead = vfile.read(buffer, size, offset);
                if(bytesRead > 0) {
                    while(bytesRead < size) {
                        int r = vfile.read(buffer, size - bytesRead, offset + bytesRead);
                        if (r <= 0) {
                            break;
                        } else {
                            bytesRead += r;
                        }
                    }
                }

                return bytesRead;
            } catch (Exception e) {
                LOGGER.error("", e);
                return -ErrorCodes.EIO();
            }
        } else {
            return -ErrorCodes.EBADF();
        }
    }

    @Override
    protected int truncate(String path, long size) {
        LOGGER.info("truncate {} {}", path, size);
        //TODO: implement

        return 0;
    }

    @Override
    protected int unlink(String path) {
        LOGGER.info("unlink {}", path);
        mVirtualFS.unlink(path);
        return 0;
    }

    @Override
    protected int rename(String path, String newName) {
        LOGGER.info("rename {} ==> {}", path, newName);
        mVirtualFS.rename(path, newName);
        return 0;
    }


}
