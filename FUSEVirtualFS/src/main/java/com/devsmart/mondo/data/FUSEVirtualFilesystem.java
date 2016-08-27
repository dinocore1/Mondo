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
    private final HashMap<String, VirtualFile> mOpenFiles = new HashMap<String, VirtualFile>();
    private final FileHandlePool mFileHandles = new FileHandlePool(200);
    private final FilesystemStorage mFilesystemStorage;
    private final LinkedList<VirtualFile> mFlushQueue = new LinkedList<VirtualFile>();
    private final ScheduledExecutorService mScheduledIO = Executors.newScheduledThreadPool(3);
    private final ScheduledFuture<?> mFlushTask;

    public void shutdown() {
        mFlushTask.cancel(false);
        mScheduledIO.shutdown();
        mFlushJob.run();
    }

    private final Runnable mFlushJob = new Runnable() {

        private VirtualFile dequeue() {
            synchronized (mFlushQueue) {
                return mFlushQueue.poll();
            }
        }

        @Override
        public void run() {
            VirtualFile f;
            while((f = dequeue()) != null) {
                try {
                    f.fsync();
                    final int refCount = f.mRefCount.get();
                    if(refCount <= 0) {
                        synchronized (mOpenFiles) {
                            f.close();
                            mOpenFiles.remove(f.mKey);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("", e);
                }
            }
        }
    };

    public FUSEVirtualFilesystem(VirtualFilesystem virtualFilesystem, FilesystemStorage storage) {
        mVirtualFS = virtualFilesystem;
        mFilesystemStorage = storage;
        mFlushTask = mScheduledIO.scheduleAtFixedRate(mFlushJob, 10, 10, TimeUnit.SECONDS);
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

        if(!"/".equals(path) && !mVirtualFS.pathExists(path)) {
            return -ErrorCodes.ENOENT();
        }

        Iterable<String> files = mVirtualFS.getFilesInDir(path);
        filler.add(files);

        return 0;
    }

    @Override
    protected int getattr(String path, StructStat stat) {
        LOGGER.info("getattr {}", path);
        FileMetadata metadata;
        if("/".equals(path)) {
            stat.mode(TypeMode.S_IFDIR | 0775);
            return 0;
        }

        VirtualFile vfile;
        synchronized (mOpenFiles) {
            vfile = mOpenFiles.get(path);
        }
        if(vfile != null) {
            stat.mode(TypeMode.S_IFREG | 0644);
            stat.size(vfile.getSize());
        } else if((metadata = mVirtualFS.getFile(path)) != null) {
            if(metadata.isDirectory()) {
                stat.mode(TypeMode.S_IFDIR | 0755);
            } else {
                stat.mode(TypeMode.S_IFREG | 0644);
                stat.size(metadata.getSize());
            }
        } else {
            return -ErrorCodes.ENOENT();
        }
        return 0;
    }

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
        mVirtualFS.mknod(path);
        return 0;
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

        vfile = new VirtualFile(mVirtualFS, mFilesystemStorage);
        vfile.mMetadata = metadata;
        vfile.setDataFile(mVirtualFS.mDataObjects.get(vfile.mMetadata.mDataFileId));
        vfile.mPath = mVirtualFS.mPathPool.borrow();
        vfile.mPath.setFilepath(path);
        vfile.mKey = path;

        return vfile;
    }

    @Override
    protected int open(String path, StructFuseFileInfo info) {
        LOGGER.info("open {}", path);

        try {
            synchronized (mOpenFiles) {
                VirtualFile vfile = mOpenFiles.get(path);
                if (vfile == null) {
                    vfile = openVirtualFile(path);
                    vfile.mRefCount.set(1);
                    mOpenFiles.put(path, vfile);
                } else {
                    vfile.mRefCount.incrementAndGet();
                }
            }
            return 0;
        } catch (ErrorCodeException e) {
            return e.mErrorCode;
        }
    }

    @Override
    protected int release(String path, StructFuseFileInfo info) {
        LOGGER.info("release {}", path);

        VirtualFile vfile;
        synchronized (mOpenFiles) {
            vfile = mOpenFiles.get(path);
        }
        if(vfile != null) {
            final int refcount = vfile.mRefCount.decrementAndGet();
            if (refcount <= 0) {
                addToFlushQueue(vfile);
            }
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
        LOGGER.info("read {} {} {}", path, offset, size);
        VirtualFile vfile;
        synchronized (mOpenFiles) {
            vfile = mOpenFiles.get(path);
        }
        if(vfile == null) {
            return -ErrorCodes.ENOENT();
        }

        vfile.mRefCount.incrementAndGet();
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
            return 0;
        } finally {
            vfile.mRefCount.decrementAndGet();
        }
    }

    @Override
    protected int truncate(String path, long size) {
        LOGGER.info("truncate {} {}", path, size);
        VirtualFile vfile;
        synchronized (mOpenFiles) {
            vfile = mOpenFiles.get(path);
        }
        if(vfile == null) {
            return -ErrorCodes.ENOENT();
        }
        vfile.mRefCount.incrementAndGet();
        try {
            vfile.truncate(size);
            return 0;
        } catch (IOException e) {
            LOGGER.error("", e);
            return 0;
        } finally {
            vfile.mRefCount.decrementAndGet();
        }
    }

    @Override
    protected int unlink(String path) {
        LOGGER.info("unlink {}", path);
        FileMetadata file = mVirtualFS.getFile(path);
        if(file == null) {
            return -ErrorCodes.ENOENT();
        } else {
            mVirtualFS.unlink(path);
            return 0;
        }
    }

    @Override
    protected int rename(String path, String newName) {
        LOGGER.info("rename {} ==> {}", path, newName);
        return super.rename(path, newName);
    }


}
