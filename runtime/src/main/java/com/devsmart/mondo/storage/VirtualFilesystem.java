package com.devsmart.mondo.storage;


import com.devsmart.ObjectPool;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.mapdb.Atomic;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class VirtualFilesystem implements Closeable {

    public static class Path {

        public final char mPathSeperator;

        private String mParent;
        private String mFilename;
        final Object[] mDBKey = new Object[2];

        public Path(char pathSeperator) {
            mPathSeperator = pathSeperator;
        }

        public void setFilepath(String filepath) {
            final int strLen = filepath.length();
            int i = filepath.lastIndexOf(mPathSeperator);

            if(strLen == 1 && i == 0) {
                mParent = "";
                mFilename = "";
            } else {
                mParent = filepath.substring(0, Math.max(i + 1, 1));
                mFilename = filepath.substring(i + 1, strLen);
            }


            mDBKey[0] = mParent;
            mDBKey[1] = mFilename;
        }


        public String getParent() {
            return mParent;
        }

        public String getName() {
            return mFilename;
        }

        @Override
        public String toString() {
            return mParent + mPathSeperator + mFilename;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualFilesystem.class);

    final DB mDB;
    final char mPathSeparator;
    public final ObjectPool<Path> mPathPool;
    private final BTreeMap<Object[], FileMetadata> mFiles;
    public final BTreeMap<Long, DataFile> mDataObjects;
    private final Atomic.Long mDataFileId;


    public VirtualFilesystem(DB database, final char pathSeparator) {
        mDB = database;
        mPathSeparator = pathSeparator;

        mFiles = mDB.treeMap("files")
                .keySerializer(new SerializerArrayTuple(Serializer.STRING_DELTA2, Serializer.STRING))
                .valueSerializer(FileMetadata.SERIALIZER)
                .createOrOpen();

        mDataObjects = mDB.treeMap("data")
                .valuesOutsideNodesEnable()
                .keySerializer(Serializer.LONG)
                .valueSerializer(DataFile.SERIALIZER)
                .createOrOpen();

        mDataFileId = mDB.atomicLong("dataFileId")
                .createOrOpen();

        mPathPool = new ObjectPool<Path>(10, new ObjectPool.PooledCreator<Path>() {
            @Override
            public Path create() {
                return new Path(pathSeparator);
            }
        });

        Path rootDir = mPathPool.borrow();
        rootDir.setFilepath("/");
        if(!mFiles.containsKey(rootDir.mDBKey)) {
            FileMetadata info = new FileMetadata();
            info.mFlags = FileMetadata.FLAG_DIR |
                    FileMetadata.FLAG_READ | FileMetadata.FLAG_WRITE | FileMetadata.FLAG_EXECUTE;
            mFiles.put(rootDir.mDBKey, info);
        }
        mPathPool.release(rootDir);

    }

    public synchronized void writeBack(VirtualFile virtualFile) {
        FileMetadata metadata = getFile(virtualFile.mPath);
        if(metadata != null) {
            metadata.mSize = virtualFile.getSize();

            if (metadata.mDataFileId == -1) {
                metadata.mDataFileId = mDataFileId.incrementAndGet();
            }

            mFiles.put(new Object[]{virtualFile.mPath.getParent(), virtualFile.mPath.getName()}, metadata);
            mDataObjects.put(metadata.mDataFileId, virtualFile.getDataFile());

            mDB.commit();
        } else {
            LOGGER.warn("meta data is null. Did the file get deleted?");
        }
    }

    @Override
    public void close() throws IOException {
        mDB.close();
    }

    public synchronized Iterable<String> getFilesInDir(String filePathStr) {
            Map<Object[], FileMetadata> files = mFiles.prefixSubMap(new Object[]{filePathStr});
        return Iterables.transform(files.keySet(), new Function<Object[], String>() {
            @Override
            public String apply(Object[] input) {
                return (String) input[1];
            }
        });
    }

    public synchronized FileMetadata getFile(Path path) {
        FileMetadata retval = mFiles.get(path.mDBKey);
        return retval;
    }

    public synchronized FileMetadata getFile(String filePath) {
        Path path = mPathPool.borrow();
        try {
            path.setFilepath(filePath);
            FileMetadata metadata = mFiles.get(path.mDBKey);
            return metadata;
        } finally {
            mPathPool.release(path);
        }
    }

    public synchronized void mkdir(String filePath) {
        Path path = mPathPool.borrow();
        try {
            path.setFilepath(filePath);

            FileMetadata info = new FileMetadata();
            info.mFlags = FileMetadata.FLAG_DIR |
                    FileMetadata.FLAG_READ | FileMetadata.FLAG_WRITE | FileMetadata.FLAG_EXECUTE;
            mFiles.put(path.mDBKey, info);
            mDB.commit();
        } finally {
            mPathPool.release(path);
        }
    }

    public synchronized void rmdir(String filePath) {
        Path path = mPathPool.borrow();
        try {
            path.setFilepath(filePath);

            mFiles.remove(path.mDBKey);
            mDB.commit();

        } finally {
            mPathPool.release(path);
        }
    }

    public synchronized void mknod(String filePath) {
        Path path = mPathPool.borrow();
        try {
            path.setFilepath(filePath);

            FileMetadata info = new FileMetadata();
            info.mFlags = 0;
            mFiles.put(path.mDBKey, info);
            mDB.commit();
        } finally {
            mPathPool.release(path);
        }
    }

    public synchronized void unlink(String filePath) {
        Path path = mPathPool.borrow();
        try {
            path.setFilepath(filePath);

            FileMetadata metadata = mFiles.remove(path.mDBKey);
            if(metadata != null && metadata.mDataFileId >= 0) {
                mDataObjects.remove(metadata.mDataFileId);
            }
            mDB.commit();

        } finally {
            mPathPool.release(path);
        }
    }

    public synchronized void truncate(String filePath, long size) {
        Path path = mPathPool.borrow();
        try {
            path.setFilepath(filePath);
            FileMetadata metadata = mFiles.get(path.mDBKey);
            metadata.mSize = size;
            mFiles.put(path.mDBKey, metadata);
            mDB.close();
        } finally {
            mPathPool.release(path);
        }

    }

    public synchronized void rename(String oldFilepath, String newFilepath) {
        Path path = mPathPool.borrow();
        try {
            path.setFilepath(oldFilepath);
            FileMetadata metadata = mFiles.remove(path.mDBKey);

            path.setFilepath(newFilepath);
            mFiles.put(path.mDBKey, metadata);

            mDB.commit();

        } finally {
            mPathPool.release(path);
        }
    }

    public synchronized boolean pathExists(String filePath) {
        Path path = mPathPool.borrow();
        try {
            path.setFilepath(filePath);
            return mFiles.containsKey(path.mDBKey);
        } finally {
            mPathPool.release(path);
        }

    }


}
