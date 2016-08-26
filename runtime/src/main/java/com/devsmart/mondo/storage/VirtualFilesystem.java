package com.devsmart.mondo.storage;


import com.google.common.base.Function;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterables;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.jetbrains.annotations.NotNull;
import org.mapdb.*;
import org.mapdb.serializer.GroupSerializerObjectArray;
import org.mapdb.serializer.SerializerArrayTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;

public class VirtualFilesystem implements Closeable {


    public static class Path {

        public final char mPathSeperator;

        private String mParent;
        private String mFilename;

        public Path(char pathSeperator) {
            mPathSeperator = pathSeperator;
        }

        public void setFilepath(String filepath) {
            int i = filepath.lastIndexOf(mPathSeperator);
            if(i == 0) {
                mParent = "/";
            } else {
                mParent = filepath.substring(0, i);
            }

            mFilename = filepath.substring(i + 1, filepath.length());
        }


        public String getParent() {
            return mParent;
        }

        public String getName() {
            return mFilename;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualFilesystem.class);

    final DB mDB;
    final char mPathSeparator;
    public final ObjectPool<Path> mPathPool;
    private final BTreeMap<Object[], FileMetadata> mFiles;
    public final BTreeMap<Long, DataFile> mDataObjects;
    private final Atomic.Long mDataFileId;


    public VirtualFilesystem(DB database, char pathSeparator) {
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

        mPathPool = new GenericObjectPool<Path>(new BasePooledObjectFactory<Path>() {
            @Override
            public Path create() throws Exception {
                return new Path(mPathSeparator);
            }

            @Override
            public PooledObject<Path> wrap(Path obj) {
                return new DefaultPooledObject<Path>(obj);
            }
        });

    }

    public synchronized void writeBack(VirtualFile virtualFile) {
        FileMetadata metadata = getFile(virtualFile.mPath);
        metadata.mSize = virtualFile.getSize();

        if(metadata.mDataFileId == -1) {
            metadata.mDataFileId = mDataFileId.incrementAndGet();
        }

        mFiles.put(new Object[]{ virtualFile.mPath.getParent(), virtualFile.mPath.getName()}, metadata);
        mDataObjects.put(metadata.mDataFileId, virtualFile.getDataFile());

        mDB.commit();
    }

    @Override
    public void close() throws IOException {
        mDB.close();
        mPathPool.close();
    }

    public Iterable<String> getFilesInDir(String filePathStr) {
        Map<Object[], FileMetadata> files = mFiles.prefixSubMap(new Object[]{filePathStr});
        return Iterables.transform(files.keySet(), new Function<Object[], String>() {
            @Override
            public String apply(Object[] input) {
                return (String) input[1];
            }
        });
    }

    public synchronized FileMetadata getFile(Path path) {
        String dir = path.getParent();
        String fileName = path.getName();
        FileMetadata retval = mFiles.get(new Object[]{dir, fileName});
        return retval;
    }

    public FileMetadata getFile(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);
                return getFile(path);
            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
            return null;
        }

    }

    public synchronized void mkdir(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();

                FileMetadata info = new FileMetadata();
                info.mFlags = FileMetadata.FLAG_DIR;
                Object[] key = new Object[]{dir, fileName};
                mFiles.put(key, info);
            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public synchronized void rmdir(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();

                mFiles.remove(new Object[]{dir, fileName});

            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public synchronized void mknod(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();

                FileMetadata info = new FileMetadata();
                info.mFlags = 0;
                Object[] key = new Object[]{dir, fileName};
                mFiles.put(key, info);
            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public synchronized void unlink(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();


                mFiles.remove(new Object[]{dir, fileName});

            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public synchronized boolean pathExists(String filePath) {

        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();

                return mFiles.containsKey(new Object[]{dir, fileName});
            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
            return false;
        }


    }


}
