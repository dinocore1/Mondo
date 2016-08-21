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
    private final NavigableSet<Object[]> mFiles;
    private final BTreeMap<Long, DataFile> mDataObjects;


    public VirtualFilesystem(DB database, char pathSeparator) {
        mDB = database;
        mPathSeparator = pathSeparator;

        mFiles = mDB.treeSet("files")
                .serializer(new SerializerArrayTuple(Serializer.STRING_DELTA2, Serializer.STRING, FileMetadata.SERIALIZER))
                .createOrOpen();

        mDataObjects = mDB.treeMap("data")
                .valuesOutsideNodesEnable()
                .keySerializer(Serializer.LONG)
                .valueSerializer(DataFile.SERIALIZER)
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

    @Override
    public void close() throws IOException {
        mDB.close();
        mPathPool.close();
    }

    public Iterable<String> getFilesInDir(String filePathStr) {
        SortedSet<Object[]> files = mFiles.subSet(new Object[]{filePathStr}, new Object[]{filePathStr, null});
        return Iterables.transform(files, new Function<Object[], String>() {
            @Override
            public String apply(Object[] input) {
                return (String) input[1];
            }
        });
    }

    public FileMetadata getFile(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);
                String dir = path.getParent();
                String fileName = path.getName();

                Object[] parts = mFiles.ceiling(new Object[]{dir, fileName});

                FileMetadata retval = null;
                if(parts != null) {
                    retval = (FileMetadata) parts[2];
                }
                return retval;

            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
            return null;
        }

    }

    public void mkdir(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();

                FileMetadata info = new FileMetadata();
                info.mFlags = FileMetadata.FLAG_DIR;
                Object[] parts = new Object[]{dir, fileName, info};
                mFiles.add(parts);
            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public void rmdir(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();

                Object[] parts = mFiles.ceiling(new Object[]{dir, fileName});
                mFiles.remove(parts);

            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public void mknod(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();

                FileMetadata info = new FileMetadata();
                info.mFlags = 0;
                Object[] parts = new Object[]{dir, fileName, info};
                mFiles.add(parts);
            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    /**
     *
     * @param path
     * @param buffer
     * @param bytesAvailable
     * @param offset
     * @return num bytes read
     */
    public int read(String path, ByteBuffer buffer, long bytesAvailable, long offset) {
        return 0;
    }

    /**
     *
     * @param path
     * @param buf
     * @param numBytes
     * @param writeOffset
     * @return num bytes written
     */
    public int write(String path, ByteBuffer buf, long numBytes, long writeOffset) {
        return (int) numBytes;
    }

    public void unlink(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();


                Object[] parts = mFiles.ceiling(new Object[]{dir, fileName});
                mFiles.remove(parts);

            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public boolean pathExists(String filePath) {

        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();

                return mFiles.ceiling(new Object[]{dir, fileName}) != null;
            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
            return false;
        }


    }


}
