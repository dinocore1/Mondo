package com.devsmart.mondo.storage;


import com.google.common.base.Function;
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
    private final ObjectPool<Path> mPathPool;
    private final NavigableSet<Object[]> mFiles;
    private final BTreeMap<Long, DataFile> mDataObjects;


    public VirtualFilesystem(DB database, char pathSeparator) {
        mDB = database;
        mPathSeparator = pathSeparator;

        mFiles = mDB.treeSet("files")
                .serializer(new SerializerArrayTuple(Serializer.STRING_DELTA2, Serializer.STRING, FileInfo.SERIALIZER))
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

    static class FileInfo {
        int mBits;
        long mDataFileId;

        static final GroupSerializerObjectArray<FileInfo> SERIALIZER = new GroupSerializerObjectArray<FileInfo>() {

            @Override
            public void serialize(@NotNull DataOutput2 out, @NotNull FileInfo value) throws IOException {
                out.packInt(value.mBits);
                out.packLong(value.mDataFileId);
            }

            @Override
            public FileInfo deserialize(@NotNull DataInput2 input, int available) throws IOException {
                FileInfo retval = new FileInfo();
                retval.mBits = input.unpackInt();
                retval.mDataFileId = input.unpackLong();
                return retval;
            }

            @Override
            public int compare(FileInfo a, FileInfo b) {
                return 0;
            }
        };
    }

    public Iterable<VirtualFile> getFilesInDir(String filePathStr) {
        SortedSet<Object[]> files = mFiles.subSet(new Object[]{filePathStr}, new Object[]{filePathStr, null});
        return Iterables.transform(files, DB_FILES_TO_VIRTUALFILE);
    }

    private static Function<Object[], VirtualFile> DB_FILES_TO_VIRTUALFILE = new Function<Object[], VirtualFile>() {
        @Override
        public VirtualFile apply(Object[] input) {
            VirtualFile f = new VirtualFile();
            f.mName = (String) input[1];

            FileInfo info = (FileInfo) input[2];
            f.mFlags = info.mBits;
            f.mDataFileId = info.mDataFileId;

            return f;
        }
    };

    public VirtualFile getFile(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);
                String dir = path.getParent();
                String fileName = path.getName();

                Object[] parts = mFiles.ceiling(new Object[]{dir, fileName});

                VirtualFile retval = null;
                if(parts != null) {
                    retval = DB_FILES_TO_VIRTUALFILE.apply(parts);
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

                FileInfo info = new FileInfo();
                info.mBits = VirtualFile.FLAG_DIR;
                Object[] parts = new Object[]{dir, fileName, info};
                mFiles.add(parts);
            } finally {
                mPathPool.returnObject(path);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public void create(String filePath) {
        try {
            Path path = mPathPool.borrowObject();
            try {
                path.setFilepath(filePath);

                String dir = path.getParent();
                String fileName = path.getName();

                FileInfo info = new FileInfo();
                info.mBits = 0;
                Object[] parts = new Object[]{dir, fileName, info};
                mFiles.add(parts);
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
