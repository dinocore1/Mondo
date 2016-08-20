package com.devsmart.mondo.storage;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.jetbrains.annotations.NotNull;
import org.mapdb.*;
import org.mapdb.serializer.GroupSerializerObjectArray;
import org.mapdb.serializer.SerializerArrayTuple;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.SortedSet;

public class VirtualFilesystem implements Closeable {


    final DB mDB;
    private final NavigableSet<Object[]> mFiles;
    private final BTreeMap<Long, DataFile> mDataObjects;


    public VirtualFilesystem(File databaseFile) {
        mDB = DBMaker.fileDB(databaseFile)
                .transactionEnable()
                .make();

        mFiles = mDB.treeSet("files")
                .serializer(new SerializerArrayTuple(Serializer.STRING_DELTA2, Serializer.STRING, FileInfo.SERIALIZER))
                .createOrOpen();

        mDataObjects = mDB.treeMap("data")
                .valuesOutsideNodesEnable()
                .keySerializer(Serializer.LONG)
                .valueSerializer(DataFile.SERIALIZER)
                .createOrOpen();

    }

    @Override
    public void close() throws IOException {
        mDB.close();
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

    public Iterable<VirtualFile> getFilesInDir(String filePath) {
        SortedSet<Object[]> files = mFiles.subSet(new Object[]{filePath}, new Object[]{filePath, null});
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
        VirtualFile retval = null;
        final int i = filePath.lastIndexOf('/');
        String dir = filePath.substring(0, i);
        String fileName = filePath.substring(i+1, filePath.length());

        Object[] parts = mFiles.floor(new Object[]{dir, fileName});
        if(parts != null) {
            retval = DB_FILES_TO_VIRTUALFILE.apply(parts);
        }
        return retval;
    }

    public void mkdir(String filePath) {
        final int i = filePath.lastIndexOf('/');
        String dir = filePath.substring(0, i);
        String fileName = filePath.substring(i+1, filePath.length());

        FileInfo info = new FileInfo();
        info.mBits = VirtualFile.FLAG_DIR;
        Object[] parts = new Object[]{dir, fileName, info};
        mFiles.add(parts);
    }

    public void create(String filePath) {
        final int i = filePath.lastIndexOf('/');
        String dir = filePath.substring(0, i);
        String fileName = filePath.substring(i+1, filePath.length());

        FileInfo info = new FileInfo();
        info.mBits = 0;
        Object[] parts = new Object[]{dir, fileName, info};
        mFiles.add(parts);
    }

    public boolean pathExists(String filePath) {
        return mFiles.contains(new Object[]{filePath});
    }


}
