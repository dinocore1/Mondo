package com.devsmart.mondo.storage;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;

import java.io.File;
import java.util.NavigableSet;
import java.util.SortedSet;

public class VirtualFilesystem {


    private final DB mDB;
    private final NavigableSet<Object[]> mFilePaths;
    private final BTreeMap<Long, VirtualFile> mFiles;


    public VirtualFilesystem(File databaseFile) {
        mDB = DBMaker.fileDB(databaseFile)
                .make();

        mFilePaths = mDB.treeSet("filePaths")
                .serializer(new SerializerArrayTuple(Serializer.STRING_ASCII, Serializer.LONG))
                .createOrOpen();

        mFiles = mDB.treeMap("files")
                .valuesOutsideNodesEnable()
                .keySerializer(Serializer.LONG)
                .valueSerializer(VirtualFile.SERIALIZER)
                .createOrOpen();



    }

    public Iterable<VirtualFile> getFilesInDir(String filePath) {
        SortedSet<Object[]> files = mFilePaths.subSet(new Object[]{filePath}, new Object[]{filePath, null});
        return Iterables.transform(files, new Function<Object[], VirtualFile>() {
            @Override
            public VirtualFile apply(Object[] input) {
                final Long fileId = (Long) input[1];
                return mFiles.get(fileId);
            }
        });
    }
}
