package com.devsmart.mondo.storage;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;

import java.io.File;
import java.util.NavigableSet;
import java.util.SortedSet;

public class VirtualFilesystem {


    private final DB mDB;
    private final NavigableSet<Object[]> mFiles;


    public VirtualFilesystem(File databaseFile) {
        mDB = DBMaker.fileDB(databaseFile)
                .make();

        mFiles = mDB.treeSet("files")
                .serializer(new SerializerArrayTuple(Serializer.STRING_ASCII, VirtualFile.SERIALIZER))
                .createOrOpen();


    }

    public Iterable<VirtualFile> getFilesInDir(String filePath) {
        SortedSet<Object[]> files = mFiles.subSet(new Object[]{filePath}, new Object[]{filePath, null});
        return Iterables.transform(files, new Function<Object[], VirtualFile>() {
            @Override
            public VirtualFile apply(Object[] input) {
                return (VirtualFile)input[1];
            }
        });
    }
}
