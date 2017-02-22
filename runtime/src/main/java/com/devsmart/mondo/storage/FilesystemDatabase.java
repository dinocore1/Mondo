package com.devsmart.mondo.storage;


import org.mapdb.Atomic;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;

public class FilesystemDatabase {

    private final DB mDB;
    private final BTreeMap<Object[], FileMetadata> mFilesIndex;
    private final BTreeMap<Long, DataFile> mDataObjects;
    private final Atomic.Long mDataFileId;



    public class Path {
        String prefix;
        String filename;
        FileMetadata metadata;
    }

    public FilesystemDatabase(DB database) {
        mDB = database;

        mFilesIndex = mDB.treeMap("files")
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
    }
}
