package com.devsmart.mondo;


import org.mapdb.*;
import org.mapdb.serializer.SerializerArray;
import org.mapdb.serializer.SerializerArrayTuple;

import java.io.File;
import java.nio.file.Path;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.*;

public class MondoFileStore {

    private final File mStorageRoot;
    private final FileSystemState mState;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock mReadLock;
    private final Lock mWriteLock;
    private final NavigableSet<Object[]> mFiles;
    private File mDBFile;
    private DB mDB;


    MondoFileStore(File storageRoot) {
        checkNotNull(storageRoot);

        if(!storageRoot.exists()) {
            checkState(storageRoot.mkdirs());
        }

        checkState(storageRoot.isDirectory());

        mStorageRoot = storageRoot;
        mState = new FileSystemState();
        mReadLock = lock.readLock();
        mWriteLock = lock.writeLock();

        mDBFile = new File(storageRoot, "mondo");
        mDB = DBMaker.fileDB(mDBFile)
                .make();

        mFiles = mDB.treeSet("files")
                .serializer(new SerializerArrayTuple(Serializer.STRING_DELTA, Serializer.STRING, MondoFile.SERIALIZER))
                .createOrOpen();

        SortedSet<Object[]> rootSet = mFiles.subSet(new Object[]{"", ""}, new Object[]{"", "", null});
        if(rootSet.isEmpty()) {
            MondoFile root = new MondoFile();
            root.isFile = false;
            mFiles.add(new Object[]{"", "", root});
        }


    }



    public FileSystemState state() {
        return mState;
    }

    public Lock readLock() {
        mReadLock.lock();
        return mReadLock;
    }

    public Lock writeLock() {
        mWriteLock.lock();
        return mWriteLock;
    }

    public MondoFile lookUpWithLock(MondoFSPath path) {
        MondoFile retval = null;
        path = path.normalize();

        String parentStr;
        MondoFSPath parent = path.getParent();
        if(parent != null) {
            parentStr = parent.toString();
        } else {
            parentStr = "";
        }

        String filenameStr = null;
        MondoFSPath filename = path.getFileName();
        if(filename != null) {
            filenameStr = filename.toString();
        } else {
            filenameStr = "";
        }


        mReadLock.lock();
        try {

            SortedSet<Object[]> file = mFiles.subSet(new Object[]{parentStr, filenameStr}, new Object[]{parentStr, filenameStr, null});
            if(!file.isEmpty()) {
                Object[] record = file.first();
                retval = (MondoFile) record[2];
            }

        } finally {
            mReadLock.unlock();
        }
        return retval;
    }

}
