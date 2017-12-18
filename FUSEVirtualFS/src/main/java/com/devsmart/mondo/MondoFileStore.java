package com.devsmart.mondo;


import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MondoFileStore {

    private final FileSystemState mState;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock mReadLock;
    private final Lock mWriteLock;


    MondoFileStore() {
        mState = new FileSystemState();
        mReadLock = lock.readLock();
        mWriteLock = lock.writeLock();
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

}
