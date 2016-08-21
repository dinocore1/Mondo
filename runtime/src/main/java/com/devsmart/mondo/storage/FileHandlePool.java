package com.devsmart.mondo.storage;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FileHandlePool {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileHandlePool.class);

    public class OutOfHandlesException extends Exception {

    }

    private final TreeSet<Integer> mFreeHandles = new TreeSet<Integer>();
    private final TreeSet<Integer> mAllocatedHandles = new TreeSet<Integer>();

    public FileHandlePool(int maxHandles) {
        for(int i=0;i<maxHandles;i++) {
            mFreeHandles.add(i);
        }
    }

    public synchronized int allocate() throws OutOfHandlesException {
        if(!mFreeHandles.isEmpty()) {
            Integer handle = mFreeHandles.pollFirst();
            mAllocatedHandles.add(handle);
            return handle;
        } else {
            throw new OutOfHandlesException();
        }
    }

    public void free(int handle) {
        if(mAllocatedHandles.remove(handle)){
            mFreeHandles.add(handle);
        } else {
            LOGGER.warn("trying to free unallocated handle: {}", handle);
        }
    }


}
