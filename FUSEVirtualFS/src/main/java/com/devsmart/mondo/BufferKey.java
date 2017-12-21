package com.devsmart.mondo;


import com.google.common.hash.HashCode;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkState;

public class BufferKey implements Comparable<BufferKey>{

    private byte[] mHashCode;
    private final int mBufferNum;

    public BufferKey(HashCode hashCode, int bufferNum) {
        mHashCode = hashCode.asBytes();
        checkState(mHashCode.length == 20);
        mBufferNum = bufferNum;
    }

    @Override
    public int compareTo(@NotNull BufferKey other) {
        int retval;

        for(int i=0;i<20;i++) {
            retval = mHashCode[i] - other.mHashCode[i];
            if(retval != 0) {
                return retval;
            }
        }

        retval = mBufferNum - other.mBufferNum;
        return retval;
    }

    @Override
    public boolean equals(Object o) {
        if(o == null || getClass() != o.getClass()) {
            return false;
        }

        return compareTo((BufferKey) o) == 0;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(mHashCode) ^ mBufferNum;
    }
}
