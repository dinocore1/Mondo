package com.devsmart.mondo.data;

import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

public class Buzhash {

    private static final int WORD_SIZE = 64;
    private static long[] HASHMAP;

    static {
        HASHMAP = new long[256];
        Random r = new Random(1);
        for(int i=0;i<HASHMAP.length;i++){
            HASHMAP[i] = r.nextLong();
        }
    }


    private final int mWindowSize;
    private final byte[] mWindow;
    private long mHashvalue;
    private int mBufPos;
    private long mBytesAdded ;

    public Buzhash(int windowSize) {
        checkArgument(windowSize > 0 && windowSize < WORD_SIZE);
        mWindowSize = windowSize;
        mWindow = new byte[mWindowSize];
        reset();
    }


    public long addByte(final byte b) {

        mHashvalue = Long.rotateLeft(mHashvalue, 1);

        if(++mBytesAdded > mWindowSize) {
            mHashvalue ^= Long.rotateLeft(hash(mWindow[mBufPos]), mWindowSize);
        }

        mHashvalue ^= hash(b);

        mWindow[mBufPos] = b;
        mBufPos = (mBufPos + 1) % mWindowSize;

        return mHashvalue;
    }

    public void reset() {
        mHashvalue = 0;
        mBytesAdded = 0;
        mBufPos = 0;
    }

    private long hash(byte b) {
        return HASHMAP[0xff & b];
    }
}
