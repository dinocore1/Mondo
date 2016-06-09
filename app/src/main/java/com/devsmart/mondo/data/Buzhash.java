package com.devsmart.mondo.data;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.Random;

import static com.google.common.base.Preconditions.*;

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
    private long mHashvalue;

    public Buzhash(int windowSize) {
        checkArgument(windowSize > 0 && windowSize <= WORD_SIZE);
        mWindowSize = windowSize;
    }


    public long addByte(byte b) {
        mHashvalue = fastleftshift1(mHashvalue);
        mHashvalue ^= hash(b);
        return mHashvalue;
    }


    public long roll(byte out, byte in) {
        mHashvalue =  fastleftshift1(mHashvalue) ^ hash(in) ^ fastleftshiftn(hash(out));
        return mHashvalue;
    }

    private long hash(byte b) {
        return HASHMAP[0xff & b];
    }

    private long fastleftshiftn(long x)  {
        return  (x << mWindowSize ) | (x >>> (WORD_SIZE-mWindowSize)) ;
    }

    private static long fastleftshift1(long x)  {
        return  (x << 1 ) | (x >>> (WORD_SIZE-1)) ;
    }
}
