package com.devsmart.mondo.data;


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class DataStreamBreaker {

    private static final int WIDTH = 64; //number of bytes in window
    private static final long SEED = 2273; // rolling hash seed

    private final HashFunction mSecureHash;
    private final long mMask;

    public DataStreamBreaker(HashFunction secureHash, int numBits) {
        mSecureHash = secureHash;
        mMask = (long) ((1 << numBits) - 1);
    }

    public Iterable<Segment> getSegments(InputStream in) throws IOException {

        LinkedList<Segment> retval = new LinkedList<Segment>();

        Hasher hasher = mSecureHash.newHasher();

        long maxSeed = SEED;
        byte[] buffer = new byte[32 * 1024];
        byte[] circle = new byte[WIDTH];
        long hash = 0;
        int circleIndex = 0;
        long last = 0;
        long pos = 0;

        for(int i=0;i<WIDTH;i++) {
            maxSeed *= maxSeed;
        }

        int bytesRead;

        while((bytesRead = in.read(buffer, 0, buffer.length)) > 0) {
            for (int i = 0; i < bytesRead; i++) {
                hash = buffer[i] + ((hash - (maxSeed * circle[circleIndex])) * SEED);
                circle[circleIndex] = buffer[i];
                circleIndex = ++circleIndex % WIDTH;

                pos++;

                hasher.putByte(buffer[i]);

                if ((hash | mMask) == hash) {
                    //segment boundery found
                    Segment segment = new Segment(last, pos - last, hasher.hash());
                    retval.add(segment);

                    last = pos;
                    hash = 0;
                    Arrays.fill(circle, (byte) 0);
                    circleIndex = 0;
                    hasher = mSecureHash.newHasher();
                }
            }
        }

        if(pos > last) {
            Segment segment = new Segment(last, pos - last, hasher.hash());
            retval.add(segment);
        }


        return retval;
    }
}
