package com.devsmart.mondo.data;


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;

public class DataStreamBreaker {

    private final int WINDOW_SIZE = 48;
    private final Buzhash mBuzHash = new Buzhash(WINDOW_SIZE);
    private final HashFunction mSecureHash;
    private final long mMask;

    public DataStreamBreaker(HashFunction secureHash, int numBits) {
        mSecureHash = secureHash;
        mMask = (long) ((1 << numBits) - 1);
    }

    public Iterable<Segment> getSegments(InputStream in) throws IOException {

        LinkedList<Segment> retval = new LinkedList<Segment>();

        Hasher hasher = mSecureHash.newHasher();


        byte[] buffer = new byte[32 * 1024];
        byte[] circle = new byte[WINDOW_SIZE];
        long hash = 0;
        int circleIndex = 0;
        long last = 0;
        long pos = 0;

        int bytesRead;

        while((bytesRead = in.read(buffer, 0, buffer.length)) > 0) {
            for (int i = 0; i < bytesRead; i++) {
                hash = mBuzHash.roll(circle[circleIndex], buffer[i]);
                circle[circleIndex] = buffer[i];
                circleIndex = ++circleIndex % WINDOW_SIZE;

                pos++;

                hasher.putByte(buffer[i]);

                if ((hash & mMask) == 0) {
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
