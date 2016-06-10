package com.devsmart.mondo.data;


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;

public class DataStreamBreaker {

    private static final int WINDOW_SIZE = 50;
    private final HashFunction mSecureHash;
    private final long mMask;

    public DataStreamBreaker(HashFunction secureHash, int numBits) {
        mSecureHash = secureHash;
        mMask = (long) ((1 << numBits) - 1);
    }

    public Iterable<Segment> getSegments(InputStream in) throws IOException {

        LinkedList<Segment> retval = new LinkedList<Segment>();

        Hasher hasher = mSecureHash.newHasher();

        Buzhash buzHash = new Buzhash(WINDOW_SIZE);
        buzHash.reset();
        byte[] buffer = new byte[32 * 1024];

        long last = 0;
        long pos = 0;
        int bytesRead;

        while((bytesRead = in.read(buffer, 0, buffer.length)) > 0) {
            for (int i = 0; i < bytesRead; i++) {
                byte newByte = buffer[i];
                long hash = buzHash.addByte(newByte);
                hasher.putByte(newByte);
                pos++;

                if ((hash & mMask) == 0) {
                    //segment boundery found
                    Segment segment = new Segment(last, pos - last, hasher.hash());
                    retval.add(segment);

                    last = pos;
                    buzHash.reset();
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
