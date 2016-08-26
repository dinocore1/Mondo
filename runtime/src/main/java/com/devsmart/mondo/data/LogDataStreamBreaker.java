package com.devsmart.mondo.data;


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

public class LogDataStreamBreaker {

    private static final int WINDOW_SIZE = 50;
    private final HashFunction mSecureHash;
    private final int mNumBitsTarget;

    public LogDataStreamBreaker(HashFunction secureHash, int numBits) {
        mSecureHash = secureHash;
        mNumBitsTarget = numBits;
    }

    public Iterable<SecureSegment> getSegments(InputStream in) throws IOException {

        LinkedList<SecureSegment> retval = new LinkedList<SecureSegment>();

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

                final long length = pos - last;
                final int numBitsNeeded = (Long.SIZE - Long.numberOfLeadingZeros(Math.max(1, (1 << mNumBitsTarget) - length)));
                final long mask = (long) ((1 << numBitsNeeded) -1);
                if ((hash & mask) == 0) {
                    //segment boundery found
                    SecureSegment segment = new SecureSegment(last, length, hasher.hash());
                    retval.add(segment);

                    last = pos;
                    buzHash.reset();
                    hasher = mSecureHash.newHasher();
                }
            }
        }

        if(pos > last) {
            SecureSegment segment = new SecureSegment(last, pos - last, hasher.hash());
            retval.add(segment);
        }


        return retval;
    }
}
