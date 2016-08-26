package com.devsmart.mondo.data;


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

public class DataStreamBreaker {

    public interface Callback {
        void onNewSegment(Segment segment);
    }

    private static final int WINDOW_SIZE = 50;
    private final HashFunction mSecureHash;
    private final long mMask;
    private Callback mCallback;

    public DataStreamBreaker(HashFunction secureHash, int numBits) {
        mSecureHash = secureHash;
        mMask = (1 << numBits) - 1;
    }

    public void setCallback(Callback cb) {
        mCallback = cb;
    }

    private void newSegment(Segment s) {
        if(mCallback != null) {
            mCallback.onNewSegment(s);
        }
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
                if ((hash & mMask) == 0) {
                    //segment boundery found
                    SecureSegment segment = new SecureSegment(last, length, hasher.hash());
                    newSegment(segment);
                    retval.add(segment);

                    last = pos;
                    buzHash.reset();
                    hasher = mSecureHash.newHasher();
                }
            }
        }

        if(pos > last) {
            SecureSegment segment = new SecureSegment(last, pos - last, hasher.hash());
            newSegment(segment);
            retval.add(segment);
        }


        return retval;
    }
}
