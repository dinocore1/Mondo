package com.devsmart.mondo.data;


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;

public class DataStreamBreaker {

    public interface Callback {
        OutputStream createOutputStream();
        void onNewSegment(SecureSegment segment);
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

    private void newSegment(SecureSegment s) {
        if(mCallback != null) {
            mCallback.onNewSegment(s);
        }
    }

    public Iterable<SecureSegment> getSegments(InputStream in) throws IOException {

        OutputStream outputStream = null;
        LinkedList<SecureSegment> retval = new LinkedList<SecureSegment>();

        Hasher hasher = mSecureHash.newHasher();

        Buzhash buzHash = new Buzhash(WINDOW_SIZE);
        buzHash.reset();
        byte[] buffer = new byte[32 * 1024];

        long last = 0;
        long pos = 0;
        int bytesRead;

        if(mCallback != null) {
            outputStream = mCallback.createOutputStream();
        }

        while((bytesRead = in.read(buffer, 0, buffer.length)) > 0) {
            for (int i = 0; i < bytesRead; i++) {
                byte newByte = buffer[i];
                long hash = buzHash.addByte(newByte);
                hasher.putByte(newByte);
                if(outputStream != null) {
                    outputStream.write(newByte);
                }
                pos++;

                final long length = pos - last;
                if ((hash & mMask) == 0) {
                    //segment boundery found
                    if(outputStream != null) {
                        outputStream.close();
                        outputStream = null;
                    }
                    if(mCallback != null) {
                        outputStream = mCallback.createOutputStream();
                    }

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
            if(outputStream != null) {
                outputStream.close();
            }

            SecureSegment segment = new SecureSegment(last, pos - last, hasher.hash());
            newSegment(segment);
            retval.add(segment);
        }


        return retval;
    }
}
