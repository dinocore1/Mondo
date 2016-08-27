package com.devsmart.mondo.data;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;


public class DataBreakerInputStream extends InputStream {

    private static final int WINDOW_SIZE = 50;

    public interface Callback {
        void onNewSegment(SecureSegment segment, InputStream in);
    }

    private final InputStream mInputStream;
    private final HashFunction mSecureHashFunction;
    private final long mMask;
    private Callback mCallback;
    private Buzhash mBuzHash;
    private Hasher mSecureHash;
    private long mLast = 0;
    private long mPos = 0;
    private ByteArrayOutputStream mSegmentBuffer;


    public DataBreakerInputStream(InputStream in, HashFunction secureHash, int numBits) {
        mInputStream = in;
        mSecureHashFunction = secureHash;
        mMask = (1 << numBits) -1;
        mBuzHash = new Buzhash(WINDOW_SIZE);
        mBuzHash.reset();
        mSecureHash = mSecureHashFunction.newHasher();
        mSegmentBuffer = new ByteArrayOutputStream(1 << numBits);
    }

    public void setCallback(Callback cb) {
        mCallback = cb;
    }

    private void newSegment(SecureSegment s, byte[] data) {
        if(mCallback != null) {
            mCallback.onNewSegment(s, new ByteArrayInputStream(data));
        }
    }

    private boolean mEndReached = false;

    @Override
    public int read() throws IOException {
        final int data = mInputStream.read();
        if(data < 0) {
            if(mPos - mLast > 0 && !mEndReached) {
                mEndReached = true;
                SecureSegment segment = new SecureSegment(mLast, mPos - mLast, mSecureHash.hash());

                mSegmentBuffer.flush();
                newSegment(segment, mSegmentBuffer.toByteArray());
                mSegmentBuffer.reset();
            }
            return data;
        }
        final long hash = mBuzHash.addByte((byte)data);
        mSecureHash.putByte((byte)data);
        mSegmentBuffer.write(data);
        mPos++;
        if((hash & mMask) == 0) {
            SecureSegment segment = new SecureSegment(mLast, mPos - mLast, mSecureHash.hash());
            mSegmentBuffer.flush();
            newSegment(segment, mSegmentBuffer.toByteArray());
            mSegmentBuffer.reset();

            mLast = mPos;
            mBuzHash.reset();
            mSecureHash = mSecureHashFunction.newHasher();
        }

        return data;
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        int bytesRead = mInputStream.read(b, off, len);
        if(bytesRead > 0) {
            for(int i=0;i<bytesRead;i++) {
                final byte data = b[off + i];
                final long hash = mBuzHash.addByte(data);
                mSecureHash.putByte(data);
                mSegmentBuffer.write(data);
                mPos++;
                if((hash & mMask) == 0) {
                    SecureSegment segment = new SecureSegment(mLast, mPos - mLast, mSecureHash.hash());
                    mSegmentBuffer.flush();
                    newSegment(segment, mSegmentBuffer.toByteArray());
                    mSegmentBuffer.reset();

                    mLast = mPos;
                    mBuzHash.reset();
                    mSecureHash = mSecureHashFunction.newHasher();
                }
            }
        } else {
            if(mPos - mLast > 0 && !mEndReached) {
                mEndReached = true;
                SecureSegment segment = new SecureSegment(mLast, mPos - mLast, mSecureHash.hash());

                mSegmentBuffer.flush();
                newSegment(segment, mSegmentBuffer.toByteArray());
                mSegmentBuffer.reset();
            }
        }
        return bytesRead;
    }
}
