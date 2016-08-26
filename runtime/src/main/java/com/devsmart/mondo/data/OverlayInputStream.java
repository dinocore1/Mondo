package com.devsmart.mondo.data;

import com.devsmart.IOUtils;
import com.devsmart.mondo.storage.FilesystemStorage;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Collection;


public class OverlayInputStream extends InputStream {


    private final RandomAccessFile mTempFile;
    private final SegmentList mTempfileSegments;
    private final FilesystemStorage mFSStorage;
    private final SegmentList mStorageSegments;
    private final long mFileSize;
    private long mOffset;
    private SecureSegment mCachedFSSegment;
    private byte[] mCachedBuff;

    public OverlayInputStream(RandomAccessFile tempFile, SegmentList tempFileSegments, FilesystemStorage fsStorage, Collection<SecureSegment> storageSegments) {
        mTempFile = tempFile;
        mTempfileSegments = tempFileSegments;
        mFSStorage = fsStorage;
        mStorageSegments = new SegmentList(storageSegments);
        mFileSize = Math.max(tempFileSegments.end(), mStorageSegments.end());
        mOffset = 0;
    }

    private void loadFSSegment(SecureSegment segment) throws IOException {
        if(mCachedFSSegment == null || !mCachedFSSegment.getID().equals(segment.getID())) {
            InputStream in = mFSStorage.load(segment.getID());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.pump(in, out);
            mCachedBuff = out.toByteArray();
            mCachedFSSegment = segment;
        }
    }

    @Override
    public int read() throws IOException {
        if(mOffset >= mFileSize) {
            return -1;
        }
        int retval = 0;
        SecureSegment fsstorageSegment;
        if(mTempfileSegments.contains(mOffset)) {
            mTempFile.seek(mOffset);
            retval = mTempFile.read();
        } else if((fsstorageSegment = (SecureSegment) mStorageSegments.getSegment(mOffset)) != null) {
            loadFSSegment(fsstorageSegment);
            retval = mCachedBuff[(int) (mOffset - mCachedFSSegment.offset)];
        } else {
            throw new IOException("missing data segment. Offset: " + mOffset);
        }

        mOffset++;

        return retval;
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        if(mOffset >= mFileSize) {
            return -1;
        }

        int retval;
        Segment tempSegment;
        SecureSegment fsSegment;
        final Segment requestedSegment = new Segment(mOffset, len);


        if((tempSegment = mTempfileSegments.getContainer(requestedSegment)) != null) {
            mTempFile.seek(mOffset);
            retval = mTempFile.read(b, off, (int) tempSegment.length);
        } else if((fsSegment = (SecureSegment) mStorageSegments.getSegment(mOffset)) != null) {
            loadFSSegment(fsSegment);
            final int bufOffset = (int) (mOffset-mCachedFSSegment.offset);
            retval = Math.min(len, mCachedBuff.length-bufOffset);
            System.arraycopy(mCachedBuff, bufOffset, b, off, retval);
        } else {
            throw new IOException("missing data segment. Offset: " + mOffset);
        }

        mOffset += retval;
        return retval;

    }
}
