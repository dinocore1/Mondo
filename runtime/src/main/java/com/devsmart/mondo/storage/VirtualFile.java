package com.devsmart.mondo.storage;


import com.devsmart.mondo.data.Segment;
import com.devsmart.mondo.data.SegmentList;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.TreeSet;

public class VirtualFile {

    public static final int BLOCK_SIZE = 4096;

    private static final Comparator<Segment> COMPARATOR = new Comparator<Segment>() {
        @Override
        public int compare(Segment a, Segment b) {
            return Long.compare(a.offset, b.offset);
        }
    };

    public VirtualFilesystem.Path mPath;
    public FileMetadata mMetadata;
    public int mHandle;
    public DataFile mDatafile;



    private SegmentList mLiveData = new SegmentList();
    private Segment mBufferSegment;
    private byte[] mBuffer = new byte[BLOCK_SIZE];

    public boolean isDirectory() {
        return (mMetadata.mFlags & FileMetadata.FLAG_DIR) > 0;
    }

    public boolean isFile() {
        return (mMetadata.mFlags & FileMetadata.FLAG_DIR) == 0;
    }

    public String getName() {
        return mPath.getName();
    }



    public void write(ByteBuffer inputBuffer, long size, long offset) {
        Segment newSegment = new Segment(offset, size);
        if(mBufferSegment.contains(newSegment)) {
            mLiveData.merge(newSegment);
            inputBuffer.get(mBuffer, (int)(offset - mBufferSegment.offset), (int)size);
            mLiveData.merge(newSegment);

        } else {
            //need to flush buffer
        }






    }



}
