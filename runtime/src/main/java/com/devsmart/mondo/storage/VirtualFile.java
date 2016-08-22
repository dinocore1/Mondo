package com.devsmart.mondo.storage;


import com.devsmart.mondo.data.SecureSegment;
import com.devsmart.mondo.data.Segment;
import com.devsmart.mondo.data.SegmentList;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashCode;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

public class VirtualFile implements Closeable {

    public static final int BLOCK_SIZE = 4096;


    private final FilesystemStorage mFilesystemStorage;

    public VirtualFilesystem.Path mPath;
    public FileMetadata mMetadata;
    public int mHandle;

    public void setDataFile(DataFile datafile) {
        long offset = 0;
        if(datafile != null && datafile.mParts != null && datafile.mParts.length >= 0) {
            ArrayList<SecureSegment> segments = new ArrayList<SecureSegment>(datafile.mParts.length);
            for (int i = 0; i < datafile.mParts.length; i++) {
                FilePart part = datafile.mParts[i];
                segments.add(new SecureSegment(offset, part.getSize(), part.getSha1Checksum()));
            }

            mStoredSegments = new SegmentList(segments);
        } else {
            mStoredSegments = new SegmentList();
        }
    }


    private SegmentList mTransientSegments = new SegmentList();
    private SegmentList mStoredSegments;


    private Segment mBufferSegment;
    private byte[] mBuffer = new byte[BLOCK_SIZE];
    private File mTempFile;
    private RandomAccessFile mTempBufferFile;

    public VirtualFile(FilesystemStorage storage) {
        mFilesystemStorage = storage;
    }

    public boolean isDirectory() {
        return (mMetadata.mFlags & FileMetadata.FLAG_DIR) > 0;
    }

    public boolean isFile() {
        return (mMetadata.mFlags & FileMetadata.FLAG_DIR) == 0;
    }

    public String getName() {
        return mPath.getName();
    }

    public long getSize() {
        return Math.max(mTransientSegments.end(), mStoredSegments.end());
    }


    @Override
    public void close() throws IOException {
        if(mTempBufferFile != null) {
            mTempBufferFile.close();
            mTempBufferFile = null;
        }
    }


    public int write(ByteBuffer inputBuffer, long size, long offset) throws IOException {

        RandomAccessFile backingFile = getBackingFile();

        FileChannel channel = backingFile.getChannel();
        channel.position(offset);
        long bytesWritten = 0;
        while(bytesWritten < size) {
            bytesWritten += channel.write(inputBuffer);
        }

        Segment s = new Segment(offset, bytesWritten);
        mTransientSegments.merge(s);

        return (int) bytesWritten;
    }

    public int read(ByteBuffer buffer, long size, long offset) throws IOException {
        Segment s = new Segment(offset, size);
        Segment transientSegment = mTransientSegments.getContainer(s);
        if(transientSegment != null) {
            RandomAccessFile transientFile = getBackingFile();
            FileChannel channel = transientFile.getChannel();
            channel.position(offset);
            return channel.read(buffer);
        } else if((transientSegment = mStoredSegments.getContainer(s)) != null){
            //read from data store
            return -1;
        } else {
            throw new IOException("no data for offset: " + offset);
        }

    }

    private RandomAccessFile getBackingFile() throws IOException {
        if(mTempFile == null) {
            mTempFile = mFilesystemStorage.createTempFile();
        }

        if(mTempBufferFile == null) {
            mTempBufferFile = new RandomAccessFile(mTempFile, "rw");
        }
        return mTempBufferFile;
    }
}
