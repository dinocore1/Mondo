package com.devsmart.mondo.storage;


import com.devsmart.mondo.data.SecureSegment;
import com.devsmart.mondo.data.Segment;
import com.devsmart.mondo.data.SegmentList;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class VirtualFile implements Closeable {

    public static final int BLOCK_SIZE = 4096;


    private final FilesystemStorage mFilesystemStorage;

    public VirtualFilesystem.Path mPath;
    public FileMetadata mMetadata;
    public int mHandle;
    private SegmentList mTransientSegments = new SegmentList();
    private SegmentList mStoredSegments;
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


    @Override
    public void close() throws IOException {
        if(mTempBufferFile != null) {
            mTempBufferFile.close();
            mTempBufferFile = null;
        }
    }

    public synchronized void fsync() throws IOException {


        /*
            create overlay stream
            pump the overlay stream while running buzhash to break it up into new segments
            write out new segments
         */


        for(Segment s : mTransientSegments) {

            mStoredSegments.getContainer(s);
        }

    }


    public synchronized int write(ByteBuffer inputBuffer, long size, long offset) throws IOException {

        RandomAccessFile backingFile = getBackingFile();

        FileChannel channel = backingFile.getChannel();
        channel.position(offset);
        long bytesWritten = channel.write(inputBuffer);

        Segment s = new Segment(offset, bytesWritten);
        mTransientSegments.merge(s);

        return (int) bytesWritten;
    }

    public synchronized int read(ByteBuffer buffer, long size, long offset) throws IOException {
        Segment containingSegment;

        Segment s = new Segment(offset, size);

        if((containingSegment = mTransientSegments.getContainer(s)) != null) {
            RandomAccessFile transientFile = getBackingFile();
            FileChannel channel = transientFile.getChannel();
            channel.position(offset);
            return channel.read(buffer);
        } else if((containingSegment = mStoredSegments.getContainer(s)) != null){
            //read from data store
            return -1;
        } else {
            throw new IOException("no data for offset: " + offset);
        }

    }

    public synchronized void truncate(long size) throws IOException {

        if(size == 0) {
            mStoredSegments.clear();
        } else {
            //TODO: figure out how to do this properly
        }

        if(mTempBufferFile != null) {
            mTempBufferFile.getChannel().truncate(size);
        }
        mTransientSegments.truncate(size);

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
