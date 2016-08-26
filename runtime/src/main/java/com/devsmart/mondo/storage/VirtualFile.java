package com.devsmart.mondo.storage;


import com.devsmart.IOUtils;
import com.devsmart.mondo.data.*;
import com.devsmart.mondo.kademlia.ID;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class VirtualFile implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualFile.class);


    private final FilesystemStorage mFilesystemStorage;
    private final VirtualFilesystem mVirtualFS;

    public VirtualFilesystem.Path mPath;
    public FileMetadata mMetadata;
    public int mHandle;
    private SegmentList mTransientSegments = new SegmentList();
    private ArrayList<SecureSegment> mStoredSegments;
    private File mTempFile;
    private RandomAccessFile mTempBufferFile;
    private SecureSegment mCachedFSSegment;
    private byte[] mCachedBuff;

    public VirtualFile(VirtualFilesystem virtualFS, FilesystemStorage storage) {
        Preconditions.checkArgument(virtualFS != null && storage != null);
        mVirtualFS = virtualFS;
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
        return Math.max(mTransientSegments.end(), mMetadata.getSize());
    }

    public DataFile getDataFile() {
        DataFile retval = new DataFile();
        retval.mParts = new FilePart[mStoredSegments.size()];
        int i = 0;
        for(Segment s : mStoredSegments) {
            SecureSegment ss = (SecureSegment) s;
            retval.mParts[i++] = new FilePart((int) ss.length, ss.secureHash.asBytes());
        }

        return retval;
    }

    public void setDataFile(DataFile datafile) {
        long offset = 0;
        if(datafile != null && datafile.mParts != null && datafile.mParts.length >= 0) {
            mStoredSegments = new ArrayList<SecureSegment>(datafile.mParts.length);
            for (int i = 0; i < datafile.mParts.length; i++) {
                FilePart part = datafile.mParts[i];
                final long size = part.getSize();
                mStoredSegments.add(new SecureSegment(offset, size, part.getSha1Checksum()));
                offset += size;
            }

        } else {
            mStoredSegments = new ArrayList<SecureSegment>();
        }
    }


    @Override
    public void close() throws IOException {
        fsync();
        if(mTempBufferFile != null) {
            mTempBufferFile.close();
            mTempBufferFile = null;
            mTempFile.delete();
            mTempFile = null;
        }
    }

    public synchronized void fsync() throws IOException {


        /*
            create overlay stream
            pump the overlay stream while running buzhash to break it up into new segments
            write out new segments
         */

        if(!mTransientSegments.isEmpty()) {
            DataBreakerInputStream breaker = new DataBreakerInputStream(new OverlayInputStream(getBackingFile(), mTransientSegments, mFilesystemStorage, mStoredSegments),
                    Hashing.sha1(), 15);
            breaker.setCallback(new DataBreakerInputStream.Callback() {
                @Override
                public void onNewSegment(SecureSegment segment, InputStream in) {
                    try {
                        ID id = mFilesystemStorage.store(in);
                        mStoredSegments.add(segment);
                    } catch (Exception e) {
                        LOGGER.error("", e);
                        Throwables.propagate(e);
                    }
                }
            });
            IOUtils.pump(breaker, new NullOutputStream());

            mVirtualFS.writeBack(this);
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

    private void loadFSSegment(SecureSegment segment) throws IOException {
        if(mCachedFSSegment == null || !mCachedFSSegment.getID().equals(segment.getID())) {
            InputStream in = mFilesystemStorage.load(segment.getID());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.pump(in, out);
            mCachedBuff = out.toByteArray();
            mCachedFSSegment = segment;
        }
    }

    private SecureSegment getStoredSegmentContainer(Segment s) {
        for(SecureSegment storedSegment : mStoredSegments) {
            if(storedSegment.contains(s)) {
                return storedSegment;
            }
        }
        return null;
    }

    public synchronized int read(ByteBuffer buffer, long size, long offset) throws IOException {
        SecureSegment storedSegment;
        Segment s = new Segment(offset, size);

        if(mTransientSegments.getContainer(s) != null) {
            RandomAccessFile transientFile = getBackingFile();
            FileChannel channel = transientFile.getChannel();
            channel.position(offset);
            return channel.read(buffer);
        } else if((storedSegment = getStoredSegmentContainer(s)) != null){
            //read from data store
            loadFSSegment(storedSegment);
            final int bufOffset = (int) (offset-mCachedFSSegment.offset);
            int len = (int)Math.min(size, mCachedBuff.length-bufOffset);
            buffer.put(mCachedBuff, bufOffset, len);
            return len;
        } else {
            return 0;
            //throw new IOException("no data for offset: " + offset);
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
