package com.devsmart.mondo;


import com.devsmart.mondo.data.Buzhash;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class WriteOutBlockAction {

    public static final HashFunction HASH_FUNCTION = Hashing.sha1();
    public static final int BUFFER_SIZE = 8196;
    public static final long HASH_MASK = (1 << 16) - 1;

    MondoFileStore mFileStore;
    MondoFileChannel mFileChannel;

    private File mTempFile;
    private FileOutputStream mOutputStream;
    private Hasher mBlockHash;
    private Hasher mChecksumHash;
    private List<HashCode> mBlockHashList = new ArrayList<HashCode>();


    public void doIt() throws IOException {

        mChecksumHash = HASH_FUNCTION.newHasher();

        mTempFile = mFileStore.createTmpFile();
        FileOutputStream outputStream = new FileOutputStream(mTempFile);
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

        Buzhash buzHash = new Buzhash(40);
        mBlockHash = HASH_FUNCTION.newHasher();

        mFileChannel.mOpenMode |= MondoFileChannel.MODE_READ;

        mFileChannel.position(0);

        while(mFileChannel.position() < mFileChannel.size()) {

            ensureOutputStream();

            mFileChannel.read(buffer);
            buffer.flip();
            while(buffer.remaining() > 0) {
                final byte value = buffer.get();

                mChecksumHash.putByte(value);
                mBlockHash.putByte(value);
                mOutputStream.write(value);
                long hash = buzHash.addByte(value);


                if((hash & HASH_MASK) == 0) {
                    segment();
                }
            }
            buffer.clear();
        }



    }

    private void ensureOutputStream() throws IOException {

        if(mOutputStream == null) {
            mOutputStream = new FileOutputStream(mTempFile);
        }
    }

    private void segment() throws IOException {

        mOutputStream.close();

        final HashCode secureHash = mBlockHash.hash();
        mBlockHashList.add(secureHash);
        File f = mFileStore.getFileBlock(secureHash);
        mTempFile.renameTo(f);

        mOutputStream = null;
    }

}
