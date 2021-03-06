package com.devsmart.mondo.storage;


import com.devsmart.mondo.kademlia.ID;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.UUID;

public class FilesystemStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemStorage.class);
    private static final HashFunction HASH_FUNCTION = Hashing.sha1();

    private final File mRootDir;
    private final File mTempDir;

    public FilesystemStorage(File rootDir) {
        mRootDir = rootDir;
        mTempDir = new File(mRootDir, "tmp");
        mTempDir.mkdirs();
    }

    public File createTempFile() throws IOException {
        return new File(mTempDir, UUID.randomUUID().toString() + ".dat");
    }

    public boolean contains(ID id) {
        File destFile = getFile(id);
        return destFile.exists();
    }

    public ID store(InputStream in) throws IOException {
        File tempFile = File.createTempFile("baking", ".dat", mTempDir);
        Hasher hasher = HASH_FUNCTION.newHasher();

        FileOutputStream fout = new FileOutputStream(tempFile);

        byte[] buffer = new byte[4096];
        int bytesRead;
        while((bytesRead = in.read(buffer, 0, buffer.length)) > 0) {
            hasher.putBytes(buffer, 0, bytesRead);
            fout.write(buffer, 0, bytesRead);
        }

        fout.close();
        in.close();

        final HashCode checksum = hasher.hash();
        final ID id = new ID(checksum.asBytes(), 0);

        File destFile = getFile(id);
        if(!destFile.exists()) {
            destFile.getParentFile().mkdirs();
            tempFile.renameTo(destFile);
        } else {
            LOGGER.warn("already exists: {}", id);
        }

        return id;
    }

    public InputStream load(ID id) throws IOException {
        File file = getFile(id);
        if(file.exists()) {
            return new FileInputStream(file);
        } else {
            return null;
        }
    }

    public boolean delete(ID id) {
        File file = getFile(id);
        if(file.exists()) {
            return file.delete();
        } else {
            return false;
        }
    }

    public File getFile(ID id) {
        String filename = id.toString(BaseEncoding.base16());

        File file = new File(mRootDir, filename.substring(0, 2));
        file = new File(file, filename.substring(2, 4));
        file = new File(file, filename);
        return file;
    }
}
