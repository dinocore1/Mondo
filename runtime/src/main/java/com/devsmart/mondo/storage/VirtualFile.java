package com.devsmart.mondo.storage;


public class VirtualFile {

    String mName;
    FileMetadata mMetadata;

    public boolean isDirectory() {
        return (mMetadata.mFlags & FileMetadata.FLAG_DIR) > 0;
    }

    public boolean isFile() {
        return (mMetadata.mFlags & FileMetadata.FLAG_DIR) == 0;
    }

    public String getName() {
        return mName;
    }



}
