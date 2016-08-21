package com.devsmart.mondo.storage;


public class VirtualFile {


    public VirtualFilesystem.Path mPath;
    public FileMetadata mMetadata;
    public int mHandle;


    public boolean isDirectory() {
        return (mMetadata.mFlags & FileMetadata.FLAG_DIR) > 0;
    }

    public boolean isFile() {
        return (mMetadata.mFlags & FileMetadata.FLAG_DIR) == 0;
    }

    public String getName() {
        return mPath.getName();
    }



}
