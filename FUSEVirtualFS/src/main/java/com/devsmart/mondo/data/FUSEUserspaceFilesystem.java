package com.devsmart.mondo.data;

import co.paralleluniverse.fuse.Fuse;
import co.paralleluniverse.javafs.JavaFS;
import com.devsmart.mondo.UserspaceFilesystem;
import com.devsmart.mondo.storage.FilesystemStorage;
import com.devsmart.mondo.storage.VirtualFilesystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;


public class FUSEUserspaceFilesystem implements UserspaceFilesystem {

    private FUSEVirtualFilesystem mFUSEVirtualFS;
    private File mMountPath;

    @Override
    public void init(VirtualFilesystem virtualFilesystem, FilesystemStorage storage) {
        mFUSEVirtualFS = new FUSEVirtualFilesystem(virtualFilesystem);
    }

    @Override
    public void mount(File file) throws IOException {
        mMountPath = file;
        mMountPath.mkdirs();
        Fuse.mount(mFUSEVirtualFS, Paths.get(mMountPath.getAbsolutePath()), false, false, null);
    }

    @Override
    public void unmount() throws IOException {
        Fuse.unmount(Paths.get(mMountPath.getAbsolutePath()));
    }
}
