package com.devsmart.mondo;


import com.devsmart.mondo.storage.FilesystemStorage;
import com.devsmart.mondo.storage.VirtualFilesystem;

import java.io.File;
import java.io.IOException;

public interface UserspaceFilesystem {
    void init(VirtualFilesystem virtualFilesystem, FilesystemStorage storage);
    void mount(File file) throws IOException;
    void unmount() throws IOException;
}
