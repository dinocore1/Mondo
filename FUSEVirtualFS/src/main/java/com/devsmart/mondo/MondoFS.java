package com.devsmart.mondo;

import java.nio.file.FileSystem;


public class MondoFS {

    static FileSystem createMondoFS() {
        MondoFileStore fileStore = new MondoFileStore();
        MondoFilesystemProvider provider = new MondoFilesystemProvider(fileStore);
        MondoFilesystem retval = new MondoFilesystem(provider, fileStore);
        return retval;
    }
}
