package com.devsmart.mondo;

import java.io.File;
import java.nio.file.FileSystem;


public class MondoFS {

    static FileSystem createMondoFS() {
        File root = new File(".mondofs");
        MondoFileStore fileStore = new MondoFileStore(root);
        MondoFilesystemProvider provider = new MondoFilesystemProvider(fileStore);
        MondoFilesystem retval = new MondoFilesystem(provider, fileStore);
        return retval;
    }
}
