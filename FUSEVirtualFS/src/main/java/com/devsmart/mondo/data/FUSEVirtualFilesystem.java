package com.devsmart.mondo.data;


import co.paralleluniverse.fuse.DirectoryFiller;
import co.paralleluniverse.fuse.StructFuseFileInfo;
import com.devsmart.mondo.storage.VirtualFile;
import com.devsmart.mondo.storage.VirtualFilesystem;
import co.paralleluniverse.fuse.*;
import co.paralleluniverse.fuse.AbstractFuseFilesystem;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class FUSEVirtualFilesystem extends AbstractFuseFilesystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(FUSEVirtualFilesystem.class);


    private final VirtualFilesystem mVirtualFS;

    public FUSEVirtualFilesystem(VirtualFilesystem virtualFilesystem) {
        mVirtualFS = virtualFilesystem;
    }

    @Override
    protected int readdir(String path, StructFuseFileInfo info, DirectoryFiller filler) {
        LOGGER.info("readdir {}", path);

        if(!"/".equals(path) && !mVirtualFS.pathExists(path)) {
            return -ErrorCodes.ENOENT();
        }

        Iterable<VirtualFile> files = mVirtualFS.getFilesInDir(path);
        Iterable<String> filenames = Iterables.transform(files, new Function<VirtualFile, String>() {
            @Override
            public String apply(VirtualFile input) {
                return input.getName();
            }
        });

        filler.add(filenames);

        return 0;
    }

    @Override
    protected int getattr(String path, StructStat stat) {
        LOGGER.info("getattr {}", path);
        if("/".equals(path)) {
            stat.mode(TypeMode.S_IFDIR | 0775);
            return 0;
        }

        VirtualFile file = mVirtualFS.getFile(path);
        if(file == null) {
            return -ErrorCodes.ENOENT();
        } else {
            if(file.isDirectory()) {
                stat.mode(TypeMode.S_IFDIR | 0755);
            } else {
                stat.mode(TypeMode.S_IFREG | 0644);
            }
            return 0;
        }
    }

    @Override
    protected int mkdir(String path, long mode) {
        LOGGER.info("mkdir {} {}", path, mode);
        mVirtualFS.mkdir(path);
        return 0;
    }
}
