package com.devsmart.mondo;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.ClosedFileSystemException;
import java.util.concurrent.atomic.AtomicBoolean;


public class FileSystemState implements Closeable {

    private final AtomicBoolean open = new AtomicBoolean(true);

    public boolean isOpen() {
        return open.get();
    }

    /**
     * Checks that the file system is open, throwing {@link ClosedFileSystemException} if it is not.
     */
    public void checkOpen() {
        if (!open.get()) {
            throw new ClosedFileSystemException();
        }
    }

    @Override
    public void close() throws IOException {


    }
}
