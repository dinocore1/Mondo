package com.devsmart.mondo.storage;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;


public class MondoFilesystem extends FileSystem {

    private final String mKey;
    private MondoFileSystemProvider mProvider;

    public MondoFilesystem(MondoFileSystemProvider provider, String key) {
        mProvider = provider;
        mKey = key;
    }

    public String getKey() {
        return mKey;
    }

    @Override
    public FileSystemProvider provider() {
        return mProvider;
    }

    @Override
    public void close() throws IOException {
        mProvider.close(this);

    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return null;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return null;
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return null;
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return null;
    }

    @Override
    public Path getPath(String first, String... more) {
        return null;
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        return null;
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        return null;
    }

    @Override
    public WatchService newWatchService() throws IOException {
        return null;
    }
}
