package com.devsmart.mondo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;


public class MondoFilesystem extends FileSystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(MondoFilesystem.class);

    public static final String URI_SCHEME = "mondofs";
    private final MondoFilesystemProvider mProvider;
    private final MondoFileStore mFileStore;

    MondoFilesystem(MondoFilesystemProvider provider, MondoFileStore fileStore) {
        mProvider = provider;
        mFileStore = fileStore;
    }

    @Override
    public FileSystemProvider provider() {
        return mProvider;
    }

    @Override
    public void close() throws IOException {
        LOGGER.trace("close()");
        mFileStore.state().close();
    }

    @Override
    public boolean isOpen() {
        LOGGER.trace("isOpen()");
        return mFileStore.state().isOpen();
    }

    @Override
    public boolean isReadOnly() {
        LOGGER.trace("isReadOnly()");
        return false;
    }

    @Override
    public String getSeparator() {
        LOGGER.trace("getSeparator()");
        return null;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        LOGGER.trace("getRootDirectories()");
        return null;
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        LOGGER.trace("getFileStores()");
        return null;
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        LOGGER.trace("supportedFileAttributeViews()");
        return null;
    }

    @Override
    public Path getPath(String first, String... more) {
        LOGGER.trace("getPath {} {}", first, more);
        mFileStore.state().checkOpen();
        return MondoFSPath.parsePath(this, first, more);
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        LOGGER.trace("getPathMatcher() {}", syntaxAndPattern);
        return null;
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        LOGGER.trace("getUserPrincipalLookupService()");
        return null;
    }

    @Override
    public WatchService newWatchService() throws IOException {
        LOGGER.trace("newWatchService()");
        return null;
    }
}
