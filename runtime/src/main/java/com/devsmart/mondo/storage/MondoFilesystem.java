package com.devsmart.mondo.storage;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
import java.util.Set;


public class MondoFilesystem extends FileSystem {

    private final String mKey;
    private MondoFileSystemProvider mProvider;

    class MondoPath implements Path {

        private final String mFirst;
        private final String[] mMore;

        public MondoPath(String first, String[] more) {
            mFirst = first;
            mMore = more;
        }

        @Override
        public FileSystem getFileSystem() {
            return MondoFilesystem.this;
        }

        @Override
        public boolean isAbsolute() {
            return false;
        }

        @Override
        public Path getRoot() {
            return null;
        }

        @Override
        public Path getFileName() {
            return null;
        }

        @Override
        public Path getParent() {
            return null;
        }

        @Override
        public int getNameCount() {
            return 0;
        }

        @Override
        public Path getName(int index) {
            return null;
        }

        @Override
        public Path subpath(int beginIndex, int endIndex) {
            return null;
        }

        @Override
        public boolean startsWith(Path other) {
            return false;
        }

        @Override
        public boolean startsWith(String other) {
            return false;
        }

        @Override
        public boolean endsWith(Path other) {
            return false;
        }

        @Override
        public boolean endsWith(String other) {
            return false;
        }

        @Override
        public Path normalize() {
            return null;
        }

        @Override
        public Path resolve(Path other) {
            return null;
        }

        @Override
        public Path resolve(String other) {
            return null;
        }

        @Override
        public Path resolveSibling(Path other) {
            return null;
        }

        @Override
        public Path resolveSibling(String other) {
            return null;
        }

        @Override
        public Path relativize(Path other) {
            return null;
        }

        @Override
        public URI toUri() {
            return null;
        }

        @Override
        public Path toAbsolutePath() {
            return null;
        }

        @Override
        public Path toRealPath(LinkOption... options) throws IOException {
            return null;
        }

        @Override
        public File toFile() {
            return null;
        }

        @Override
        public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
            return null;
        }

        @Override
        public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
            return null;
        }

        @Override
        public Iterator<Path> iterator() {
            return null;
        }

        @Override
        public int compareTo(Path other) {
            return 0;
        }

        @Override
        public boolean equals(Object other) {
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return null;
        }
    }

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
        return new MondoPath(first, more);
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

    public SeekableByteChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) {
        return null;
    }
}
