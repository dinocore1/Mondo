package com.devsmart.mondo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;


public class MondoFilesystemProvider extends FileSystemProvider {

    public static final Logger LOGGER = LoggerFactory.getLogger(MondoFilesystemProvider.class);

    private final MondoFileStore mStore;

    public MondoFilesystemProvider(MondoFileStore fileStore) {
        mStore = fileStore;
    }

    @Override
    public String getScheme() {
        return MondoFilesystem.URI_SCHEME;
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        throw new UnsupportedOperationException(
                "This method should not be called directly; "
                        + "use FileSystems.getFileSystem(URI) instead.");
    }

    @Override
    public Path getPath(URI uri) {
        throw new UnsupportedOperationException(
                "This method should not be called directly; " + "use Paths.get(URI) instead.");
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        LOGGER.trace("newByteChannel() {}", path);
        return null;
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        LOGGER.trace("newDirectoryStream() {}", dir);
        return null;
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        LOGGER.trace("createDirectory() {}", dir);
    }

    @Override
    public void delete(Path path) throws IOException {
        LOGGER.trace("delete(): {}", path);
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        LOGGER.trace("copy(): {} {}", source, target);
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        LOGGER.trace("move(): {} {}", source, target);
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        LOGGER.trace("isSameFile(): {} {}", path, path2);
        return false;
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        LOGGER.trace("isHidden(): {}", path);
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        LOGGER.trace("getFileStore(): {}", path);
        return null;
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        LOGGER.trace("checkAccess(): {}", path);
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        LOGGER.trace("getFileAttributeView(): {}", path);
        return null;
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        LOGGER.trace("readAttributes(): {}", path);
        Lock lock = mStore.readLock();
        try {


        } finally {
            lock.unlock();
        }
        return null;
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        LOGGER.trace("readAttributes(): {}", path);
        return null;
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        LOGGER.trace("setAttribute(): {}", path);
    }

}
