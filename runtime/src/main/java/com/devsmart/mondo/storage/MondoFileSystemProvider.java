package com.devsmart.mondo.storage;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class MondoFileSystemProvider extends FileSystemProvider {

    public static final String SCHEME = "mondofs";

    private static final Map<String, MondoFilesystem> mFileSystems;

    static {
        mFileSystems = new HashMap<String, MondoFilesystem>();
    }


    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public synchronized FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        final String key = uri.getAuthority();
        if(mFileSystems.containsKey(key)) {
            throw new FileSystemAlreadyExistsException("auth");
        }

        MondoFilesystem retval = new MondoFilesystem(this, key);
        mFileSystems.put(key, retval);

        return retval;
    }

    private MondoFilesystem getMondoFS(URI uri) {
        final String key = uri.getAuthority();
        MondoFilesystem retval = mFileSystems.get(key);
        if(retval == null) {
            throw new FileSystemNotFoundException(key);
        }
        return retval;
    }

    synchronized void close(MondoFilesystem mondoFilesystem) {
        final String key = mondoFilesystem.getKey();
        mFileSystems.remove(key);
    }

    @Override
    public synchronized FileSystem getFileSystem(URI uri) {
        return getMondoFS(uri);
    }

    @Override
    public Path getPath(URI uri) {
        return null;
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        MondoFilesystem mondo = (MondoFilesystem) path.getFileSystem();
        return mondo.newFileChannel(path, options, attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        return new DirectoryStream<Path>() {
            @Override
            public Iterator<Path> iterator() {
                return new Iterator<Path>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Path next() {
                        return null;
                    }

                    @Override
                    public void remove() {

                    }
                };
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {

    }

    @Override
    public void delete(Path path) throws IOException {

    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {

    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {

    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        return false;
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        return null;
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {

    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return null;
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        return (A) new BasicFileAttributes() {

            @Override
            public FileTime lastModifiedTime() {
                return FileTime.from(0, TimeUnit.SECONDS);
            }

            @Override
            public FileTime lastAccessTime() {
                return FileTime.from(0, TimeUnit.SECONDS);
            }

            @Override
            public FileTime creationTime() {
                return FileTime.from(0, TimeUnit.SECONDS);
            }

            @Override
            public boolean isRegularFile() {
                return false;
            }

            @Override
            public boolean isDirectory() {
                return false;
            }

            @Override
            public boolean isSymbolicLink() {
                return false;
            }

            @Override
            public boolean isOther() {
                return false;
            }

            @Override
            public long size() {
                return 0;
            }

            @Override
            public Object fileKey() {
                return null;
            }
        };
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        return null;
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {

    }
}
