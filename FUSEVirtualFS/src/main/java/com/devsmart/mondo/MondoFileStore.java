package com.devsmart.mondo;


import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.mapdb.*;
import org.mapdb.serializer.SerializerArray;
import org.mapdb.serializer.SerializerArrayTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.*;

public class MondoFileStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(MondoFileStore.class);

    private final File mStorageRoot;
    private final FileSystemState mState;
    private final ReadWriteLock mRWLock = new ReentrantReadWriteLock();
    private final Lock mReadLock;
    private final Lock mWriteLock;
    private final NavigableSet<Object[]> mFiles;
    private File mDBFile;
    private DB mDB;


    MondoFileStore(File storageRoot) {
        checkNotNull(storageRoot);

        if(!storageRoot.exists()) {
            checkState(storageRoot.mkdirs());
        }

        checkState(storageRoot.isDirectory());

        mStorageRoot = storageRoot;
        mState = new FileSystemState();
        mReadLock = mRWLock.readLock();
        mWriteLock = mRWLock.writeLock();

        mDBFile = new File(storageRoot, "mondo");
        mDB = DBMaker.fileDB(mDBFile)
                .transactionEnable()
                .make();

        mFiles = mDB.treeSet("files")
                .serializer(new SerializerArrayTuple(Serializer.STRING_DELTA, Serializer.STRING, MondoFile.SERIALIZER))
                .createOrOpen();

        SortedSet<Object[]> rootSet = mFiles.subSet(new Object[]{"", ""}, new Object[]{"", "", null});
        if(rootSet.isEmpty()) {
            MondoFile root = new MondoFile();
            root.isFile = false;
            mFiles.add(new Object[]{"", "", root});
        }


    }



    public FileSystemState state() {
        return mState;
    }

    public Lock readLock() {
        mReadLock.lock();
        return mReadLock;
    }

    public Lock writeLock() {
        mWriteLock.lock();
        return mWriteLock;
    }

    public MondoFile lookUpWithLock(MondoFSPath path) {
        MondoFile retval = null;
        path = path.normalize();

        String parentStr;
        MondoFSPath parent = path.getParent();
        if(parent != null) {
            parentStr = parent.toString();
        } else {
            parentStr = "";
        }

        String filenameStr = null;
        MondoFSPath filename = path.getFileName();
        if(filename != null) {
            filenameStr = filename.toString();
        } else {
            filenameStr = "";
        }


        mReadLock.lock();
        try {

            SortedSet<Object[]> file = mFiles.subSet(new Object[]{parentStr, filenameStr}, new Object[]{parentStr, filenameStr, null});
            if(!file.isEmpty()) {
                Object[] record = file.first();
                retval = (MondoFile) record[2];
            }

        } finally {
            mReadLock.unlock();
        }
        return retval;
    }

    public DirectoryStream<Path> createDirectoryStream(MondoFSPath dir, DirectoryStream.Filter<? super Path> filter) {
        return new DBDirectoryStream(dir, filter);
    }

    private class DBDirectoryStream implements DirectoryStream<Path> {

        private final MondoFSPath mDir;
        private final SortedSet<Object[]> mDirFileSet;
        private final Predicate<Path> mPredicate;

        DBDirectoryStream(MondoFSPath dir, DirectoryStream.Filter<? super Path> filter) {
            mDir = dir.normalize();
            String dirStr = dir.toString();
            mDirFileSet = mFiles.subSet(new Object[]{dirStr, "", null}, new Object[]{dirStr, null, null});
            mPredicate = createPredicate(filter);
        }

        @Override
        public Iterator<Path> iterator() {
            Iterable<Path> paths = Iterables.transform(mDirFileSet, TO_PATH);
            paths = Iterables.filter(paths, mPredicate);
            return paths.iterator();
        }

        @Override
        public void close() throws IOException {

        }

        private Function<Object[], Path> TO_PATH = new Function<Object[], Path>() {
            @Override
            public Path apply(Object[] input) {
                String root = (String) input[0];
                String names = (String) input[1];

                MondoFSPath retval = MondoFSPath.createPath(mDir.getFileSystem(), MondoFSPath.TO_NAME.apply(root),
                        Iterables.transform(MondoFSPath.PATH_SPLITTER.split(names), MondoFSPath.TO_NAME));
                return retval;

            }
        };

        private Predicate<Path> createPredicate(final Filter<? super Path> filter) {
            return new Predicate<Path>() {
                @Override
                public boolean apply(Path input) {
                    boolean accept = false;
                    try {
                        accept = filter.accept(input);
                    } catch (IOException e) {
                        LOGGER.error("", e);
                    }
                    return accept;
                }
            };
        }
    }

    public SeekableByteChannel newByteChannel(MondoFSPath path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) throws IOException {

        MondoFile file = lookUpWithLock(path);
        if(Iterables.contains(options, StandardOpenOption.CREATE_NEW) && file != null){
            throw new FileAlreadyExistsException(path.toString());
        }

        if(Iterables.contains(options, StandardOpenOption.CREATE_NEW) || Iterables.contains(options, StandardOpenOption.CREATE)) {
            file = createNewFile(path);
        }

        return new MondoFileChannel(file);

    }

    private MondoFile createNewFile(MondoFSPath path) {
        MondoFile file;
        mWriteLock.lock();
        try {
            path = path.normalize();

            file = new MondoFile();
            file.isFile = true;
            file.creationTime = System.currentTimeMillis();
            file.lastAccessedTime = file.creationTime;
            file.lastModifiedTime = file.creationTime;

            mFiles.add(new Object[]{path.getParent().toString(), path.getFileName().toString(), file});

        } finally {
            mWriteLock.unlock();
        }
        return file;
    }

    private class MondoFileChannel implements SeekableByteChannel {

        private final MondoFile mFile;
        private boolean mIsOpen = false;
        private long mPosition = 0;
        private long mSize = 0;

        public MondoFileChannel(MondoFile file) {
            mFile = file;
            mIsOpen = true;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return 0;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            int bytesRead = 0;
            byte[] buf = new byte[4096];
            int remaining = src.remaining();

            bytesRead = Math.min(buf.length, remaining);
            src.get(buf, 0, bytesRead);

            mPosition += bytesRead;
            if(mPosition > mSize) {
                mSize = mPosition;
            }
            return bytesRead;
        }

        @Override
        public long position() throws IOException {
            return mPosition;
        }

        @Override
        public SeekableByteChannel position(long newPosition) throws IOException {
            return null;
        }

        @Override
        public long size() throws IOException {
            return mSize;
        }

        @Override
        public SeekableByteChannel truncate(long size) throws IOException {
            return null;
        }

        @Override
        public boolean isOpen() {
            return mIsOpen;
        }

        @Override
        public void close() throws IOException {

            mFile.size = mSize;
            mWriteLock.lock();
            try {
                //TODO: update the file entry
            } finally {
                mWriteLock.unlock();
            }

            mIsOpen = false;


        }
    }


}
