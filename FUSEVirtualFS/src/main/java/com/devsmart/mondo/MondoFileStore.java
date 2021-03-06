package com.devsmart.mondo;


import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashCode;
import org.mapdb.Atomic;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkState;

public class MondoFileStore implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MondoFileStore.class);

    private static final Function<MondoFSPath, Object[]> TO_DB_KEY = new Function<MondoFSPath, Object[]>() {

        private String pathPartToStr(MondoFSPath path) {
            String retval;
            if(path != null) {
                retval = path.toString();
            } else {
                retval = "";
            }
            return retval;
        }

        @Override
        public Object[] apply(MondoFSPath input) {
            input = input.normalize();
            return new Object[]{pathPartToStr(input.getParent()), pathPartToStr(input.getFileName())};
        }
    };

    public static final int BUFFER_SIZE = 8192;

    private DB mDB;
    private final File mDataRoot;
    private final File mTempFileDir;
    private final File mDataFileDir;
    private final FileSystemState mState;
    private final ReadWriteLock mRWLock = new ReentrantReadWriteLock();
    private final Lock mReadLock;
    private final Lock mWriteLock;
    private final BTreeMap<Object[], FileMetadata> mFileMetadata;
    final BTreeMap<Long, BlockGroup> mBlockGroups;
    private final Atomic.Long mBlockGroupId;
    private final ScheduledExecutorService mIOScheduler = Executors.newScheduledThreadPool(1);


    MondoFileStore(DB db, File dataRoot) {
        mDB = db;
        mDataRoot = dataRoot;

        mTempFileDir = new File(mDataRoot, "tmp");

        if(!mTempFileDir.exists()) {
            checkState(mTempFileDir.mkdirs());
        }

        mDataFileDir = new File(mDataRoot, "data");
        if(!mDataFileDir.exists()) {
            checkState(mDataFileDir.mkdirs());
        }

        mState = new FileSystemState();
        mReadLock = mRWLock.readLock();
        mWriteLock = mRWLock.writeLock();

        mFileMetadata = mDB.treeMap("fileMetadata")
                .keySerializer(new SerializerArrayTuple(Serializer.STRING_DELTA, Serializer.STRING))
                .valueSerializer(FileMetadata.SERIALIZER)
                .createOrOpen();

        final Object[] rootKey = new Object[]{"", ""};
        if(!mFileMetadata.containsKey(rootKey)) {
            FileMetadata root = new FileMetadata();
            root.flags = FileMetadata.FLAG_DIR;
            mFileMetadata.put(rootKey, root);
        }


        mBlockGroups = mDB.treeMap("blockGroups")
                .keySerializer(Serializer.LONG_DELTA)
                .valueSerializer(BlockGroup.SERIALIZER)
                .createOrOpen();

        mBlockGroupId = mDB.atomicLong("blockGroupId")
                .createOrOpen();

    }

    @Override
    public void close() throws IOException {
        mDB.close();
    }

    File createTmpFile() {
        return new File(mTempFileDir, UUID.randomUUID().toString() + ".dat");
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

    public FileMetadata lookUpWithLock(MondoFSPath path) {
        FileMetadata retval;
        final Object[] key = TO_DB_KEY.apply(path);

        mReadLock.lock();
        try {
            retval = mFileMetadata.get(key);
        } finally {
            mReadLock.unlock();
        }
        return retval;
    }

    public DirectoryStream<Path> createDirectoryStream(MondoFSPath dir, DirectoryStream.Filter<? super Path> filter) {
        return new DBDirectoryStream(dir, filter);
    }

    public File getFileBlock(HashCode hash) {
        String filename = hash.toString();
        File retval = new File(mDataFileDir, filename.substring(0, 2));
        retval = new File(retval, filename.substring(2, 4));
        retval = new File(retval, filename);
        return retval;
    }

    public void delete(MondoFSPath path) {
        Object[] key = TO_DB_KEY.apply(path);
        mWriteLock.lock();
        try {
            mFileMetadata.remove(key);
            mDB.commit();
        } finally {
            mWriteLock.unlock();
        }
    }

    private class DBDirectoryStream implements DirectoryStream<Path> {

        private final MondoFSPath mDir;
        private final ConcurrentNavigableMap<Object[], FileMetadata> mDirFileSet;
        private final Predicate<Path> mPredicate;

        DBDirectoryStream(MondoFSPath dir, DirectoryStream.Filter<? super Path> filter) {
            mDir = dir.normalize();
            String dirStr = dir.toString();
            mDirFileSet = mFileMetadata.subMap(new Object[]{dirStr, ""}, new Object[]{dirStr, null});

            mPredicate = createPredicate(filter);
        }

        @Override
        public Iterator<Path> iterator() {
            Iterable<Path> paths = Iterables.transform(mDirFileSet.keySet(), TO_PATH);
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

        FileMetadata metadata = lookUpWithLock(path);
        if(Iterables.contains(options, StandardOpenOption.CREATE_NEW) && metadata != null){
            throw new FileAlreadyExistsException(path.toString());
        }

        if(Iterables.contains(options, StandardOpenOption.CREATE_NEW) || Iterables.contains(options, StandardOpenOption.CREATE)) {
            metadata = createNewFile(path);
        }

        FileChannel scratchFileChannel = null;

        int openMode = 0;
        if(Iterables.contains(options, StandardOpenOption.WRITE)) {
            File scratchFile = createTmpFile();
            scratchFileChannel = FileChannel.open(scratchFile.toPath(),
                    StandardOpenOption.READ, StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.DELETE_ON_CLOSE);
            openMode |= MondoFileChannel.MODE_WRITE;
        }


        if(Iterables.contains(options, StandardOpenOption.READ)) {
            openMode |= MondoFileChannel.MODE_READ;
        }

        MondoFileChannel mfc = new MondoFileChannel(openMode, scratchFileChannel, metadata, this);
        mfc.mPath = path;
        return mfc;

    }

    void onFileChannelClose(MondoFileChannel mondoFileChannel) throws IOException {
        if((mondoFileChannel.mOpenMode & MondoFileChannel.MODE_WRITE) > 0) {
            mIOScheduler.execute(createFlushFileTask(mondoFileChannel));
        }

    }

    private Runnable createFlushFileTask(final MondoFileChannel fileChannel) {

        return new Runnable() {
            @Override
            public void run() {
                try {
                    final WriteOutBlockAction writeOutAction = new WriteOutBlockAction();
                    writeOutAction.mFileStore = MondoFileStore.this;
                    writeOutAction.mFileChannel = fileChannel;

                    BlockGroup bg = writeOutAction.doIt();
                    LOGGER.info("flush file: {} {}", fileChannel.mPath, bg.checksum);

                    mWriteLock.lock();
                    try {
                        mBlockGroups.put(fileChannel.mMetadata.blockId, bg);

                        Object[] key = TO_DB_KEY.apply(fileChannel.mPath);
                        mFileMetadata.put(key, fileChannel.mMetadata);

                        mDB.commit();

                    } finally {
                        mWriteLock.unlock();
                    }

                } catch (Exception e) {
                    LOGGER.error("", e);
                } finally {
                    try {
                        fileChannel.mScratchFile.close();
                    } catch (IOException e) {
                        LOGGER.error("", e);
                    }
                }


            }
        };
    }

    private FileMetadata createNewFile(MondoFSPath path) {
        FileMetadata file;

        path = path.normalize();

        file = new FileMetadata();
        file.blockId = mBlockGroupId.getAndIncrement();
        file.flags = 0;
        file.creationTime = System.currentTimeMillis();
        file.lastAccessedTime = file.creationTime;
        file.lastModifiedTime = file.creationTime;

        Object[] key = TO_DB_KEY.apply(path);

        mWriteLock.lock();
        try {
            mFileMetadata.put(key, file);
            mDB.commit();
        } finally {
            mWriteLock.unlock();
        }
        return file;
    }


}
