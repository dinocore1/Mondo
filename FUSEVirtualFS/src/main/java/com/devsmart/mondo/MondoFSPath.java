package com.devsmart.mondo;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;


public class MondoFSPath implements Path {

    private static final Logger LOGGER = LoggerFactory.getLogger(MondoFSPath.class);

    public static final Joiner PATH_JOINER = Joiner.on('/');
    public static final Splitter PATH_SPLITTER = Splitter.on('/');

    private static final Predicate<Object> NOT_EMPTY = new Predicate<Object>() {
        @Override
        public boolean apply(Object input) {
            return !input.toString().isEmpty();
        }
    };

    public static final Function<String, Name> TO_NAME = new Function<String, Name>() {
        @Override
        public Name apply(String input) {
            return Name.create(input);
        }
    };

    public static MondoFSPath createPath(MondoFilesystem fs, Name root, Iterable<Name> names) {
        ImmutableList<Name> nameList = ImmutableList.copyOf(Iterables.filter(names, NOT_EMPTY));
        return new MondoFSPath(fs, root, nameList);
    }

    public static MondoFSPath createFileName(MondoFilesystem fs, Name name) {
        return createPath(fs, null, ImmutableList.of(name));
    }

    public static MondoFSPath parsePath(MondoFilesystem fs, String first, String[] more) {
        String pathStr = PATH_JOINER.join(Iterables.filter(Lists.asList(first, more), NOT_EMPTY));

        Name root = pathStr.startsWith("/") ? Name.create("/") : null;
        Iterable<Name> names = Iterables.transform(PATH_SPLITTER.split(pathStr), TO_NAME);
        return createPath(fs, root, names);
    }

    private final MondoFilesystem fs;
    private final Name root;
    private final ImmutableList<Name> names;

    private MondoFSPath(MondoFilesystem fs, Name root, Iterable<Name> names) {
        this.fs = fs;
        this.root = root;
        this.names = ImmutableList.copyOf(names);
    }

    @Override
    public MondoFilesystem getFileSystem() {
        LOGGER.trace("getFileSystem()");
        return fs;
    }

    @Override
    public boolean isAbsolute() {
        return root != null;
    }

    @Override
    public MondoFSPath getRoot() {
        LOGGER.trace("getRoot()");
        return createPath(fs, root, Collections.EMPTY_LIST);
    }

    @Override
    public MondoFSPath getFileName() {
        return names.isEmpty() ? null : getName(names.size() - 1);
    }

    @Override
    public MondoFSPath getParent() {
        if (names.isEmpty() || names.size() == 1 && root == null) {
            return null;
        }

        return createPath(fs, root, names.subList(0, names.size() - 1));

    }

    @Override
    public int getNameCount() {
        return names.size();
    }

    @Override
    public MondoFSPath getName(int index) {
        checkArgument(index >= 0 && index < names.size(),
                "index (%s) must be >= 0 and < name count (%s)",
                index,
                names.size());

        return createFileName(fs, names.get(index));
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        LOGGER.trace("subpath()");
        return null;
    }

    @Override
    public boolean startsWith(Path other) {
        LOGGER.trace("startsWith()");
        return false;
    }

    @Override
    public boolean startsWith(String other) {
        LOGGER.trace("startsWith()");
        return false;
    }

    @Override
    public boolean endsWith(Path other) {
        LOGGER.trace("endsWith()");
        return false;
    }

    @Override
    public boolean endsWith(String other) {
        LOGGER.trace("endsWith()");
        return false;
    }

    @Override
    public MondoFSPath normalize() {
        if (isNormal()) {
            return this;
        }

        Deque<Name> newNames = new ArrayDeque<>();
        for (Name name : names) {
            if (name.equals(Name.PARENT)) {
                Name lastName = newNames.peekLast();
                if (lastName != null && !lastName.equals(Name.PARENT)) {
                    newNames.removeLast();
                } else if(!isAbsolute()) {
                    // if there's a root and we have and extra ".." that would go up above the root, ignore it
                    newNames.add(name);
                }
            } else if (!name.equals(Name.SELF)) {
                newNames.add(name);
            }
        }

        return newNames.equals(names) ? this : createPath(fs, root, newNames);
    }

    /**
     * Returns whether or not this path is in a normalized form. It's normal if it both contains no
     * "." names and contains no ".." names in a location other than the start of the path.
     */
    private boolean isNormal() {
        if (getNameCount() == 0 || getNameCount() == 1 && !isAbsolute()) {
            return true;
        }

        boolean foundNonParentName = isAbsolute(); // if there's a root, the path doesn't start with ..
        boolean normal = true;
        for (Name name : names) {
            if (name.equals(Name.PARENT)) {
                if (foundNonParentName) {
                    normal = false;
                    break;
                }
            } else {
                if (name.equals(Name.SELF)) {
                    normal = false;
                    break;
                }

                foundNonParentName = true;
            }
        }
        return normal;
    }

    @Override
    public Path resolve(Path other) {
        LOGGER.trace("resolve()");
        return null;
    }

    @Override
    public Path resolve(String other) {
        LOGGER.trace("resolve()");
        return null;
    }

    @Override
    public Path resolveSibling(Path other) {
        LOGGER.trace("resolveSibling()");
        return null;
    }

    @Override
    public Path resolveSibling(String other) {
        LOGGER.trace("resolveSibling()");
        return null;
    }

    @Override
    public Path relativize(Path other) {
        LOGGER.trace("relativize()");
        return null;
    }

    @Override
    public URI toUri() {
        LOGGER.trace("toUri()");
        return null;
    }

    @Override
    public Path toAbsolutePath() {
        LOGGER.trace("toAbsolutePath()");
        return null;
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        LOGGER.trace("toRealPath()");
        return null;
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        LOGGER.trace("register()");
        return null;
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        LOGGER.trace("register()");
        return null;
    }

    @Override
    public Iterator<Path> iterator() {
        return asList().iterator();
    }

    private List<Path> asList() {
        return new AbstractList<Path>() {
            @Override
            public Path get(int index) {
                return getName(index);
            }

            @Override
            public int size() {
                return getNameCount();
            }
        };
    }

    @Override
    public int compareTo(Path path) {
        MondoFSPath other = (MondoFSPath) path;

        return ComparisonChain.start()
                .compare(this.root.display, other.root.display, Ordering.natural())
                .result();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (root != null) {
            builder.append(root);
        }
        PATH_JOINER.appendTo(builder, names);
        return builder.toString();
    }
}
