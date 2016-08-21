package com.devsmart.mondo.storage;


import com.google.common.collect.Iterables;
import org.junit.Test;
import static org.junit.Assert.*;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class VirtualFilesystemTests {

    @Test
    public void testPath() {
        DB db = DBMaker.memoryDB().make();

        VirtualFilesystem.Path p = new VirtualFilesystem.Path('/');
        p.setFilepath("/");
        assertEquals("/", p.getParent());
        assertEquals("", p.getName());

        p.setFilepath("/test");
        assertEquals("/", p.getParent());
        assertEquals("test", p.getName());

        p.setFilepath("/test/word");
        assertEquals("/test", p.getParent());
        assertEquals("word", p.getName());

        p.setFilepath("/test/word/cool");
        assertEquals("/test/word", p.getParent());
        assertEquals("cool", p.getName());
    }

    @Test
    public void testIterateFiles() throws Exception {
        DB db = DBMaker.memoryDB().make();

        VirtualFilesystem vfs = new VirtualFilesystem(db, '/');

        assertEquals(0, Iterables.size(vfs.getFilesInDir("/")));

        vfs.mkdir("/test");
        assertEquals(1, Iterables.size(vfs.getFilesInDir("/")));
        assertTrue(vfs.pathExists("/"));
        assertFalse(vfs.pathExists("/word"));
        assertTrue(vfs.pathExists("/test"));

        VirtualFile testDir = vfs.getFile("/test");
        assertNotNull(testDir);

        vfs.mkdir("/test/word");
        assertEquals(1, Iterables.size(vfs.getFilesInDir("/")));
        assertEquals(1, Iterables.size(vfs.getFilesInDir("/test")));

        vfs.mkdir("/ok");
        assertEquals(2, Iterables.size(vfs.getFilesInDir("/")));
        assertEquals(1, Iterables.size(vfs.getFilesInDir("/test")));

    }
}
