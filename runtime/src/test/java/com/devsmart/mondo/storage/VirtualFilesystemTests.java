package com.devsmart.mondo.storage;


import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class VirtualFilesystemTests {

    @Test
    public void testIterateFiles() throws Exception {
        DB db = DBMaker.memoryDB().make();

        VirtualFilesystem vfs = new VirtualFilesystem(db);

    }
}
