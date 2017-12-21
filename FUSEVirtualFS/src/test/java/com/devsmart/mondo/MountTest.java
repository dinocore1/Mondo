package com.devsmart.mondo;

import co.paralleluniverse.javafs.JavaFS;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;


public class MountTest {

    @Test
    public void test1() throws Exception {

        //FileSystem fs = Jimfs.newFileSystem();
        //try (DataOutputStream os = new DataOutputStream(Files.newOutputStream(fs.getPath("/jimfs.txt")))) {
        //    os.writeUTF("JIMFS");
        //}

        DB db = DBMaker.memoryDB().make();
        MondoFileStore fileStore = new MondoFileStore(db);
        MondoFilesystemProvider provider = new MondoFilesystemProvider(fileStore);
        MondoFilesystem fs = new MondoFilesystem(provider, fileStore);


        final Path mnt = Files.createTempDirectory("jfsmnt");
        try {

            JavaFS.mount(fs, mnt, false, false);

            while (true) {
                Thread.sleep(1000);
            }
        } finally {
            Files.delete(mnt);
        }
    }
}
