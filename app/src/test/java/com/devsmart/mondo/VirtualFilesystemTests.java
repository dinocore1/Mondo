package com.devsmart.mondo;


import com.devsmart.mondo.storage.FilePart;
import com.devsmart.mondo.storage.VirtualFile;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;

import java.util.NavigableSet;
import java.util.Random;

public class VirtualFilesystemTests {

    private static final Random r = new Random(1);

    private static FilePart[] createRandomFileparts() {
        FilePart[] retval = new FilePart[5];
        for(int i=0;i<retval.length;i++) {
            int size = r.nextInt(3000);
            byte[] checksum = new byte[20];
            r.nextBytes(checksum);
            retval[i] = new FilePart(size, checksum);
        }
        return retval;
    }

    @Test
    public void testVirtualFilesystem() {

        DB db = DBMaker
                .memoryDB()
                .make();

        NavigableSet<Object[]> files = db.treeSet("files")
                .serializer(new SerializerArrayTuple(Serializer.STRING_ASCII, VirtualFile.SERIALIZER))
                .createOrOpen();


        files.add(new Object[]{"", VirtualFile.createFile("helloworld.txt", createRandomFileparts())});
        files.add(new Object[]{"", VirtualFile.createFile("awesome.txt", createRandomFileparts())});

    }
}
