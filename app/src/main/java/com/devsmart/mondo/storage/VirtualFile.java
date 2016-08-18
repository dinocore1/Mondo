package com.devsmart.mondo.storage;


import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;

public class VirtualFile {

    private static final int FLAG_DIR = 0x400;
    private static final int FLAG_EXECUTE = 0x1;
    private static final int FLAG_WRITE = 0x2;
    private static final int FLAG_READ = 0x4;

    private String mName;
    private int mFlags;
    private FilePart[] mParts;

    private VirtualFile() {
    }

    public static VirtualFile createDir(String name) {
        VirtualFile retval = new VirtualFile();
        retval.mName = name;
        retval.mFlags = FLAG_DIR | FLAG_READ | FLAG_EXECUTE;
        return retval;
    }

    public static VirtualFile createFile(String name, FilePart[] parts) {
        VirtualFile retval = new VirtualFile();
        retval.mName = name;
        retval.mFlags = FLAG_READ | FLAG_WRITE;
        retval.mParts = parts;
        return retval;
    }

    public boolean isDirectory() {
        return (mFlags & FLAG_DIR) > 0;
    }

    public boolean isFile() {
        return (mFlags & FLAG_DIR) == 0;
    }

    public String getName() {
        return mName;
    }

    public long length() {
        long retval = 0;
        for(FilePart part : mParts) {
            retval += part.getSize();
        }

        return retval;
    }

    public static final GroupSerializerObjectArray<VirtualFile> SERIALIZER = new GroupSerializerObjectArray<VirtualFile>() {

        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull VirtualFile value) throws IOException {
            out.writeUTF(value.mName);
            out.writeInt(value.mFlags);
            out.packInt(value.mParts.length);
            for(FilePart part : value.mParts) {
                FilePart.SERIALIZER.serialize(out, part);
            }
        }

        @Override
        public VirtualFile deserialize(@NotNull DataInput2 input, int available) throws IOException {
            VirtualFile retval = new VirtualFile();
            retval.mName = input.readUTF();
            retval.mFlags = input.readInt();
            retval.mParts = new FilePart[input.unpackInt()];
            for(int i=0;i<retval.mParts.length;i++) {
                retval.mParts[i] = FilePart.SERIALIZER.deserialize(input, available);
            }

            return retval;
        }

        @Override
        public int compare(VirtualFile first, VirtualFile second) {
            return super.compare(first, second);
        }
    };


}
