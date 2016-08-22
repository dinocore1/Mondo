package com.devsmart.mondo.storage;


import com.google.common.hash.HashCode;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;
import java.util.Arrays;

public class FilePart {


    private final int mSize;
    private final byte[] mSha1Checksum = new byte[20];

    public FilePart(int size, byte[] sha1) {
        mSize = size;
        System.arraycopy(sha1, 0, mSha1Checksum, 0, mSha1Checksum.length);
    }

    public long getSize() {
        return mSize;
    }

    public HashCode getSha1Checksum() {
        return HashCode.fromBytes(mSha1Checksum);
    }

    @Override
    public int hashCode() {
        return mSize ^ Arrays.hashCode(mSha1Checksum);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        FilePart other = (FilePart) obj;
        return mSize == other.mSize && Arrays.equals(mSha1Checksum, other.mSha1Checksum);
    }

    public static final GroupSerializerObjectArray<FilePart> SERIALIZER = new GroupSerializerObjectArray<FilePart>() {

        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull FilePart value) throws IOException {
            out.writeInt(value.mSize);
            Serializer.BYTE_ARRAY.serialize(out, value.mSha1Checksum);
        }

        @Override
        public FilePart deserialize(@NotNull DataInput2 input, int available) throws IOException {
            final int size = input.readInt();
            final byte[] sha1Checksum = Serializer.BYTE_ARRAY.deserialize(input, available);
            FilePart retval = new FilePart(size, sha1Checksum);
            return retval;
        }

        @Override
        public int compare(FilePart a, FilePart b) {
            return super.compare(a, b);
        }
    };


}
