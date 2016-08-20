package com.devsmart.mondo.storage;


import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;

public class DataFile {

    FilePart[] mParts;

    public static final GroupSerializerObjectArray<DataFile> SERIALIZER = new GroupSerializerObjectArray<DataFile>() {
        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull DataFile value) throws IOException {
            out.packInt(value.mParts.length);
            for(FilePart part : value.mParts) {
                FilePart.SERIALIZER.serialize(out, part);
            }

        }

        @Override
        public DataFile deserialize(@NotNull DataInput2 input, int available) throws IOException {
            DataFile retval = new DataFile();
            retval.mParts = new FilePart[input.unpackInt()];
            for(int i=0;i<retval.mParts.length;i++) {
                retval.mParts[i] = FilePart.SERIALIZER.deserialize(input, available);
            }

            return retval;
        }

        @Override
        public int compare(DataFile a, DataFile b) {
            return 0;
        }
    };

}
