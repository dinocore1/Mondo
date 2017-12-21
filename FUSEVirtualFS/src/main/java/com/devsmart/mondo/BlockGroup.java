package com.devsmart.mondo;


import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializer;
import org.mapdb.serializer.GroupSerializerObjectArray;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

public class BlockGroup {

    public static final HashFunction HASH_FUNCTION = Hashing.sha1();

    public final int offset;
    public final ImmutableList<HashCode> blocks;

    public BlockGroup(int offset, ImmutableList<HashCode> blocks) {
        this.offset = offset;
        this.blocks = blocks;
    }

    public static final GroupSerializer<BlockGroup> SERIALIZER = new GroupSerializerObjectArray<BlockGroup>() {
        @Override
        public void serialize(@NotNull DataOutput2 out, @NotNull BlockGroup value) throws IOException {
            out.packInt(value.offset);
            int numBlocks = value.blocks.size();
            out.packInt(numBlocks);
            for(HashCode hashCode : value.blocks) {
                byte[] data = hashCode.asBytes();
                checkState(data.length == 20);
                out.write(data);
            }

        }

        @Override
        public BlockGroup deserialize(@NotNull DataInput2 input, int available) throws IOException {
            byte[] data = new byte[20];
            final int offset = input.readInt();

            int numBlocks = input.unpackInt();
            ImmutableList.Builder<HashCode> builder = ImmutableList.builder();

            for(int i=0;i<numBlocks;i++) {
                input.readFully(data);
                builder.add(HashCode.fromBytes(data));
            }

            return new BlockGroup(offset, builder.build());
        }

        @Override
        public int compare(BlockGroup a, BlockGroup b) {
            return 0;
        }
    };
}
