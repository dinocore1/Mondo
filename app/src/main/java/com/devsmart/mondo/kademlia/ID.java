package com.devsmart.mondo.kademlia;


import com.google.common.io.BaseEncoding;

import static com.google.common.base.Preconditions.checkArgument;

public class ID {
    public static final int NUM_BYTES = 20;

    private byte[] mData = new byte[NUM_BYTES];

    public ID(byte[] buf, int offset) {
        System.arraycopy(buf, offset, mData, 0, NUM_BYTES);
    }

    public String toString(BaseEncoding encoding) {
        return encoding.encode(mData, 0, NUM_BYTES);
    }

    @Override
    public String toString() {
        return toString(BaseEncoding.base16()).substring(0, 6);
    }

    public byte[] distance(ID o) {
        byte[] retval = new byte[NUM_BYTES];
        for(int i=0;i<NUM_BYTES;i++) {
            retval[i] = (byte) (mData[i] ^ o.mData[i]);
        }

        return retval;
    }


}
