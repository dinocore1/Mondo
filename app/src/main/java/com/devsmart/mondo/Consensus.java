package com.devsmart.mondo;


import java.lang.reflect.Array;
import java.net.InetSocketAddress;
import java.util.HashMap;

/**
 * This datastructor protects against a small number of misbehaving peers
 * tring to attack the system by giving a bunch of nonsence values. It relies on the
 * assumption that most peers are not malicious and will report similar correct values.
 * Under the covers, this datastructor can be though of an efficient histogram using
 * votes from unique peers.
 * @param <T>
 */
public class Consensus<T> {

    /**
     * http://preshing.com/20110504/hash-collision-probabilities/
     * @param uniquenessConfidence
     * @param numExpectedUniqueValues
     * @return
     */
    public static int getBinSize(float uniquenessConfidence, int numExpectedUniqueValues) {
        return -1;
    }

    private final int DIM_X = 32;
    private final int mDimY;
    private final T[][] mAddresses;

    @SuppressWarnings("unchecked")
    public Consensus(Class<? extends T> classType, int expectedUniqueValues) {
        mDimY = expectedUniqueValues;
        mAddresses = (T[][]) Array.newInstance(classType, DIM_X, mDimY);

    }

    public synchronized void vote(T value, InetSocketAddress from) {
        final int x = from.hashCode() % DIM_X;
        final int y = value.hashCode() % mDimY;

        mAddresses[x][y] = value;
    }


    public void compute() {
        HashMap<T, Integer> histogram = new HashMap<T, Integer>();
        for(int x=0;x<DIM_X;x++){
            for(int y=0;y<mDimY;y++) {
                T value = mAddresses[x][y];
                if(value != null) {
                    Integer count = histogram.get(value);
                    if (count == null) {
                        histogram.put(value, 1);
                    } else {
                        histogram.put(value, count + 1);
                    }
                }
            }
        }
    }


}
