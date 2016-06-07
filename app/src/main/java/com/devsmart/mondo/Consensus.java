package com.devsmart.mondo;


import java.lang.reflect.Array;
import java.net.InetSocketAddress;
import java.util.*;

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
        final int x = Math.abs(from.hashCode() % DIM_X);
        final int y = Math.abs(value.hashCode() % mDimY);

        mAddresses[x][y] = value;
    }


    public HashMap<T, Integer> compute() {
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
        return histogram;
    }

    public List<T> getMostLikely() {
        final HashMap<T, Integer> historgram = compute();
        ArrayList<T> retval = new ArrayList<T>(historgram.keySet());
        Collections.sort(retval, new Comparator<T>() {
            @Override
            public int compare(T a, T b) {
                final int countA = historgram.get(a);
                final int countB = historgram.get(b);

                return countB - countA;
            }
        });


        return retval;
    }


}
