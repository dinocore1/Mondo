package com.devsmart.mondo.data;


import com.devsmart.mondo.storage.BlockStorage;

import java.util.Iterator;
import java.util.TreeSet;

public class BlockSet<T> implements Iterable<Block<T>>{

    private final TreeSet<Block<T>> mBlockSet = new TreeSet<Block<T>>();

    public void add(Block<T> b) {
        if(!mBlockSet.isEmpty()) {
            Block<T> left = mBlockSet.lower(b);
            if(left != null && left.intersects(b)) {
                mBlockSet.remove(left);
                final Block newleft = new Block<T>(left.offset, (int) (b.offset - left.offset), left.secondaryOffset, left.continer);

                Block<T> right = mBlockSet.higher(left);
                if(right != null && right.intersects(b)) {
                    mBlockSet.remove(right);

                    final long newRightOffset = b.offset + b.len;
                    final long increment = newRightOffset - right.offset;

                    Block<T> newRight = new Block<T>(newRightOffset, (int) (right.end() - newRightOffset), right.secondaryOffset + increment, right.continer);
                    add(newRight);
                }
                add(newleft);
            }
        }

        mBlockSet.add(b);
    }

    public Block<T> getBlockContaining(long offset) {
        final Block<T> key = Block.createKey(offset);
        Block retval = mBlockSet.floor(key);
        if(retval == null) {
            return null;
        } else {
            return retval.containsOffset(offset) ? retval : null;
        }
    }

    public int size() {
        return mBlockSet.size();
    }

    @Override
    public Iterator<Block<T>> iterator() {
        return mBlockSet.iterator();
    }

    public Block<T> getLast() {
        return mBlockSet.last();
    }

    public Block<T> next(Block<T> block) {
        return mBlockSet.higher(block);
    }

    public void clear() {
        mBlockSet.clear();
    }
}
