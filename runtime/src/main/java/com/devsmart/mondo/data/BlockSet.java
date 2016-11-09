package com.devsmart.mondo.data;


import java.util.TreeSet;

public class BlockSet {

    final TreeSet<Block> mBlockSet = new TreeSet<Block>();

    public void add(Block b) {
        if(!mBlockSet.isEmpty()) {
            Block left = mBlockSet.lower(b);
            if(left != null && left.intersects(b)) {
                mBlockSet.remove(left);
                final Block newleft = new Block(left.offset, (int) (b.offset - left.offset), left.secondaryOffset);

                Block right = mBlockSet.higher(left);
                if(right != null && right.intersects(b)) {
                    mBlockSet.remove(right);

                    final long newRightOffset = b.offset + b.len;
                    final long increment = newRightOffset - right.offset;

                    Block newRight = new Block(newRightOffset, (int) (right.end() - newRightOffset), right.secondaryOffset + increment);
                    add(newRight);
                }
                add(newleft);
            }
        }

        mBlockSet.add(b);
    }

    public int size() {
        return mBlockSet.size();
    }
}
