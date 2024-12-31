package com.lartimes.unicom.mapreduce.groups;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 23:25
 */
public class TextPartitionerComparator extends WritableComparator {
    protected TextPartitionerComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String key1 = a.toString();
        String key2 = b.toString();
        return key1.compareTo(key2);
    }
}

