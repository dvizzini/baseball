package com.danielvizzini.baseball;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

class IntArrayWritable extends ArrayWritable
{
    public IntArrayWritable() {
        super(IntWritable.class);
    }
    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }
}