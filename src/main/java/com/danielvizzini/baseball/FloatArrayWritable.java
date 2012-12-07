package com.danielvizzini.baseball;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;

class FloatArrayWritable extends ArrayWritable
{
    public FloatArrayWritable() {
        super(FloatWritable.class);
    }
    public FloatArrayWritable(FloatWritable[] values) {
        super(FloatWritable.class, values);
    }
}
