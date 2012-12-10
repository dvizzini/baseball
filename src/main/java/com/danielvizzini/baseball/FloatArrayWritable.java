package com.danielvizzini.baseball;

import org.apache.hadoop.io.FloatWritable;

class FloatArrayWritable extends SerializableArrayWritable {
	
    public FloatArrayWritable() {
        super(FloatWritable.class);
    }
    public FloatArrayWritable(FloatWritable[] values) {
        super(FloatWritable.class, values);
    }
        
}
