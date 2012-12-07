package com.danielvizzini.baseball;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
        super(IntWritable.class);
    }
    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }

    @Override
    public String toString() {
    	String prefix = "";
        StringBuilder sb = new StringBuilder();
        for (String s : super.toStrings())
        {
        	sb.append(prefix);
        	sb.append(s);
            prefix = ",";
        }
        
        return sb.toString();
    }
}