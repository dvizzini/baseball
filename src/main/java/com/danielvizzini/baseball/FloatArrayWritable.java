package com.danielvizzini.baseball;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;

public class FloatArrayWritable extends ArrayWritable
{
    public FloatArrayWritable() {
        super(FloatWritable.class);
    }
    public FloatArrayWritable(FloatWritable[] values) {
        super(FloatWritable.class, values);
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
