package com.danielvizzini.baseball;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

class SerializableArrayWritable extends ArrayWritable {
    protected SerializableArrayWritable(Class<? extends Writable> klass) {
        super(klass);
    }
    protected SerializableArrayWritable(Class<? extends Writable> klass, Writable[] values) {
        super(klass, values);
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