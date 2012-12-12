package com.danielvizzini.baseball;

import java.text.DecimalFormat;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;

class RatioArrayWritable extends ArrayWritable {
	
    public RatioArrayWritable() {
        super(FloatWritable.class);
    }
    public RatioArrayWritable(FloatWritable[] values) {
        super(FloatWritable.class, values);
    }
        
    @Override
    public String toString() {
        
    	StringBuilder sb = new StringBuilder();

        Float toBeFormatted = ((FloatWritable) this.get()[0]).get();
    	sb.append(new DecimalFormat("000").format(toBeFormatted));
    	
    	DecimalFormat df = new DecimalFormat("0.00000000");
        for (int i = 1; i < this.get().length; i++) {
        	sb.append(",");
        	toBeFormatted = ((FloatWritable) this.get()[i]).get();
        	sb.append(df.format(toBeFormatted));
        }
        
        return sb.toString();

    }    
}
