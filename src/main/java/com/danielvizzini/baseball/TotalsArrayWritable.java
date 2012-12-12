package com.danielvizzini.baseball;

import java.util.Iterator;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Wrapper of ArrayWritable with an IntWritable array, with the first element being the number of atbats, followed by alternating numerators and denominators.
 */
class TotalsArrayWritable extends ArrayWritable {
	
	private RuntimeException rte = new RuntimeException("Can only construct using odd-lengthed array of IntWritable, with the first element being the number of atbats, followed by alternating numerators and denominators");

	public TotalsArrayWritable() {
        super(IntWritable.class);
    }
	
    public TotalsArrayWritable(IntWritable[] values) throws RuntimeException {
        super(IntWritable.class, values);
    	if (values.length % 2 == 0) throw rte;
    }
    
	/**
	 * @param minimum smallest number of atbats to return true
	 * @return true if totals are the sum of at least minimum at bats, false otherwise
	 */
	public boolean doesNotHaveMin(int minimum) {
		if (this.get() == null) return false;
		return this.get()[0].get() < minimum;
	}
	
	/**
	 * @return the number of atbats that constitue totals
	 */
	public int getNumAtBats() {
		return this.get()[0].get();
	}
	
	/**
	 * @return FloatWritableArray of quotients, in which even-indexed values of this.get()[i] are numerators and values this.get()[i + 1] are denominators for all even values of i
	 */
	public RatioArrayWritable getRatios() {
		FloatWritable[] ratios = new FloatWritable[1 + this.get().length / 2];
		ratios[0] = new FloatWritable(Float.valueOf(this.get()[0].get()));
		for (int i = 1; i < this.get().length; i += 2) {
			ratios[(i + 1) / 2] = new FloatWritable(Float.valueOf(this.get()[i].get()) / Float.valueOf(this.get()[i + 1].get()));			
		}
		
		return new RatioArrayWritable(ratios);
	}
	
	/**
	 * @return true if any denominator field (odd-indexed value of this.get()) is zero, false otherwise
	 */
	public boolean denominatorIsZero() {
		for (int i = 2; i < this.get().length; i += 2) {
			if (this.get()[i].get() == 0) return true;			
		}
		return false;
	}
		
    @Override
    public IntWritable[] get() {
    	
    	if(super.get() == null) return null;
    	
    	IntWritable[] values = new IntWritable[super.get().length];
    	int i = 0;
    	
    	for (Writable value : super.get()) {
    		values[i++] = (IntWritable) value;
    	}
    	
    	return values;
    }
    
    /**
     * Combines another instance of TotalsArrayWritable with this, summing equivalently-indexed elements of arrays
     * @param values instance to be combined with this
     * @throws RuntimeException if arrays are of unequal length
     */
	public void combine(Iterator<TotalsArrayWritable> values) throws RuntimeException {
		
		IntWritable[] totalsArr = this.get();

		//calculate statistics
		while (values.hasNext()) {
			
			TotalsArrayWritable value = values.next();
			if (totalsArr == null) {
				int length = value.get().length;
				totalsArr = new IntWritable[length];
				for (int i = 0; i < length; i++) {
					totalsArr[i] = new IntWritable();
				}
			} else if (totalsArr.length != value.get().length) {
				throw new RuntimeException("Adding arrays of unequal length");
			} else if (value.get().length % 2 == 0) throw rte;

			
			for (int i = 0; i < value.get().length; i++) {
				totalsArr[i].set((totalsArr[i] == null ? 0 : totalsArr[i].get()) + value.get()[i].get());
			}
			
			if (this.get() == null) this.set(totalsArr);
			
		}

	}
	
}