package com.danielvizzini.baseball;

import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

class TotalsArrayWritable extends SerializableArrayWritable {
	
	private RuntimeException rte = new RuntimeException("Can only add an even-lengthed array of IntWritable, with alternating numerators and denominators");
	private int numAtBats = 0;

	/**
	 * @return numAtBats that constitute totals
	 */
	public int getNumAtBats() {
		return this.numAtBats;
	}
	
	/**
	 * @return FloatWritableArray of quotients, in which even-indexed values of this.get()[i] are numerators and values this.get()[i + 1] are denominators for all even values of i
	 */
	public FloatArrayWritable getRatios() {
		FloatWritable[] ratios = new FloatWritable[this.get().length / 2];
		for (int i = 0; i < this.get().length; i += 2) {
			ratios[i / 2] = new FloatWritable(Float.valueOf(this.get()[i].get()) / Float.valueOf(this.get()[i + 1].get()));			
		}
		
		return new FloatArrayWritable(ratios);
	}
	
	/**
	 * @return true if any denominator field (odd-indexed value of this.get()) is zero, false otherwise
	 */
	public boolean denominatorIsZero() {
		for (int i = 1; i < this.get().length; i += 2) {
			if (this.get()[i].get() == 0) return true;			
		}
		return true;
	}
		
	public TotalsArrayWritable() {
        super(IntWritable.class);
    }
	
    public TotalsArrayWritable(IntWritable[] values) throws RuntimeException {
        super(IntWritable.class, values);
    	if (values.length % 2 == 1) throw rte;
        this.numAtBats = 1;
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
    
	void combine(Iterator<TotalsArrayWritable> values) throws RuntimeException {
		
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
				throw new RuntimeException("The length of all arrays in the parameter values must be equal to the length of the array in the parameter totals, or totals must contain no values");
			} else if (value.get().length % 2 == 1) throw rte;

			
			for (int i = 0; i < value.get().length; i++) {
				totalsArr[i].set((totalsArr[i] == null ? 0 : totalsArr[i].get()) + value.get()[i].get());
			}
			
			this.numAtBats += value.getNumAtBats();
			this.set(totalsArr);
		}

	}
	
}