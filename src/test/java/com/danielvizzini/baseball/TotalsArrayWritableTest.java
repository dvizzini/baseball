package com.danielvizzini.baseball;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class TotalsArrayWritableTest {

	ArrayList<TotalsArrayWritable> arrList = new ArrayList<TotalsArrayWritable>();
	IntWritable zero = new IntWritable(0);
	IntWritable one = new IntWritable(1);
	IntWritable two = new IntWritable(2);
	IntWritable three = new IntWritable(3);
	IntWritable four = new IntWritable(4);
	IntWritable five = new IntWritable(5);
	IntWritable six = new IntWritable(6);
	
	IntWritable[] forward = {zero,one,two,three,four,five};
	IntWritable[] reverse = {five,four,three,two,one,zero};
	IntWritable[] ones = {one,one,one,one,one,one};
	IntWritable[] tooMuch = {zero,one,two,three,four,five,six};	
	
	@Test
	public void testCombine () {
				
		arrList.add(new TotalsArrayWritable(forward));
		arrList.add(new TotalsArrayWritable(reverse));
		arrList.add(new TotalsArrayWritable(ones));
		
		TotalsArrayWritable totals = new TotalsArrayWritable();		
		totals.combine(arrList.iterator());
		
		assertEquals(totals.getNumAtBats(), 3);
		for (int i = 0; i < totals.get().length; i++) {
			assertEquals(totals.get()[i].get(), 6);			
		}
		
		totals.combine(arrList.iterator());
		
		assertEquals(totals.getNumAtBats(), 6);
		for (int i = 0; i < totals.get().length; i++) {
			assertEquals(totals.get()[i].get(), 12);			
		}
	}
	
	@Test(expected = RuntimeException.class)
	public void testCombineException() {
		arrList.add(new TotalsArrayWritable(forward));
		arrList.add(new TotalsArrayWritable(reverse));
		arrList.add(new TotalsArrayWritable(ones));
		arrList.add(new TotalsArrayWritable(tooMuch));
		TotalsArrayWritable totals = new TotalsArrayWritable();
		totals.combine(arrList.iterator());		
	}
	

}
