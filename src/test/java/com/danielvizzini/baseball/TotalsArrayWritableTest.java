package com.danielvizzini.baseball;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class TotalsArrayWritableTest {

	ArrayList<TotalsArrayWritable> arrList = new ArrayList<TotalsArrayWritable>();
	IntWritable counter = new IntWritable(1);
	IntWritable zero = new IntWritable(0);
	IntWritable one = new IntWritable(1);
	IntWritable two = new IntWritable(2);
	IntWritable three = new IntWritable(3);
	IntWritable four = new IntWritable(4);
	IntWritable five = new IntWritable(5);
	IntWritable six = new IntWritable(6);
	IntWritable seven= new IntWritable(7);
	
	IntWritable[] forward = {counter,zero,one,two,three,four,five};
	IntWritable[] reverse = {counter,five,four,three,two,one,zero};
	IntWritable[] ones = {counter,one,one,one,one,one,one};
	IntWritable[] tooMuch = {counter,zero,one,two,three,four,five,six,seven};	
	
	@Test
	public void testConstructor() {
		
		TotalsArrayWritable totals = new TotalsArrayWritable(forward);		
		assertEquals(totals.getNumAtBats(), 1);		
		assertEquals(totals.doesNotHaveMin(0), false);		
		assertEquals(totals.doesNotHaveMin(1), false);		
		assertEquals(totals.doesNotHaveMin(2), true);
		
		arrList.add(new TotalsArrayWritable(reverse));
		totals.combine(arrList.iterator());
		assertEquals(totals.getNumAtBats(), 2);		
		assertEquals(totals.doesNotHaveMin(1), false);		
		assertEquals(totals.doesNotHaveMin(2), false);		
		assertEquals(totals.doesNotHaveMin(3), true);
		
	}
	
	@Test
	public void testDenominatorIsZero() {
		assertEquals(new TotalsArrayWritable(forward).denominatorIsZero(), false);
		assertEquals(new TotalsArrayWritable(reverse).denominatorIsZero(), true);
	}
	
	@Test
	public void testCombine () {
				
		arrList.add(new TotalsArrayWritable(forward));
		arrList.add(new TotalsArrayWritable(reverse));
		arrList.add(new TotalsArrayWritable(ones));
		
		TotalsArrayWritable totals = new TotalsArrayWritable();		
		totals.combine(arrList.iterator());
		
		assertEquals(totals.doesNotHaveMin(3), false);
		assertEquals(totals.doesNotHaveMin(4), true);
		for (int i = 1; i < totals.get().length; i++) {
			assertEquals(totals.get()[i].get(), 6);			
		}
		
		totals.combine(arrList.iterator());
		
		assertEquals(totals.doesNotHaveMin(6), false);
		assertEquals(totals.doesNotHaveMin(7), true);
		for (int i = 1; i < totals.get().length; i++) {
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