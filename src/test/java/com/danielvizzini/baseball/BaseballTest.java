package com.danielvizzini.baseball;

import org.junit.Test;

import com.danielvizzini.baseball.Baseball;

import junit.framework.TestCase;

public class BaseballTest extends TestCase {
	
	@Test
	public void testCountPitches() {
		assertEquals(Baseball.countPitches("BBBB"), 4);
		assertEquals(Baseball.countPitches("C*BX"), 3);
		assertEquals(Baseball.countPitches("*BBX,3"), 3);
		assertEquals(Baseball.countPitches("CBBBS>S"), 6);
		assertEquals(Baseball.countPitches("TBFBB>B"), 6);
	}

	@Test
	public void testCountRbis() {
		assertEquals(Baseball.countRbis("46(1)3/GDP"), 0);
		assertEquals(Baseball.countRbis("W.3-H;2-3;1-2, 1"), 1);
		assertEquals(Baseball.countRbis("HP"), 0);
		assertEquals(Baseball.countRbis("D9/L.1-H"), 1);
		assertEquals(Baseball.countRbis("HR/89/F"), 1);
		assertEquals(Baseball.countRbis("S9/G.3-H;1-2"), 1);
		assertEquals(Baseball.countRbis("S9/L.3-H;2-H;1-3"), 2);
		assertEquals(Baseball.countRbis("HR/9/F.1-H"), 2);
		assertEquals(Baseball.countRbis("HR/8/F"), 1);
		assertEquals(Baseball.countRbis("T7/F"), 0);
	}

	@Test
	public void testCountBases() {
		assertEquals(Baseball.countBases("46(1)3/GDP"), 0);
		assertEquals(Baseball.countBases("W.3-H;2-3;1-2, 1"), 1);
		assertEquals(Baseball.countBases("HP"), 1);
		assertEquals(Baseball.countBases("D9/L.1-H"), 2);
		assertEquals(Baseball.countBases("HR/89/F"), 4);
		assertEquals(Baseball.countBases("S9/G.3-H;1-2"), 1);
		assertEquals(Baseball.countBases("S9/L.3-H;2-H;1-3"), 1);
		assertEquals(Baseball.countBases("HR/9/F.1-H"), 4);
		assertEquals(Baseball.countBases("HR/8/F"), 4);
		assertEquals(Baseball.countBases("T7/F"), 3);
	}
}