package com.danielvizzini.baseball;

import org.junit.Test;

import junit.framework.TestCase;

public class BaseballUtilTest extends TestCase {
	
	@Test
	public void testCountPitches() {
		assertEquals(BaseballUtil.countPitches("BBBB"), 4);
		assertEquals(BaseballUtil.countPitches("C*BX"), 3);
		assertEquals(BaseballUtil.countPitches("*BBX,3"), 3);
		assertEquals(BaseballUtil.countPitches("CBBBS>S"), 6);
		assertEquals(BaseballUtil.countPitches("TBFBB>B"), 6);
	}

	@Test
	public void testCountRbis() {
		assertEquals(BaseballUtil.countRbis("46(1)3/GDP"), 0);
		assertEquals(BaseballUtil.countRbis("W.3-H;2-3;1-2, 1"), 1);
		assertEquals(BaseballUtil.countRbis("HP"), 0);
		assertEquals(BaseballUtil.countRbis("D9/L.1-H"), 1);
		assertEquals(BaseballUtil.countRbis("HR/89/F"), 1);
		assertEquals(BaseballUtil.countRbis("S9/G.3-H;1-2"), 1);
		assertEquals(BaseballUtil.countRbis("S9/L.3-H;2-H;1-3"), 2);
		assertEquals(BaseballUtil.countRbis("HR/9/F.1-H"), 2);
		assertEquals(BaseballUtil.countRbis("HR/8/F"), 1);
		assertEquals(BaseballUtil.countRbis("T7/F"), 0);
	}

	@Test
	public void testCountBases() {
		assertEquals(BaseballUtil.countBases("46(1)3/GDP"), 0);
		assertEquals(BaseballUtil.countBases("W.3-H;2-3;1-2, 1"), 1);
		assertEquals(BaseballUtil.countBases("HP"), 1);
		assertEquals(BaseballUtil.countBases("D9/L.1-H"), 2);
		assertEquals(BaseballUtil.countBases("HR/89/F"), 4);
		assertEquals(BaseballUtil.countBases("S9/G.3-H;1-2"), 1);
		assertEquals(BaseballUtil.countBases("S9/L.3-H;2-H;1-3"), 1);
		assertEquals(BaseballUtil.countBases("HR/9/F.1-H"), 4);
		assertEquals(BaseballUtil.countBases("HR/8/F"), 4);
		assertEquals(BaseballUtil.countBases("T7/F"), 3);
	}
}