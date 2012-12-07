package com.danielvizzini.baseball;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BaseballUtil {

	/**
	 * @param pitch field of play record
	 * @return all actions that constitute a pitch, as defined by http://www.retrosheet.org/eventfile.htm
	 */
	static int countPitches(String str) {
		int pitches = 0;
		Matcher matcher = Pattern.compile("[BCFHKLMOPQRSTUXY]").matcher(str);
		while (matcher.find()) pitches++;
		return pitches;
	}
	
	/**
	 * @param event field of play record
	 * @return all runs scored during event
	 */
	static int countRbis(String str) {
		int rbis = 0;
		Matcher matcher = Pattern.compile("-H|HR").matcher(str);
		while (matcher.find()) rbis++; 
		return rbis;
	}
	
	/**
	 * @param event field of play record
	 * @return all bases gained by battter during atbat
	 */
	static int countBases(String str) {
		//hit by pitch
		if (str.startsWith("HP")) return 1;
		
		switch (str.charAt(0)) {
		//single, intentional walk, and walk
		case 'S':
		case 'I':
		case 'W':
			return 1;
		case 'D':
			return 2;
		case 'T':
			return 3;
		case 'H':
			return 4;
		default:
			return 0;
		}
	}	
}
