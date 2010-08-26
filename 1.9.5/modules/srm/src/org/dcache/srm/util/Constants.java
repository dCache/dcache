//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 11/06 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________


package org.dcache.srm.util;

public class Constants { 
	public static final long SECOND = 1000;
	public static final long MINUTE = 60*SECOND;
	public static final long HOUR   = 60*MINUTE;
	public static final long DAY    = 24*HOUR;
	public static final long MONTH  = 30*DAY;
	public static final long YEAR   = 365*DAY;
	public static final long LEAP_YEAR   = 366*DAY;
	public static void main(String args[]) { 
		System.out.println(Constants.SECOND);
		System.out.println(Constants.MINUTE);
		System.out.println(Constants.HOUR);
		System.out.println(Constants.DAY);
		System.out.println(Constants.MONTH);
		System.out.println(Constants.YEAR);
		System.out.println(Constants.LEAP_YEAR);
	}
}

