//______________________________________________________________________________
//
// $Id: Range.java,v 1.1.2.2 2007-06-21 22:47:24 litvinse Exp $
// $Author: litvinse $
//
// Utility class to hold pairs of low,high 
//
// created 03/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

package diskCacheV111.util;

import java.util.* ;


public class Range implements Comparable { 
	private long low;
	private long high;

	public Range( long x , long y ){
		if( x > y ) {
			low = y ; high = x; 
		}
		else { 
			low = x; high = y;
		}
	}
	
	public Range(Range x){
		low=x.getLow();
		high=x.getHigh();
	}
	
	public boolean equals( Object obj ){
		Range x = (Range)obj ;
		if( low==x.getLow() && high==x.getHigh()) {
			return true;
		}
		return false;
	}
	
	public int compareTo( Object x ) throws ClassCastException {
		if (!(x instanceof Range)) { 
			throw new ClassCastException();
		}
		Range r=(Range)x;
		if(low==r.getLow()&&high==r.getHigh()) return 0;
		if(low<r.getLow()||(low==r.getLow()&&high<r.getHigh())) return -1;
		return 1; 
	}
	public String toString(){
		return " ["+low+","+high+"]";
	}
	
	public long getLow()  { 
		return low;
	}
	
	public long getHigh() { 
		return high;
	}
	
	public boolean overlaps(Range x) { 
		return(low <= x.getHigh() && high >= x.getLow()) ;
	}
	public boolean adjacent(Range x) { 
		if (overlaps(x)) { 
			return (low==x.getHigh()||high==x.getLow()) ;
		}
		return false;
	}
        
	public boolean merge(Range x) { 
                if(x == null) {
                    return false;
                }
		if (overlaps(x)) { 
			if (low>x.getLow()) { 
				low=x.getLow();
			}
			if (high<x.getHigh()){
				high=x.getHigh();
			}
			return true;
		}
		return false;
	}
}
