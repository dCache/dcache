//______________________________________________________________________________
//
// $Id: Ranges.java,v 1.5 2007-06-21 22:31:08 litvinse Exp $
// $Author: litvinse $
//
// Utility class to hold ranges of variables
//
// created 03/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

package diskCacheV111.util;

import java.util.* ;

public class Ranges {
    private TreeSet list=null;

    public Ranges() {
	list = new TreeSet();
    }

    public void clear() {
	list.clear();
    }

    public boolean isContiguous() {
	return (list.size()==1);
    }

    public void addRange(Range newRange) {
	if (list.size()==0) {
	    list.add(newRange);
	}
	else {
	    Iterator i=list.iterator();
	    boolean  isMerged=false;
	    while(i.hasNext()) {
		Range r = (Range)i.next();
		if (r.merge(newRange)) {
		    if(i.hasNext()) {
			Range nextRange = (Range)i.next();
			if(r.merge(nextRange)) {
			    i.remove();
			}
		    }
		    isMerged=true;
		    break;
		}
	    }
	    if (isMerged==false) {
		list.add(newRange);
	    }
	}
    }

    public TreeSet getRanges() {
	return list;
    }

    public String toString(){
	Iterator i=list.iterator();
	StringBuffer sb = new StringBuffer();
	while(i.hasNext()) {
		sb.append((Range)i.next());
	}
	sb.append("\n");
	return sb.toString();
    }


    public static void main(String argv[]) {
	Ranges ranges = new Ranges();
	ranges.addRange(new Range(20,30));
	ranges.addRange(new Range(0,10));
	ranges.addRange(new Range(9,22));
	System.out.println("range "+ranges+" is contiguous "+ranges.isContiguous());
	ranges.addRange(new Range(12,10));
	ranges.addRange(new Range(35,55));
	ranges.addRange(new Range(55,75));
	System.out.println("range "+ranges+" is contiguous "+ranges.isContiguous());
    }
}
