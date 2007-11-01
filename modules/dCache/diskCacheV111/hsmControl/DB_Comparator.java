// $Id: DB_Comparator.java,v 1.1 2002-05-06 09:00:52 cvs Exp $

package diskCacheV111.hsmControl ;

import java.util.* ;

import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;

public class DB_Comparator implements  Comparator {

	public int compare(Object o1, Object o2) {
	
		long result;
	
		if(! (o1 instanceof StagerMessage) || ! (o2 instanceof StagerMessage) ) {
			throw( new ClassCastException() );
		}

		result = ((StagerMessage)o1).getStageTime() - ((StagerMessage)o2).getStageTime();
	
		if( result != 0 ) return (int)result;
	
		if( ((StagerMessage)o1).getPnfsId() != ((StagerMessage)o2).getPnfsId() ) {
			return 1;
		}
		
		return 0;
		
	}
	
	public boolean equals(Object o) {
		return o instanceof DB_Comparator;
	}
	

}
