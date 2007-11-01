// $Id: DB_Comparator.java,v 1.2 2007-06-23 22:05:22 tigran Exp $

package diskCacheV111.hsmControl ;

import java.util.Comparator;

import diskCacheV111.vehicles.StagerMessage;

public class DB_Comparator implements  Comparator<StagerMessage> {

	public int compare(StagerMessage o1, StagerMessage o2) {
	
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
