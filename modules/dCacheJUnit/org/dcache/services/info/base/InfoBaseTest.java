package org.dcache.services.info.base;

import java.util.Set;

/**
 * A collection of methods that are useful to multiple tests.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class InfoBaseTest {

	/**
	 * A test-and-set routine that checks whether a hashCode has already been
	 * seen.  If it has not, an Integer is added to the Set.
	 * @param seenHashCodes the Set for storing the hashes
	 * @param hash the hash value to test-and-set.
	 * @return true if the hash has already been seen, false otherwise.
	 */
	protected boolean hashAlreadySeen( Set<Integer> seenHashCodes, int hash) {
		Integer hashInt = new Integer( hash);
		
		if( seenHashCodes.contains(hash))
			return true;
		
		seenHashCodes.add( hashInt);
		
		return false;
	}

}
