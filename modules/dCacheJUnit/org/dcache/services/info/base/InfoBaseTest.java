package org.dcache.services.info.base;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A collection of methods that are useful to multiple tests.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class InfoBaseTest {

	private static final long TIME_TOLLERANCE = 500;
	
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
	
	
	/**
	 * Assert that a StateComponent is not null and ephemeral.
	 * @param msg a String message to display if the StateComponent is not ephemeral. 
	 * @param component the StateComponent to test.
	 */
	protected void assertIsEphemeral( String msg, StateComponent component) {
		assertNotNull( msg, component);
		
		assertFalse( msg, component.isImmortal());
		assertTrue( msg, component.isEphemeral());
		assertFalse( msg, component.isMortal());
	}
	

	/**
	 * Assert that a StateComponent is not null and immortal.
	 * @param msg a String message to display if the StateComponent is not immortal. 
	 * @param component the StateComponent to test.
	 */
	protected void assertIsImmortal( String msg, StateComponent component) {
		assertNotNull( msg, component);
		
		assertTrue( msg, component.isImmortal());
		assertFalse( msg, component.isEphemeral());
		assertFalse( msg, component.isMortal());
	}
	
	

	/**
	 * Assert that a StateComponent is not null is mortal and will expire at the expected number
	 * of seconds in the future.
	 * @param msg a String message to display if the StateComponent is not mortal.
	 * @param component the StateComponent to test.
	 * @param lifetime the expected lifetime of this StateComponent, in seconds.
	 */
	protected void assertIsMortal( String msg, StateComponent component, long lifetime) {
		assertNotNull( msg, component);
		
		assertFalse( msg, component.isImmortal());
		assertFalse( msg, component.isEphemeral());
		assertTrue( msg, component.isMortal());
				
		/**
		 *  Check that the expiry date matches (within tolerance)
		 */
		Date actualExpiry = component.getExpiryDate();
		Date expectedExpiry = new Date( System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(lifetime));
		
		assertTrue( msg, Math.abs(actualExpiry.getTime() - expectedExpiry.getTime()) < TIME_TOLLERANCE);
	}
}
