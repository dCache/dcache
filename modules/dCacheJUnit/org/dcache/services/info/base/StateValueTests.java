package org.dcache.services.info.base;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class StateValueTests {

	/** The tolerance of time comparisons, in milliseconds. */
	private static final long TIME_TOLERANCE = 500;
	
	/** Different test times, in seconds */
	private static final long EXPIRY_TIMES[] = {-1, 0, 1, 2};
	
	@Test
	public void testGetExpiryDate() {
		
		for( long expTime : EXPIRY_TIMES) {
		
			StateValue testVal = newMortalStateValue( expTime);
			Date expDate = testVal.getExpiryDate();
			
			long expectedTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( expTime < 0 ? 0 : expTime);
			
			assertTrue( "time mismatch with getExpiryDate(): " + expDate + " != " + new Date( expectedTime), Math.abs(expDate.getTime() - expectedTime) < TIME_TOLERANCE);
		}
	}

	@Test
	public void testHasExpired() {
		
		for( long expTime : EXPIRY_TIMES) {
			
			StateValue testVal = newMortalStateValue( expTime);
			
			boolean expectHasExpired = expTime < 1;
			assertTrue( "hasExpired() for "+ expTime +" returned "+ testVal.hasExpired() +", which is the wrong result", testVal.hasExpired() == expectHasExpired); 
		}
	}

	@Test
	public void testBuildTransition() {
		StateValue testVal = newEphemeralStateValue();
		
		boolean caughtException = false;
		
		try {
			testVal.buildTransition( new StatePath("path"), new StatePath("childPath"), newEphemeralStateValue(), new StateTransition());
		} catch( MetricStatePathException e) {
			caughtException = true;
		}
		
		assertTrue( "failed to catch MetricStatePathException", caughtException);
	}
	

	@Test
	public void testBuildRemovalTransition() {
		StateValue testVal = newEphemeralStateValue();

		// Should be a no-op.
		testVal.buildRemovalTransition( new StatePath( "path"), new StateTransition(), false);
	}
	
	
	/**
	 *  P R I V A T E   F U N C T I O N S
	 */
	
	private StateValue newEphemeralStateValue() {
		return new IntegerStateValue(0);
	}
	
	private StateValue newMortalStateValue( long lifetime) {
		return new IntegerStateValue( 0, lifetime);
	}

}
