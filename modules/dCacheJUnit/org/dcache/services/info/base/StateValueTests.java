package org.dcache.services.info.base;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class StateValueTests extends InfoBaseTest {

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

	@Test
	public void testEphemeralValue() {
	    StateValue testVal = newEphemeralStateValue();
	    assertIsEphemeral( "ephemeral testValue", testVal);
	}

    @Test
    public void testMortalValue() {
        final long lifetime = 100;

        StateValue testVal = newMortalStateValue( lifetime);
        assertIsMortal( "mortal testValue", testVal, lifetime);
    }

    @Test
    public void testImmortalValue() {
        StateValue testVal = newImmortalStateValue();
        assertIsImmortal( "immortal testValue", testVal);
    }

    @Test
    public void testEarliestChild() {
        StateValue testVal = newEphemeralStateValue();

        assertNull( testVal.getEarliestChildExpiryDate());
    }

    @Test
    public void testApplyTransition() {
        TestStateValue testVal = newTestStateValue();
        StateTransition transition = new StateTransition();
        StatePath ourPath = StatePath.parsePath( "aaa.bbb");

        testVal.applyTransition( null, transition);
        // There isn't much to test.
        assertEquals( "visitor list not zero", testVal.getVisitorInfo().size(), 0);


        testVal.applyTransition( ourPath, transition);

        assertEquals( "visitor list not zero", testVal.getVisitorInfo().size(), 0);
    }


	/**
	 *  P R I V A T E   F U N C T I O N S
	 */
    private TestStateValue newTestStateValue() {
        return new TestStateValue( false);
    }

	private StateValue newEphemeralStateValue() {
		return new TestStateValue( false);
	}

    private StateValue newImmortalStateValue() {
        return new TestStateValue( true);
    }

	private StateValue newMortalStateValue( long lifetime) {
		return new TestStateValue( lifetime);
	}

}
