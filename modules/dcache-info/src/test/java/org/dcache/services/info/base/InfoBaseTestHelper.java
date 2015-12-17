package org.dcache.services.info.base;


import org.hamcrest.Matcher;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 * A collection of methods that are useful to multiple tests.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class InfoBaseTestHelper
{
    private static final long ROUNDING_PERIOD_IN_MS = 500;
    private static final long TIMING_TOLERANCE = 400;

	/**
	 * A test-and-set routine that checks whether a hashCode has already been
	 * seen.  If it has not, an Integer is added to the Set.
	 * @param seenHashCodes the Set for storing the hashes
	 * @param hash the hash value to test-and-set.
	 * @return true if the hash has already been seen, false otherwise.
	 */
	protected boolean hashAlreadySeen( Set<Integer> seenHashCodes, int hash) {
		Integer hashInt = hash;

		if( seenHashCodes.contains(hash)) {
                    return true;
                }

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

		assertNull(msg, component.getExpiryDate());

		assertFalse( msg, component.hasExpired());
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

		assertNull(msg, component.getExpiryDate());

        assertFalse( msg, component.hasExpired());
	}


        /**
         * Assert that a StateComponent is not null is mortal and will expire at the expected number
         * of seconds in the future.
         * @param msg a String message to display if the StateComponent is not mortal.
         * @param component the StateComponent to test.
         * @param lifetime the expected lifetime of this StateComponent, in seconds.
         */
        protected void assertIsMortal(String msg, StateComponent component, long lifetime)
        {
            assertNotNull(msg, component);

            assertFalse(msg + " [isImmortal]", component.isImmortal());
            assertFalse(msg + " [isEphemeral]", component.isEphemeral());
            assertTrue(msg + " [isMortal]", component.isMortal());

            long duration = SECONDS.toMillis((lifetime < 0) ? 0 : lifetime);
            long expectedExpiry = System.currentTimeMillis() + duration;

            // Ideally expected and actual times match exactly; but,
            // because System.currentTimeMillis() is called at different
            // times (here and in the StateComponent constructor) the value
            // can be different.

            // We provide different matchers to make as strong a statement
            // as possible while allowing for this discrepency.
            Matcher<Long> matcher;
            if (component instanceof StateValue && duration > 0) {
                // As there is rounding, the difference should be exactly 0
                // or exactly ROUNDING_PERIOD_IN_MS, provided not more than
                // ROUNDING_PERIOD_IN_MS has elapsed.
                expectedExpiry = ROUNDING_PERIOD_IN_MS * (1 + (expectedExpiry - 1) / ROUNDING_PERIOD_IN_MS);
                matcher = either(equalTo(0L)).or(equalTo(ROUNDING_PERIOD_IN_MS));
            } else {
                // No rounding, simply check it is close enough.
                matcher = both(greaterThanOrEqualTo(0L)).and(lessThan(TIMING_TOLERANCE));
            }

            long delta = expectedExpiry - component.getExpiryDate().getTime();
            assertThat(msg + " [expected-getExpiryDate]", delta, matcher);
	}


	/**
	 * Assert that the laterDate is between earlierDate + timeInFuture-tolerance and
	 * earlierDate + timeInFuture+tolerance.
	 * @param msg
	 * @param earlierDate
	 * @param laterDate
	 * @param timeInFuture
	 * @param tolerance
	 */
	protected void assertDatedDuration( String msg, Date earlierDate, Date laterDate, long timeInFuture, long tolerance) {
		long s = TimeUnit.SECONDS.convert( laterDate.getTime() - earlierDate.getTime(), TimeUnit.MILLISECONDS);

		assertTrue( msg + " (time mismatch; expected: " + Long.toString(timeInFuture) + ", got: " + Long.toString( s)+")", Math.abs( timeInFuture - s) <= tolerance);
	}

    public static StateTransition buildTransition( StatePath path, StateValue metric) throws BadStatePathException {
    	StateTransition transition = new StateTransition();

    	StateUpdate update = new StateUpdate();
    	update.appendUpdate( path, metric);

    	update.updateTransition( new StateComposite(), transition);

    	return transition;
    }
}
