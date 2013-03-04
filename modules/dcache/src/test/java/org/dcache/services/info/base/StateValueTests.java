package org.dcache.services.info.base;

import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class StateValueTests extends InfoBaseTestHelper {

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


    @Test
    public void testAcceptVisitorNoTransitionNullPath() {
        TestStateValue testVal = newTestStateValue();
        StateVisitor visitor = new VerifyingVisitor();

        testVal.acceptVisitor( null, visitor);

        List<TestStateValue.AcceptVisitorInfo> visitorList = testVal.getVisitorInfo();
        assertEquals( "unexpected number of visitors", 1, visitorList.size());

        TestStateValue.AcceptVisitorInfo avi = visitorList.get( 0);

        assertNull( "StatePath when visiting", avi.getStatePath());
        assertSame( "Unknown StateVisitor", visitor, avi.getVisitor());
    }

    @Test
    public void testAcceptVisitorNoTransitionRealPath() {
        TestStateValue testVal = newTestStateValue();
        StateVisitor visitor = new VerifyingVisitor();
        StatePath path = StatePath.parsePath( "aaa.bbb.cc");

        testVal.acceptVisitor( path, visitor);

        List<TestStateValue.AcceptVisitorInfo> visitorList = testVal.getVisitorInfo();
        assertEquals( "unexpected number of visitors", 1, visitorList.size());

        TestStateValue.AcceptVisitorInfo avi = visitorList.get( 0);

        assertEquals( "StatePath when visiting", path, avi.getStatePath());
        assertSame( "Unknown StateVisitor", visitor, avi.getVisitor());
    }

    @Test
    public void testAcceptVisitorNoTransitionNoSkipNullPath() {
        TestStateValue testVal = newTestStateValue();
        StateVisitor visitor = new VerifyingVisitor();

        testVal.acceptVisitor( null, null, visitor);

        List<TestStateValue.AcceptVisitorInfo> visitorList = testVal.getVisitorInfo();
        assertEquals( "unexpected number of visitors", 1, visitorList.size());

        TestStateValue.AcceptVisitorInfo avi = visitorList.get( 0);

        assertNull( "StatePath when visiting", avi.getStatePath());
        assertSame( "Unknown StateVisitor", visitor, avi.getVisitor());
    }

    @Test
    public void testAcceptVisitorNoTransitionNoSkipRealPath() {
        TestStateValue testVal = newTestStateValue();
        StateVisitor visitor = new VerifyingVisitor();
        StatePath path = StatePath.parsePath( "aa.bb");

        testVal.acceptVisitor( path, visitor);

        List<TestStateValue.AcceptVisitorInfo> visitorList = testVal.getVisitorInfo();
        assertEquals( "unexpected number of visitors", 1, visitorList.size());

        TestStateValue.AcceptVisitorInfo avi = visitorList.get( 0);

        assertEquals( "StatePath when visiting", path, avi.getStatePath());
        assertSame( "Unknown StateVisitor", visitor, avi.getVisitor());
    }

    /**
     * This test may look a bit odd:  the transition has no effect on the visitor.
     * What happens is that any effect the transition may have had is dealt with by
     * the StateComposite immediately before the StateValue.  If a StateValue is
     * called with a StateTransition it is already known that no changes are to take
     * place to this StateValue from the StateTransition.
     */
    @Test
    public void testAcceptVisitorTransition() {
        TestStateValue testVal = newTestStateValue();
        StateVisitor visitor = new VerifyingVisitor();
        StatePath path = StatePath.parsePath( "aa.bb");
        StateTransition transition = new StateTransition();

        testVal.acceptVisitor( transition, path, visitor);

        List<TestStateValue.AcceptVisitorInfo> visitorList = testVal.getVisitorInfo();
        assertEquals( "unexpected number of visitors", 1, visitorList.size());

        TestStateValue.AcceptVisitorInfo avi = visitorList.get( 0);

        assertEquals( "StatePath when visiting", path, avi.getStatePath());
        assertSame( "Unknown StateVisitor", visitor, avi.getVisitor());
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
