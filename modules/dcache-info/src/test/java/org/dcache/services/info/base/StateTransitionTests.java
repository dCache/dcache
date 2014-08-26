package org.dcache.services.info.base;


import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class StateTransitionTests {

    static final StatePath PATH_WITH_SCS = StatePath.parsePath( "aaa.bbb");
    static final StatePath OTHER_PATH = StatePath.parsePath( "aaa.bbb.ccc");

    StateTransition _transition;
    StateChangeSet  _scs;

    @Before
    public void setUp()
    {
        _transition = new StateTransition();
        _scs = _transition.getOrCreateChangeSet( PATH_WITH_SCS);
    }


    @Test
    public void testUnknownNullGetStateChangeSet() {
        assertNull( "null StatPath", _transition.getStateChangeSet( null));
    }

    @Test
    public void testUnknownNotNullGetStateChangeSet() {
        assertNull( "OtherPath StatPath", _transition.getStateChangeSet( OTHER_PATH));
    }

    @Test
    public void testKnownGetStateChangeSet() {
        assertSame( "Couldn't find SCS", _scs, _transition.getStateChangeSet( PATH_WITH_SCS));
    }


    @Test
    public void testNullGetOrCreateChangeSet() {
        StateChangeSet nullCsc = _transition.getOrCreateChangeSet( null);
        assertSame( "null StatPath", nullCsc, _transition.getStateChangeSet( null));
    }

    @Test
    public void testOtherPathGetOrCreateChangeSet() {
        StateChangeSet otherPathCsc = _transition.getOrCreateChangeSet( OTHER_PATH);
        assertSame( "OtherPath StatPath", otherPathCsc, _transition.getStateChangeSet( OTHER_PATH));
    }

    @Test
    public void testKnownGetOrCreateChangeSet() {
        assertSame( "Scs", _scs, _transition.getOrCreateChangeSet( PATH_WITH_SCS));
        assertSame( "Scs", _scs, _transition.getStateChangeSet( PATH_WITH_SCS));
    }

    @Test
    public void testDumpContenst() {
        _transition.getOrCreateChangeSet( null);
        _transition.getOrCreateChangeSet( OTHER_PATH);

        String dump = _transition.dumpContents();

        assertTrue( "zero-length string returned", dump.length() > 0);
        assertEquals( "string not ended with newline char", '\n', dump.charAt( dump.length()-1));
    }
}
