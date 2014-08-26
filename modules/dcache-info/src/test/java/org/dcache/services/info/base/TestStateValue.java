package org.dcache.services.info.base;


import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * This is a minimal non-abstract implementation of StateComponent that extends StateValue.
 * Calls to acceptVisitor() are recorded and can be listed later.
 * Several inherited abstract methods are implemented with a boobytrap: any attempt to use
 * method the abstract methods will result in the JUnit test failing.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class TestStateValue extends StateValue {

    /**
     * Information about an individual acceptVisitor method call.
     */
    static class AcceptVisitorInfo {
        private final StatePath _path;
        private final StateVisitor _visitor;

        /**
         * Record that acceptVisitor was called with given path and visitor
         */
        private AcceptVisitorInfo( StatePath path, StateVisitor visitor) {
            _path = path;
            _visitor = visitor;
        }

        StatePath getStatePath() {
            return _path;
        }

        StateVisitor getVisitor() {
            return _visitor;
        }
    }

    private final List<AcceptVisitorInfo> _avi = new LinkedList<>();

    protected TestStateValue( boolean isImmortal) {
        super( isImmortal);
    }

    protected TestStateValue( long duration) {
        super( duration);
    }

    @Override
    public void acceptVisitor( StatePath path, StateVisitor visitor) {
        _avi.add( new AcceptVisitorInfo( path, visitor));
    }

    /**
     * Acquire information about the acceptVisitor() calls.  The provided List is the calls in
     * the order they were received.
     * @return a list of information about acceptVisitor() calls.
     */
    public List<AcceptVisitorInfo> getVisitorInfo() {
        return new LinkedList<>( _avi);
    }

    @Override
    public boolean equals( Object other) {
        fail();
        return false;
    }

    @Override
    public String getTypeName() {
        fail();
        return null;
    }

    @Override
    public int hashCode() {
        fail();
        return 0;
    }
}
