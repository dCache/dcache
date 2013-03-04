package org.dcache.util;

import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;

import static org.junit.Assert.assertTrue;

public class LifoPriorityComparatorTest {

    private Comparator<IoPrioritizable> _comparator;

    @Before
    public void setUp() {
        _comparator = new LifoPriorityComparator();
    }

    @Test
    public void testEquals() {
        IoPrioritizable p = new DummyRequest(IoPriority.LOW, 0);

        assertTrue("equal request not detected", _comparator.compare(p, p) == 0);
    }

    @Test
    public void testEqualsCtimeDifferentPriority() {
        IoPrioritizable p1 = new DummyRequest(IoPriority.LOW, 0);
        IoPrioritizable p2 = new DummyRequest(IoPriority.HIGH, 0);
        assertTrue("different priorities not detected", _comparator.compare(p1, p2) < 0);
    }

    @Test
    public void testEqualsPriorityDifferentCtime() {
        IoPrioritizable p1 = new DummyRequest(IoPriority.LOW, 0);
        IoPrioritizable p2 = new DummyRequest(IoPriority.LOW, 1);
        assertTrue("different ctime not detected", _comparator.compare(p1, p2) < 0);
    }

    private static class DummyRequest implements IoPrioritizable {

        private final IoPriority _priority;
        private final long _ctime;

        public DummyRequest(IoPriority priority, long ctime) {
            _priority = priority;
            _ctime = ctime;
        }

        @Override
        public long getCreateTime() {
            return _ctime;
        }

        @Override
        public IoPriority getPriority() {
            return _priority;
        }
    }
}
