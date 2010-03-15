package org.dcache.services.info.base.guides;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.dcache.services.info.base.StatePath;
import org.junit.Before;
import org.junit.Test;

public class SubtreeStateGuideTests {

    private static final StatePath SUBTREE_ROOT = StatePath.parsePath( "aa.bb");
    private static final StatePath ROOT = null;

    SubtreeStateGuide _guide;

    @Before
    public void setUp() {
        _guide = new SubtreeStateGuide( SUBTREE_ROOT);
    }

    @Test
    public void testRootIsVisitable() {
        assertTrue( "root is visitable", _guide.isVisitable( ROOT));
    }

    @Test
    public void testSubtreeParentIsVisitable() {
        StatePath parent = SUBTREE_ROOT.parentPath();
        assertTrue( "subtree-root's parent is visitable", _guide.isVisitable( parent));
    }

    @Test
    public void testSubtreeIsVisitable() {
        assertTrue( "subtree-root is visitable", _guide.isVisitable( SUBTREE_ROOT));
    }

    @Test
    public void testSubtreeRootChildIsVisitable() {
        StatePath child = SUBTREE_ROOT.newChild( "cc");
        assertTrue( "subtree-root child is visitable", _guide.isVisitable( child));
    }

    @Test
    public void testSubtreeRootSiblingIsNotVisitable() {
        StatePath parent = SUBTREE_ROOT.parentPath();
        StatePath sibling = parent.newChild( "dd");
        assertFalse( "subtree-root sibling is not visitable", _guide.isVisitable( sibling));
    }

    @Test
    public void testRootNotInSubtree() {
        assertFalse( "root not in subtree", _guide.isInSubtree( ROOT));
    }

    @Test
    public void testParentNotInSubtree() {
        StatePath parent = SUBTREE_ROOT.parentPath();
        assertFalse( "subtree parent not in subtree", _guide.isInSubtree( parent));
    }

    @Test
    public void testSubtreeRootNotInSubtree() {
        assertTrue( "subtree root in subtree", _guide.isInSubtree( SUBTREE_ROOT));
    }

    @Test
    public void testSubtreeRootChildInSubtree() {
        StatePath child = SUBTREE_ROOT.newChild( "cc");
        assertTrue( "child not in subtree", _guide.isInSubtree( child));
    }
}
