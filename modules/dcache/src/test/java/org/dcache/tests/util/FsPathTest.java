package org.dcache.tests.util;

import org.junit.Test;

import diskCacheV111.util.FsPath;

import static org.junit.Assert.*;

public class FsPathTest
{
    @Test
    public void testResolve() {
        FsPath path = FsPath.create("/pnfs/desy.de");

        assertEquals("Incorrect path constructed", path.toString(), "/pnfs/desy.de");

        path = path.resolve("zeus/users/patrick");

        assertEquals("Incorrect path added", path.toString(), "/pnfs/desy.de/zeus/users/patrick");

        path = path.resolve("../trude");

        assertEquals(".. should change 'current'", path.toString(), "/pnfs/desy.de/zeus/users/trude");

        path = path.resolve("/");

        assertEquals("'/' should remove others", path.toString(), "/");

        path = path.resolve("pnfs/cern.ch");

        assertEquals("Incorrect path added", path.toString(), "/pnfs/cern.ch");

        path = path.resolve("./../././");
        assertEquals("Incorrect path calculated", path.toString(), "/pnfs");
    }

    @Test
    public void testChroot() {
        FsPath path = FsPath.create("/pnfs/desy.de");
        assertEquals("/pnfs/desy.de/foo", path.chroot("foo").toString());
        assertEquals("/pnfs/desy.de/foo", path.chroot("/foo").toString());
        assertEquals("/pnfs/desy.de/foo", path.chroot("//foo").toString());
        assertEquals("/pnfs/desy.de/foo", path.chroot("/foo/").toString());
        assertEquals("/pnfs/desy.de/foo", path.chroot("/foo//").toString());
        assertEquals("/pnfs/desy.de/foo", path.chroot("/./foo/").toString());
        assertEquals("/pnfs/desy.de/foo", path.chroot("/foo/.").toString());
        assertEquals("/pnfs/desy.de/foo", path.chroot("/foo/./").toString());
        assertEquals("/pnfs/desy.de", path.chroot("/foo/../").toString());
        assertEquals("/pnfs/desy.de/bar", path.chroot("/foo/../bar").toString());
        assertEquals("/pnfs/desy.de", path.chroot("/foo/../..").toString());
        assertEquals("/pnfs/desy.de", path.chroot("/../").toString());
        assertEquals("/pnfs/desy.de", path.chroot("..").toString());
        assertEquals("/pnfs/desy.de", path.chroot("/..").toString());
        assertEquals("/pnfs/desy.de", path.chroot("../").toString());
        assertEquals("/pnfs/desy.de", path.chroot("../..").toString());
        assertEquals("/pnfs/desy.de", path.chroot("./..").toString());
    }

    @Test
    public void testStrip()
    {
        assertEquals("/foo/bar", FsPath.create("/my/root/foo/bar/").stripPrefix(FsPath.create("/my/root")));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testStripNoPrefix()
    {
        FsPath.create("/my/root2/foo/bar/").stripPrefix(FsPath.create("/my/root"));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testRelativePath()
    {
        FsPath.create("foo");
    }

    @Test
    public void testContains()
    {
        assertTrue(FsPath.create("/foo").contains("foo"));
        assertTrue(FsPath.create("/foo").contains(""));
        assertTrue(FsPath.create("/foo/bar").contains("foo"));
        assertTrue(FsPath.create("/foo/bar").contains("foo/bar"));
        assertTrue(FsPath.create("/foo/bar").contains("foo/bar/"));
        assertTrue(FsPath.create("/foo/bar").contains("bar"));
        assertTrue(FsPath.create("/foo/bar").contains("bar/"));
        assertTrue(FsPath.create("/").contains(""));
        assertFalse(FsPath.create("/").contains("foo"));
        assertFalse(FsPath.create("/bar").contains("foo"));
        assertFalse(FsPath.create("/bar/foo").contains("foo/bar"));
    }

    @Test
    public void testIsRoot()
    {
        assertTrue(FsPath.ROOT.isRoot());
        assertTrue(FsPath.create("/").isRoot());
        assertFalse(FsPath.create("/foo").isRoot());
        assertFalse(FsPath.ROOT.child("foo").isRoot());
    }

    @Test
    public void testParent()
    {
        assertEquals(FsPath.ROOT, FsPath.create("/foo").parent());
        assertEquals(FsPath.ROOT.child("foo"), FsPath.create("/foo/bar").parent());
    }

    @Test(expected = IllegalStateException.class)
    public void testParentOnRoot()
    {
        FsPath.ROOT.parent();
    }

    @Test
    public void testName()
    {
        assertEquals("/", FsPath.ROOT.name());
        assertEquals("foo", FsPath.ROOT.child("foo").name());
        assertEquals("bar", FsPath.ROOT.child("foo").child("bar").name());
    }

    @Test
    public void testLength()
    {
        assertEquals(0, FsPath.ROOT.length());
        assertEquals(1, FsPath.ROOT.child("foo").length());
        assertEquals(2, FsPath.ROOT.child("foo").child("bar").length());
    }

    @Test
    public void testDrop()
    {
        assertEquals(FsPath.ROOT, FsPath.ROOT.drop(0));
        assertEquals(FsPath.ROOT, FsPath.ROOT.drop(1));
        assertEquals(FsPath.ROOT, FsPath.ROOT.child("foo").drop(1));
        assertEquals(FsPath.ROOT.child("foo"), FsPath.ROOT.child("foo").child("bar").drop(1));
        assertEquals(FsPath.ROOT, FsPath.ROOT.child("foo").child("bar").drop(2));
        assertEquals(FsPath.ROOT, FsPath.ROOT.child("foo").child("bar").drop(3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDropNegative()
    {
        assertEquals(FsPath.ROOT, FsPath.ROOT.drop(-1));
    }

    @Test
    public void testPrefix()
    {
        assertTrue(FsPath.ROOT.hasPrefix(FsPath.ROOT));
        assertFalse(FsPath.ROOT.hasPrefix(FsPath.ROOT.child("foo")));
        assertTrue(FsPath.ROOT.child("foo").hasPrefix(FsPath.ROOT));
        assertTrue(FsPath.ROOT.child("foo").hasPrefix(FsPath.ROOT.child("foo")));
        assertFalse(FsPath.ROOT.child("foo").hasPrefix(FsPath.ROOT.child("foo").child("bar")));
        assertFalse(FsPath.ROOT.child("foo").hasPrefix(FsPath.ROOT.child("bar")));
    }
}
