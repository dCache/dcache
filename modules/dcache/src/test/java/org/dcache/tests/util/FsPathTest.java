package org.dcache.tests.util;

import org.junit.Test;

import diskCacheV111.util.FsPath;

import static org.junit.Assert.assertEquals;

public class FsPathTest
{
    @Test
    public void testFsPath() {
        FsPath path = new FsPath("/pnfs/desy.de");

        assertEquals("Incorrest path constructed", path.toString(), "/pnfs/desy.de");

        path.add("zeus/users/patrick");

        assertEquals("Incorrest path added", path.toString(), "/pnfs/desy.de/zeus/users/patrick");

        path.add("../trude");

        assertEquals(".. should change 'current'", path.toString(), "/pnfs/desy.de/zeus/users/trude");

        path.add("/");

        assertEquals("'/' should remove others", path.toString(), "/");

        path.add("pnfs/cern.ch");

        assertEquals("Incorrest path added", path.toString(), "/pnfs/cern.ch");

        path.add("./../././");
        assertEquals("Incorrest path calculated", path.toString(), "/pnfs");
    }

    @Test
    public void testRelativize()
    {
        assertEquals(new FsPath("/foo/bar"),
                     new FsPath("/my/root").relativize(new FsPath("/my/root/foo/bar/")));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testRelativizeNoPrefix()
    {
        new FsPath("/my/root").relativize(new FsPath("/my/root2/foo/bar/"));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testRelativePath()
    {
        new FsPath("foo");
    }
}
