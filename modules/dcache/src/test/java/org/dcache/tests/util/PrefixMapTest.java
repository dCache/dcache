package org.dcache.tests.util;

import org.junit.Before;
import org.junit.Test;

import diskCacheV111.util.FsPath;

import org.dcache.util.PrefixMap;

import static org.junit.Assert.assertEquals;

public class PrefixMapTest
{
    private PrefixMap<Integer> _map;

    @Before
    public void setup()
    {
        _map = new PrefixMap<>();
        _map.put(new FsPath("/a"), 1);
        _map.put(new FsPath("/a//b"), 2);
        _map.put(new FsPath("/aa"), 3);
        _map.put(new FsPath("/b/"), 4);
    }

    @Test
    public void lookup()
    {
        assertEquals(1, (int) _map.get(new FsPath("/a")));
        assertEquals(1, (int) _map.get(new FsPath("/a/")));
        assertEquals(1, (int) _map.get(new FsPath("/a/bb")));
        assertEquals(1, (int) _map.get(new FsPath("/a//c")));
        assertEquals(1, (int) _map.get(new FsPath("/a/c/")));
        assertEquals(1, (int) _map.get(new FsPath("/a/c/d")));

        assertEquals(2, (int) _map.get(new FsPath("/a/b")));
        assertEquals(2, (int) _map.get(new FsPath("/a/b/")));
        assertEquals(2, (int) _map.get(new FsPath("/a/b/c")));

        assertEquals(3, (int) _map.get(new FsPath("/aa")));
        assertEquals(3, (int) _map.get(new FsPath("/aa//")));
        assertEquals(3, (int) _map.get(new FsPath("/aa/b")));

        assertEquals(4, (int) _map.get(new FsPath("/b")));
        assertEquals(4, (int) _map.get(new FsPath("/b/")));
        assertEquals(4, (int) _map.get(new FsPath("/b/a")));

        assertEquals(null, _map.get(new FsPath("/aaa")));
        assertEquals(null, _map.get(new FsPath("/")));
        assertEquals(null, _map.get(new FsPath("/c")));
        assertEquals(null, _map.get(new FsPath("/bb")));
    }

    @Test
    public void allowRedundant()
    {
        _map.put(new FsPath("/a/foo"), 1);
    }

    @Test
    public void replaceDuplicate()
    {
        _map.put(new FsPath("/a/"), 5);
        assertEquals(5, (int) _map.get(new FsPath("/a/foo")));
    }

    @Test
    public void lookupEmptyMap()
    {
        PrefixMap<Object> map = new PrefixMap<>();
        assertEquals(null, map.get(new FsPath("/foo")));
    }

    @Test(expected=IllegalArgumentException.class)
    public void failNullGet()
    {
        _map.get(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void failNullPut()
    {
        _map.put(null, 1);
    }

    @Test(expected=IllegalArgumentException.class)
    public void failNullValue()
    {
        _map.put(new FsPath("/"), null);
    }

    @Test
    public void normalizeDot()
    {
        assertEquals(2, (int) _map.get(new FsPath("/a/./b/")));
    }

    @Test
    public void normalizeDotDot()
    {
        assertEquals(4, (int) _map.get(new FsPath("/a/../b/c")));
    }

}
