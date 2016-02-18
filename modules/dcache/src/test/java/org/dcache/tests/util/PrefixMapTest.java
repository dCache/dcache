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
        _map.put(FsPath.create("/a"), 1);
        _map.put(FsPath.create("/a//b"), 2);
        _map.put(FsPath.create("/aa"), 3);
        _map.put(FsPath.create("/b/"), 4);
    }

    @Test
    public void lookup()
    {
        assertEquals(1, (int) _map.get(FsPath.create("/a")));
        assertEquals(1, (int) _map.get(FsPath.create("/a/")));
        assertEquals(1, (int) _map.get(FsPath.create("/a/bb")));
        assertEquals(1, (int) _map.get(FsPath.create("/a//c")));
        assertEquals(1, (int) _map.get(FsPath.create("/a/c/")));
        assertEquals(1, (int) _map.get(FsPath.create("/a/c/d")));

        assertEquals(2, (int) _map.get(FsPath.create("/a/b")));
        assertEquals(2, (int) _map.get(FsPath.create("/a/b/")));
        assertEquals(2, (int) _map.get(FsPath.create("/a/b/c")));

        assertEquals(3, (int) _map.get(FsPath.create("/aa")));
        assertEquals(3, (int) _map.get(FsPath.create("/aa//")));
        assertEquals(3, (int) _map.get(FsPath.create("/aa/b")));

        assertEquals(4, (int) _map.get(FsPath.create("/b")));
        assertEquals(4, (int) _map.get(FsPath.create("/b/")));
        assertEquals(4, (int) _map.get(FsPath.create("/b/a")));

        assertEquals(null, _map.get(FsPath.create("/aaa")));
        assertEquals(null, _map.get(FsPath.create("/")));
        assertEquals(null, _map.get(FsPath.create("/c")));
        assertEquals(null, _map.get(FsPath.create("/bb")));
    }

    @Test
    public void allowRedundant()
    {
        _map.put(FsPath.create("/a/foo"), 1);
    }

    @Test
    public void replaceDuplicate()
    {
        _map.put(FsPath.create("/a/"), 5);
        assertEquals(5, (int) _map.get(FsPath.create("/a/foo")));
    }

    @Test
    public void lookupEmptyMap()
    {
        PrefixMap<Object> map = new PrefixMap<>();
        assertEquals(null, map.get(FsPath.create("/foo")));
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
        _map.put(FsPath.create("/"), null);
    }

    @Test
    public void normalizeDot()
    {
        assertEquals(2, (int) _map.get(FsPath.create("/a/./b/")));
    }

    @Test
    public void normalizeDotDot()
    {
        assertEquals(4, (int) _map.get(FsPath.create("/a/../b/c")));
    }

}
