package org.dcache.chimera.posix;

import java.time.Instant;
import java.util.TimeZone;
import static org.junit.Assert.assertEquals;

import org.dcache.chimera.UnixPermission;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.Test;

public class StatTest {

    // Central European Summer Time, which is UTC + 2.
    private static final TimeZone CEST = TimeZone.getTimeZone("GMT+2:00");

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedNotDefeinedGetDev() {
        new Stat().getDev();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetIno() {
        new Stat().getIno();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetMode() {
        new Stat().getMode();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetNlink() {
        new Stat().getNlink();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetUid() {
        new Stat().getUid();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetGid() {
        new Stat().getGid();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetRdev() {
        new Stat().getRdev();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetSize() {
        new Stat().getSize();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetATime() {
        new Stat().getATime();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetMTime() {
        new Stat().getMTime();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetCTime() {
        new Stat().getCTime();
    }

    @Test(expected = IllegalStateException.class)
    public void testNotDefeinedGetGeneration() {
        new Stat().getGeneration();
    }

    @Test
    public void testGetDev() {
        Stat stat = new Stat();
        stat.setDev(1);
        assertEquals(1, stat.getDev());
    }

    @Test
    public void testGetIno() {
        Stat stat = new Stat();
        stat.setIno(1);
        assertEquals(1, stat.getIno());

    }

    @Test
    public void testGetMode() {
        Stat stat = new Stat();
        stat.setMode(0755 | UnixPermission.S_IFDIR);
        assertEquals(0755 | UnixPermission.S_IFDIR, stat.getMode());
    }

    @Test
    public void testGetNlink() {
        Stat stat = new Stat();
        stat.setNlink(1);
        assertEquals(1, stat.getNlink());

    }

    @Test
    public void testGetUid() {
        Stat stat = new Stat();
        stat.setUid(1);
        assertEquals(1, stat.getUid());
    }

    @Test
    public void testGetGid() {
        Stat stat = new Stat();
        stat.setGid(1);
        assertEquals(1, stat.getGid());
    }

    @Test
    public void testGetRdev() {
        Stat stat = new Stat();
        stat.setRdev(1);
        assertEquals(1, stat.getRdev());
    }

    @Test
    public void testGetSize() {
        Stat stat = new Stat();
        stat.setSize(1);
        assertEquals(1, stat.getSize());
    }

    @Test
    public void testGetATime() {
        Stat stat = new Stat();
        stat.setATime(1);
        assertEquals(1, stat.getATime());
    }

    @Test
    public void testGetMTime() {
        Stat stat = new Stat();
        stat.setMTime(1);
        assertEquals(1, stat.getMTime());
    }

    @Test
    public void testGetCTime() {
        Stat stat = new Stat();
        stat.setCTime(1);
        assertEquals(1, stat.getCTime());
    }

    @Test
    public void testGetGeneration() {
        Stat stat = new Stat();
        stat.setGeneration(1);
        assertEquals(1, stat.getGeneration());
    }

    private String toStringWithDefaultTimeZone(Stat target, TimeZone zone) {
        try {
            TimeZone.setDefault(zone);
            return target.toString();
        } finally {
            TimeZone.setDefault(null);
        }
    }

    @Test
    public void testToStringWithFullyPopulatedStat() {
        Stat stat = new Stat();
        stat.setMTime(Instant.parse("1980-08-07T12:00:00Z").toEpochMilli());
        stat.setMode(0644 | UnixPermission.S_IFREG);
        stat.setNlink(2);
        stat.setUid(3);
        stat.setGid(5);
        stat.setSize(7);
        stat.setGeneration(11);
        String toString = toStringWithDefaultTimeZone(stat, CEST);
        assertThat(toString, equalTo("-rw-r--r--        2      3      5      7     11 1980-08-07 14:00:00"));
    }

    @Test
    public void testToStringWithOnlyMtime() {
        Stat stat = new Stat();
        stat.setMTime(Instant.parse("1980-08-07T12:00:00Z").toEpochMilli());
        String toString = toStringWithDefaultTimeZone(stat, CEST);
        assertThat(toString, equalTo("- - - - - - 1980-08-07 14:00:00"));
    }
}
