package org.dcache.tests.util;


import org.junit.Test;

import diskCacheV111.util.FileMetaData;

import static org.junit.Assert.*;

public class FileMetaDataTest {

    @Test
    public void testEqualsByUserMode() {

        FileMetaData m1 =  new FileMetaData(3750, 1000, 0755);
        FileMetaData m2 =  new FileMetaData(3750, 1000, 0755);

        assertTrue("Two file meta data objects with same uid, gid, and mode have to be equal", m1.equals( m2 ));
        assertTrue("Two equal meta data have to has equal hashCode", m1.hashCode() == m2.hashCode());

        m2.setGid(1);
        assertFalse("Two file meta data objects with differ  gid can't be equal", m1.equals( m2 ));
    }

    @Test
    public void testEqualsDIrectoryToFile() {

        FileMetaData m1 =  new FileMetaData(true, 3750, 1000, 0755);
        FileMetaData m2 =  new FileMetaData(false, 3750, 1000, 0755);

        assertFalse("Directory cant be equal to a file even with the same uid, gid, and mode", m1.equals( m2 ));
    }

    @Test
    public void testEqualsDirectoriesWithDifferentSizes() {
        FileMetaData m1 = new FileMetaData( true, 3750, 1000, 0755);
        FileMetaData m2 = new FileMetaData( true, 3750, 1000, 0755);

        m1.setSize( 42);
        m2.setSize( 23);

        assertTrue( "Directories differing only in size should be equal", m1.equals( m2));
    }

    @Test
    public void testEqualsBySize() {

        FileMetaData m1 =  new FileMetaData();
        FileMetaData m2 =  new FileMetaData();

        m1.setSize(17);

        assertFalse("Two file meta data objects with different size can't be equal", m1.equals( m2 ));

    }

    @Test
    public void testEqualsByMtime() {

        long now = System.currentTimeMillis();

        FileMetaData m1 =  new FileMetaData();
        FileMetaData m2 =  new FileMetaData();

        m1.setLastModifiedTime(now);

        assertFalse("Two file meta data objects with different mtime can't be equal", m1.equals( m2 ));

        m2.setLastModifiedTime(now);
        assertTrue("Two file meta data objects with equal mtime have to be equal", m1.equals( m2 ));
    }

    @Test
    public void testEqualsByAtime() {

        long now = System.currentTimeMillis();
        FileMetaData m1 =  new FileMetaData();
        FileMetaData m2 =  new FileMetaData();

        m1.setLastAccessedTime(now);

        assertFalse("Two file meta data objects with different atime can't be equal", m1.equals( m2 ));

        m2.setLastAccessedTime(now);
        assertTrue("Two file meta data objects with equal atime have to be equal", m1.equals( m2 ));

    }

    @Test
    public void testTypeIsDirectory() {
        FileMetaData fm = new FileMetaData(true, 0, 0, 0755);
        assertTrue("should be a directory", fm.isDirectory());
        assertFalse("should not be a file", fm.isRegularFile());
    }

    @Test
    public void testTypeIsFile() {
        FileMetaData fm = new FileMetaData(false, 0, 0, 0755);
        assertTrue("should be a file", fm.isRegularFile());
        assertFalse("should not be a directory", fm.isDirectory());
    }

    @Test
    public void testTypes() {


        FileMetaData m1 =  new FileMetaData();

        try {
            m1.setFileType(true, true, true);
            fail("Filesystem object can't be a link, a directory and a regular file at the same time.");
        }catch(IllegalArgumentException iae) {
            // it's ok
        }

        try {
            m1.setFileType(true, true, false);
            fail("Filesystem object can't be a directory and a regular file at the same time.");
        }catch(IllegalArgumentException iae) {
            // it's ok
        }

        try {
            m1.setFileType(false, true, true);
            fail("Filesystem object can't be a link and  a directory at the same time.");
        }catch(IllegalArgumentException iae) {
            // it's ok
        }

        try {
            m1.setFileType(true, false, true);
            fail("Filesystem object can't be a link and a regular file at the same time.");
        }catch(IllegalArgumentException iae) {
            // it's ok
        }

        try {
            m1.setFileType(false, false, false);
            fail("Filesystem object have to be a link or a directory or a regular file.");
        }catch(IllegalArgumentException iae) {
            // it's ok
        }

    }
}
