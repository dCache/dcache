package org.dcache.tests.util;


import static org.junit.Assert.*;

import org.junit.Test;

import diskCacheV111.util.FileMetaData;

public class FileMetaDataTest {

    @Test
    public void testEquals() {

        FileMetaData m1 =  new FileMetaData(true, 3750, 1000, 0755);
        FileMetaData m2 =  new FileMetaData(true, 3750, 1000, 0755);

        assertEquals("Two file meta data objects with same attributes have to be equal", m1, m2);

    }


}
