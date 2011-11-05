package org.dcache.tests.util;

import org.junit.Test;

import diskCacheV111.util.CacheException;

public class CecheExceptionTest {


    @Test
    public void testCacheExceptionNoMEssage() {

        CacheException cacheException = new CacheException(CacheException.FILE_NOT_FOUND, null);

    }

}
