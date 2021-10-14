package org.dcache.tests.util;

import diskCacheV111.util.CacheException;
import org.junit.Test;

public class CecheExceptionTest {


    @Test
    public void testCacheExceptionNoMEssage() {

        CacheException cacheException = new CacheException(CacheException.FILE_NOT_FOUND, null);

    }

}
