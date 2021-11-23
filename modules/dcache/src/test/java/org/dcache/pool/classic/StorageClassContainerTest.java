package org.dcache.pool.classic;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import diskCacheV111.util.CacheException;
import java.util.concurrent.TimeUnit;
import org.dcache.pool.nearline.NearlineStorageHandler;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.vehicles.FileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class StorageClassContainerTest {


    private StorageClassContainer scc;


    @Before
    public void setUp() throws Exception {

        scc = new StorageClassContainer();
        scc.setNearlineStorageHandler(mock(NearlineStorageHandler.class));
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testTemplatePropagation() throws CacheException, InterruptedException {

        var template1 = scc.defineStorageClass("osm1", "*");
        template1.setOpen(false);
        template1.setExpiration(TimeUnit.SECONDS.toMillis(1));
        template1.setMaxSize(2);
        template1.setPending(3);

        var template2 = scc.defineStorageClass("osm2", "*");
        template2.setOpen(true);
        template2.setExpiration(TimeUnit.SECONDS.toMillis(4));
        template2.setMaxSize(5);
        template2.setPending(6);

        var ce1 = givenCacheEntry("osm1", "test:tape");
        scc.addCacheEntry(ce1);

        var ce2 = givenCacheEntry("osm2", "test:tape");
        scc.addCacheEntry(ce2);

        StorageClassInfo sci1 = scc.getStorageClassInfo("osm1", "test:tape");
        assertMatchTemplate(template1, sci1);

        StorageClassInfo sci2 = scc.getStorageClassInfo("osm2", "test:tape");
        assertMatchTemplate(template2, sci2);

    }

    private void assertMatchTemplate(StorageClassInfo template, StorageClassInfo actual) {
        assertEquals(template.getExpiration(), actual.getExpiration());
        assertEquals(template.getMaxSize(), actual.getMaxSize());
        assertEquals(template.getPending(), actual.getPending());
        assertEquals(template.isOpen(), actual.isOpen());
    }


    private CacheEntry givenCacheEntry(String hsm, String storageClass) {


        var attr = FileAttributes.of()
              .storageClass(storageClass)
              .hsm(hsm)
              .build();

        var ce = mock(CacheEntry.class);
        when(ce.getFileAttributes()).thenReturn(attr);

        return ce;
    }
}