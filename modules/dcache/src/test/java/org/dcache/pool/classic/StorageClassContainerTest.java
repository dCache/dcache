package org.dcache.pool.classic;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.CacheException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.dcache.chimera.InodeId;
import org.dcache.namespace.FileType;
import org.dcache.pool.nearline.HsmSet;
import org.dcache.pool.nearline.NearlineStorageHandler;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;

public class StorageClassContainerTest {

    // fake clock to monotonically increase creation times
    private final AtomicLong birth = new AtomicLong();

    // NOTE: hsm type and name must be lowercase!
    public static final String HSM_A = "hsm-a";
    public static final String HSM_B = "hsm-b";

    private StorageClassContainer scc;
    private NearlineStorageHandler nearlineStorageHandler;

    private NearlineStorage nearlineStorageA;
    private NearlineStorage nearlineStorageB;

    @Before
    public void setUp() throws Exception {
        HsmSet hsmSet = mock(HsmSet.class);

        nearlineStorageA = mock(NearlineStorage.class);
        when(hsmSet.getNearlineStorageByName(HSM_A)).thenReturn(nearlineStorageA);
        when(hsmSet.getNearlineStorageByType(HSM_A)).thenReturn(nearlineStorageA);

        nearlineStorageB = mock(NearlineStorage.class);
        when(hsmSet.getNearlineStorageByName(HSM_B)).thenReturn(nearlineStorageB);
        when(hsmSet.getNearlineStorageByType(HSM_B)).thenReturn(nearlineStorageB);

        nearlineStorageHandler = new NearlineStorageHandler();
        nearlineStorageHandler.setHsmSet(hsmSet);

        scc = new StorageClassContainer();
        scc.setNearlineStorageHandler(nearlineStorageHandler);
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

    @Test
    public void testAddOne() throws CacheException, InterruptedException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));

        assertThat(scc.getStorageClassCount(), is(1));
    }

    @Test
    public void testAddMultipleSameClass() throws CacheException, InterruptedException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));

        assertThat(scc.getStorageClassCount(), is(1));
    }

    @Test
    public void testAddMultipleDifferentClass() throws CacheException, InterruptedException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        scc.addCacheEntry(givenCacheEntry(HSM_B, "a:b"));

        assertThat(scc.getStorageClassCount(), is(2));
    }

    @Test
    public void testAddMultipleDifferentHSM() throws CacheException, InterruptedException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        scc.addCacheEntry(givenCacheEntry(HSM_B, "a:b"));

        assertThat(scc.getStorageClassCount(), is(2));
    }

    @Test
    public void testFistInFifo() throws CacheException, InterruptedException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        scc.addCacheEntry(givenCacheEntry(HSM_B, "a:b"));

        scc.flushAll(1, 1_000, MoverRequestScheduler.Order.FIFO);

        verify(nearlineStorageA).flush(any());
    }

    private void assertMatchTemplate(StorageClassInfo template, StorageClassInfo actual) {
        assertEquals(template.getExpiration(), actual.getExpiration());
        assertEquals(template.getMaxSize(), actual.getMaxSize());
        assertEquals(template.getPending(), actual.getPending());
        assertEquals(template.isOpen(), actual.isOpen());
    }


    private CacheEntry givenCacheEntry(String hsm, String storageClass) {

        long ts = birth.incrementAndGet();
        var attr = FileAttributes.of()
              .fileType(FileType.REGULAR)
              .size(ThreadLocalRandom.current().nextLong(1_000_000))
              .storageClass(storageClass)
              .pnfsId(InodeId.newID(0))
              .hsm(hsm)
              .creationTime(ts)
              .accessTime(ts)
              .modificationTime(ts)
              .build();

        var ce = mock(CacheEntry.class);
        when(ce.getFileAttributes()).thenReturn(attr);
        when(ce.getPnfsId()).thenReturn(attr.getPnfsId());
        when(ce.getCreationTime()).thenReturn(attr.getCreationTime());
        when(ce.getLastAccessTime()).thenReturn(attr.getAccessTime());
        when(ce.getState()).thenReturn(ReplicaState.PRECIOUS);

        return ce;
    }
}