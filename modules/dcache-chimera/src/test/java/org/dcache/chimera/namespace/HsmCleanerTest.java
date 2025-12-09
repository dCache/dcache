package org.dcache.chimera.namespace;

import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;
import dmg.cells.nucleus.CellPath;
import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;
import org.dcache.cells.CellStub;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.*;

import static org.mockito.Mockito.*;

public class HsmCleanerTest {

    private HsmCleaner cleaner;
    private final Set<URI> deletedUris = new HashSet<>();
    private final Set<URI> failedUris = new HashSet<>();

    @Before
    public void setUp() {
        deletedUris.clear();
        failedUris.clear();
        cleaner = new HsmCleaner();
        cleaner._hasHaLeadership = true;

        CellStub poolStub = mock(CellStub.class);
        doNothing().when(poolStub).notify(any(CellPath.class), any(Serializable.class));
        cleaner.setPoolStub(poolStub);

        cleaner.setSuccessSink(deletedUris::add);
        cleaner.setFailureSink(failedUris::add);

        cleaner.setMaxCachedDeleteLocations(3);
        cleaner.setMaxFilesPerRequest(10);
        cleaner.setHsmTimeout(10);
        cleaner.setGracePeriod(Duration.of(1, ChronoUnit.SECONDS));
        cleaner.setHsmTimeoutUnit(java.util.concurrent.TimeUnit.SECONDS);
    }

    @Test
    public void testSubmitAddsToCorrectHsmBucket() throws Exception {
        URI uri = new URI("osm://hsm3/path/to/file4");
        cleaner.submit(uri);

        Assert.assertTrue(cleaner.getLocationsToDeletePerHsm().containsKey("hsm3"));
        Assert.assertTrue(cleaner.getLocationsToDeletePerHsm().get("hsm3").contains(uri));
    }

    @Test
    public void testFlushActivatesCleaningForHsmWithConnectedPool() {
        cleaner.setMaxCachedDeleteLocations(3);
        URI uri1 = URI.create("osm://hsm1/path/to/file1");
        URI uri2 = URI.create("osm://hsm1/path/to/file2");
        URI uri3 = URI.create("osm://hsm1/path/to/file3");

        cleaner.addLocationsToDeletePerHsm("hsm1", Set.of(uri1, uri2, uri3));

        PoolInformation poolsInfoList = mock(PoolInformation.class);
        when(poolsInfoList.getName()).thenReturn("pool-A");

        PoolInformationBase mockPools = mock(PoolInformationBase.class);
        when(mockPools.getAvailablePoolsWithHSM("hsm1"))
              .thenReturn(List.of(poolsInfoList));
        cleaner._pools = mockPools;

        cleaner.runDelete();

        Assert.assertFalse(cleaner.getLocationsToDeletePerHsm().containsKey("hsm1"));

        Assert.assertTrue(cleaner.getActiveDeletesPerPool().containsKey("pool-A"));
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().get("pool-A").contains(uri1));
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().get("pool-A").contains(uri2));
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().get("pool-A").contains(uri3));
    }

    @Test
    public void testFlushActivatesCleaningForHsmsWithConnectedPool() {
        cleaner.setMaxCachedDeleteLocations(3);
        URI uri1 = URI.create("osm://hsm1/path/to/file1");
        URI uri2 = URI.create("osm://hsm1/path/to/file2");
        URI uri3 = URI.create("osm://hsm2/path/to/file3");
        URI uri4 = URI.create("osm://hsm3/path/to/file4");

        cleaner.addLocationsToDeletePerHsm("hsm1", Set.of(uri1, uri2));
        cleaner.addLocationsToDeletePerHsm("hsm2", Set.of(uri3));
        cleaner.addLocationsToDeletePerHsm("hsm3", Set.of(uri4));

        PoolInformation poolsInfoListHsm1 = mock(PoolInformation.class);
        when(poolsInfoListHsm1.getName()).thenReturn("pool-A");
        PoolInformation poolsInfoListHsm2 = mock(PoolInformation.class);
        when(poolsInfoListHsm2.getName()).thenReturn("pool-B");

        PoolInformationBase mockPools = mock(PoolInformationBase.class);
        when(mockPools.getAvailablePoolsWithHSM("hsm1"))
              .thenReturn(List.of(poolsInfoListHsm1));
        when(mockPools.getAvailablePoolsWithHSM("hsm2"))
              .thenReturn(List.of(poolsInfoListHsm2));
        cleaner._pools = mockPools;

        cleaner.runDelete();

        Assert.assertFalse(cleaner.getLocationsToDeletePerHsm().containsKey("hsm1"));
        Assert.assertFalse(cleaner.getLocationsToDeletePerHsm().containsKey("hsm2"));

        Assert.assertTrue(cleaner.getActiveDeletesPerPool().containsKey("pool-A"));
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().get("pool-A").contains(uri1));
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().get("pool-A").contains(uri2));
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().containsKey("pool-B"));
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().get("pool-B").contains(uri3));
    }

    @Test
    public void testFlushActivatesLimitedCleaningForHsmWithConnectedPool() {
        cleaner.setMaxCachedDeleteLocations(3);
        cleaner.setMaxFilesPerRequest(1);
        URI uri1 = URI.create("osm://hsm1/path/to/file1");
        URI uri2 = URI.create("osm://hsm1/path/to/file2");
        URI uri3 = URI.create("osm://hsm1/path/to/file3");

        cleaner.addLocationsToDeletePerHsm("hsm1", Set.of(uri1, uri2, uri3));

        PoolInformation poolsInfoList = mock(PoolInformation.class);
        when(poolsInfoList.getName()).thenReturn("pool-A");

        PoolInformationBase mockPools = mock(PoolInformationBase.class);
        when(mockPools.getAvailablePoolsWithHSM("hsm1"))
              .thenReturn(List.of(poolsInfoList));
        cleaner._pools = mockPools;

        cleaner.runDelete();

        Assert.assertTrue(cleaner.getLocationsToDeletePerHsm().containsKey("hsm1"));
        Assert.assertEquals(2, cleaner.getLocationsToDeletePerHsm().get("hsm1").size());

        Assert.assertTrue(cleaner.getActiveDeletesPerPool().containsKey("pool-A"));
        Assert.assertEquals(1, cleaner.getActiveDeletesPerPool().get("pool-A").size());
    }

    @Test
    public void testSuccessiveFlushesActivatesCleaningOnDifferentPoolsForSameHsm() {
        cleaner.setMaxCachedDeleteLocations(3);
        cleaner.setMaxFilesPerRequest(2);
        URI uri1 = URI.create("osm://hsm1/path/to/file1");
        URI uri2 = URI.create("osm://hsm1/path/to/file2");
        URI uri3 = URI.create("osm://hsm1/path/to/file3");

        cleaner.addLocationsToDeletePerHsm("hsm1", Set.of(uri1, uri2, uri3));

        PoolInformation poolsInfo1 = mock(PoolInformation.class);
        when(poolsInfo1.getName()).thenReturn("pool-A");
        PoolInformation poolsInfo2 = mock(PoolInformation.class);
        when(poolsInfo2.getName()).thenReturn("pool-B");

        PoolInformationBase mockPools = mock(PoolInformationBase.class);
        when(mockPools.getAvailablePoolsWithHSM("hsm1")).thenReturn(
              List.of(poolsInfo1, poolsInfo2));
        cleaner._pools = mockPools;

        cleaner.runDelete();
        // should set the first 2 files to be cleaned on one of the pools

        Assert.assertTrue(cleaner.getLocationsToDeletePerHsm().containsKey("hsm1"));
        Assert.assertEquals(1, cleaner.getLocationsToDeletePerHsm().get("hsm1").size());

        Assert.assertFalse(cleaner.getActiveDeletesPerPool().isEmpty());
        String firstPool = cleaner.getActiveDeletesPerPool().keySet().iterator().next();
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().containsKey(firstPool));
        Assert.assertEquals(2, cleaner.getActiveDeletesPerPool().get(firstPool).size());

        cleaner.runDelete();
        // should set the last file to be cleaned on the other pool

        Assert.assertFalse(cleaner.getLocationsToDeletePerHsm().containsKey("hsm1"));

        Assert.assertFalse(cleaner.getActiveDeletesPerPool().isEmpty());
        String secondPool = "pool-A".equals(firstPool) ? "pool-B" : "pool-A";
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().containsKey(secondPool));
        Assert.assertEquals(1, cleaner.getActiveDeletesPerPool().get(secondPool).size());
    }

    @Test
    public void testFlushRemovesCachedLocationsForHsmWithoutConnectedPool() {
        cleaner.setMaxCachedDeleteLocations(3);
        URI uri1 = URI.create("osm://hsm1/path/to/file1");
        URI uri2 = URI.create("osm://hsm1/path/to/file2");
        URI uri3 = URI.create("osm://hsm1/path/to/file3");

        cleaner.addLocationsToDeletePerHsm("hsm1", Set.of(uri1, uri2, uri3));

        PoolInformationBase mockPools = mock(PoolInformationBase.class);
        when(mockPools.getAvailablePoolsWithHSM("hsm1"))
              .thenReturn(List.of());
        cleaner._pools = mockPools;

        cleaner.runDelete();

        Assert.assertFalse(cleaner.getLocationsToDeletePerHsm().containsKey("hsm1"));
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().isEmpty());
        Assert.assertTrue(failedUris.containsAll(Set.of(uri1, uri2, uri3)));
    }

    @Test
    public void testPoolSendsDeleteConfirmationForAllPending() {
        cleaner.setMaxCachedDeleteLocations(3);
        URI uri1 = URI.create("osm://hsm1/path/to/file1");
        URI uri2 = URI.create("osm://hsm1/path/to/file2");
        URI uri3 = URI.create("osm://hsm1/path/to/file3");

        cleaner.addActiveDeletesPerPool("pool-A", new HashSet<>(Set.of(uri1, uri2, uri3)));

        PoolRemoveFilesFromHSMMessage msg = new PoolRemoveFilesFromHSMMessage("pool-A",
              "hsm1", new HashSet<>(Set.of(uri1, uri2, uri3)));
        msg.setResult(Set.of(uri1, uri2, uri3), Set.of());
        msg.setSucceeded();

        cleaner.messageArrived(msg);

        Assert.assertTrue(cleaner.getLocationsToDeletePerHsm().isEmpty());
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().isEmpty());
        Assert.assertTrue(deletedUris.containsAll(Set.of(uri1, uri2, uri3)));
        Assert.assertTrue(failedUris.isEmpty());
    }

    @Test
    public void testPoolSendsDeleteConfirmationForSomePending() {
        URI uri1 = URI.create("osm://hsm1/path/to/file1");
        URI uri2 = URI.create("osm://hsm1/path/to/file2");
        URI uri3 = URI.create("osm://hsm1/path/to/file3");

        Set<URI> successUris = Set.of(uri1, uri2);
        Set<URI> failUris = Set.of(uri3);

        cleaner.addActiveDeletesPerPool("pool-A", new HashSet<>(Set.of(uri1, uri2, uri3)));

        PoolRemoveFilesFromHSMMessage msg = new PoolRemoveFilesFromHSMMessage("pool-A",
              "hsm1", new HashSet<>(Set.of(uri1, uri2, uri3)));
        msg.setResult(successUris, failUris);
        msg.setSucceeded();

        cleaner.messageArrived(msg);

        Assert.assertTrue(cleaner.getLocationsToDeletePerHsm().isEmpty());
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().isEmpty());

        Assert.assertTrue(deletedUris.containsAll(successUris));
        Assert.assertFalse(deletedUris.contains(uri3));

        Assert.assertTrue(failedUris.containsAll(failUris));
        Assert.assertFalse(failedUris.contains(uri1));
        Assert.assertFalse(failedUris.contains(uri2));
    }

    @Test
    public void testPoolSendsDeleteConfirmationForAllFailed() {
        URI uri1 = URI.create("osm://hsm1/path/to/file1");
        URI uri2 = URI.create("osm://hsm1/path/to/file2");
        URI uri3 = URI.create("osm://hsm1/path/to/file3");

        Set<URI> failUris = Set.of(uri1, uri2, uri3);

        cleaner.addActiveDeletesPerPool("pool-A", new HashSet<>(Set.of(uri1, uri2, uri3)));

        PoolRemoveFilesFromHSMMessage msg = new PoolRemoveFilesFromHSMMessage("pool-A",
              "hsm1", new HashSet<>(Set.of(uri1, uri2, uri3)));
        msg.setResult(Set.of(), failUris);
        msg.setSucceeded();

        cleaner.messageArrived(msg);

        Assert.assertTrue(cleaner.getLocationsToDeletePerHsm().isEmpty());
        Assert.assertTrue(cleaner.getActiveDeletesPerPool().isEmpty());

        Assert.assertTrue(deletedUris.isEmpty());
        Assert.assertTrue(failedUris.containsAll(failUris));
    }

}
