package org.dcache.space;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLinkGroup;
import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.SpaceState;
import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.GenericStorageInfo;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.vehicles.FileAttributes;
import org.junit.Test;

public class CapacityTest {

    private final PoolMonitor poolMonitor = mock(PoolMonitor.class);
    private Optional<Space> space = Optional.of(space(42, 1000, 300, 200));

    @Test
    public void shouldReportFixedSizeForNamespace() throws Exception {
        Capacity capacity = provider("namespace", "").get();

        assertThat(capacity.getTotalSpace(), is(1L << 60));
        assertThat(capacity.getUsedSpace(), is(0L));
    }

    @Test
    public void shouldSumSpaceOfAllPools() throws Exception {
        givenPools(pool("pool1", 1000, 100, 50), pool("pool2", 2000, 300, 0));

        Capacity capacity = provider("pools", "").get();

        assertThat(capacity.getTotalSpace(), is(3000L));
        assertThat(capacity.getFreeSpace(), is(450L));
    }

    @Test
    public void shouldSumSpaceOfWritePoolsOfPath() throws Exception {
        givenPools(pool("pool1", 1000, 100, 50), pool("pool2", 2000, 300, 0));
        givenWritePools("pool2");

        Capacity capacity = provider("pools", "").get(path(null), "192.0.2.1", "Xrootd/5");

        assertThat(capacity.getTotalSpace(), is(2000L));
        assertThat(capacity.getFreeSpace(), is(300L));
    }

    @Test
    public void shouldSumWritePoolsOfLinkGroupOfWriteToken() throws Exception {
        givenPools(pool("pool1", 1000, 100, 50), pool("pool2", 2000, 300, 0));
        givenLinkGroupWritePools("lg", "pool2");
        Capacity.Provider provider = provider("pools", "");
        provider.setSpaceReservation(true);
        provider.setSpaceManager(spaceManager(42, "lg"));

        Capacity capacity = provider.get(path("42"), "192.0.2.1", "Xrootd/5");

        assertThat(capacity.getTotalSpace(), is(2000L));
    }

    @Test
    public void shouldPreferLinkGroupsWithoutWriteToken() throws Exception {
        givenPools(pool("pool1", 1000, 100, 50), pool("pool2", 2000, 300, 0));
        givenLinkGroupWritePools("lg", "pool2");
        Capacity.Provider provider = provider("pools", "");
        provider.setSpaceReservation(true);

        Capacity capacity = provider.get(path(null), "192.0.2.1", "Xrootd/5");

        assertThat(capacity.getTotalSpace(), is(2000L));
    }

    @Test
    public void shouldReportFixedSizeIfPathIsNotWritable() throws Exception {
        givenPools(pool("pool1", 1000, 100, 50));
        givenWritePools();

        Capacity capacity = provider("pools", "").get(path(null), "192.0.2.1", "Xrootd/5");

        assertThat(capacity.getTotalSpace(), is(1L << 60));
    }

    @Test
    public void shouldFailWithoutPools() {
        givenPools();

        CacheException e = assertThrows(CacheException.class,
              () -> provider("pools", "").get());
        assertThat(e.getRc(), is(CacheException.SERVICE_UNAVAILABLE));
    }

    @Test
    public void shouldReportConfiguredReservation() throws Exception {
        Capacity.Provider provider = provider("reservation", "42");

        Capacity capacity = provider.get(path("7"), null, null);

        assertThat(provider.usesWriteToken(), is(false));
        assertThat(capacity.getTotalSpace(), is(1000L));
        assertThat(capacity.getFreeSpace(), is(500L));
        assertThat(capacity.getReservation(), is(Optional.of("42")));
    }

    @Test
    public void shouldReportReservationOfWriteToken() throws Exception {
        Capacity.Provider provider = provider("reservation", "");

        assertThat(provider.usesWriteToken(), is(true));
        assertThat(provider.get(path("42"), null, null).getReservation(),
              is(Optional.of("42")));
    }

    @Test
    public void shouldReportWritePoolsOfPathWithoutWriteToken() throws Exception {
        givenPools(pool("pool1", 1000, 100, 50));
        givenWritePools("pool1");

        Capacity capacity = provider("reservation", "").get(path(null), "192.0.2.1", "Xrootd/5");

        assertThat(capacity.getTotalSpace(), is(1000L));
        assertThat(capacity.getReservation(), is(Optional.empty()));
    }

    @Test
    public void shouldRecoverWhenReservationReappears() throws Exception {
        Capacity.Provider provider = provider("reservation", "42");
        space = Optional.empty();
        assertThrows(CacheException.class, () -> provider.get());
        assertThrows(CacheException.class, () -> provider.get());

        space = Optional.of(space(42, 100, 80, 50));

        assertThat(provider.get().getFreeSpace(), is(0L));
    }

    @Test
    public void shouldRejectInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> provider("1234", ""));
        assertThrows(IllegalArgumentException.class, () -> provider("reservation", "CMS_DISK"));
    }

    private Capacity.Provider provider(String source, String reservation) {
        // Not cached, so that tests can change the space reservation.
        return new Capacity.Provider(source, reservation, poolMonitor,
              CacheBuilder.newBuilder().maximumSize(0).build(CacheLoader.from(token -> space)));
    }

    private void givenPools(PoolCostInfo... pools) {
        CostModule costModule = mock(CostModule.class);
        when(costModule.getPoolCostInfos()).thenReturn(List.of(pools));
        for (PoolCostInfo pool : pools) {
            when(costModule.getPoolCostInfo(pool.getPoolName())).thenReturn(pool);
        }
        when(poolMonitor.getCostModule()).thenReturn(costModule);
    }

    private void givenWritePools(String... pools) {
        PoolPreferenceLevel[] levels = levels(pools);
        PoolSelectionUnit psu = mock(PoolSelectionUnit.class);
        when(psu.match(eq(DirectionType.WRITE), eq("192.0.2.1"), eq("Xrootd/5"), any(), isNull(),
              any())).thenReturn(levels);
        when(poolMonitor.getPoolSelectionUnit()).thenReturn(psu);
    }

    /**
     * Pools written to only in the given link group.
     */
    private void givenLinkGroupWritePools(String linkGroup, String... pools) {
        PoolPreferenceLevel[] levels = levels(pools);
        SelectionLinkGroup group = mock(SelectionLinkGroup.class);
        PoolSelectionUnit psu = mock(PoolSelectionUnit.class);
        when(psu.getLinkGroups()).thenReturn(Map.of(linkGroup, group));
        when(psu.match(eq(DirectionType.WRITE), eq("192.0.2.1"), eq("Xrootd/5"), any(), any(),
              any())).thenReturn(new PoolPreferenceLevel[0]);
        when(psu.match(eq(DirectionType.WRITE), eq("192.0.2.1"), eq("Xrootd/5"), any(),
              eq(linkGroup), any())).thenReturn(levels);
        when(poolMonitor.getPoolSelectionUnit()).thenReturn(psu);
    }

    private static PoolPreferenceLevel[] levels(String... pools) {
        PoolPreferenceLevel level = mock(PoolPreferenceLevel.class);
        when(level.getPoolList()).thenReturn(List.of(pools));
        return pools.length == 0 ? new PoolPreferenceLevel[0] : new PoolPreferenceLevel[]{level};
    }

    private CellStub spaceManager(long linkGroupId, String name) throws Exception {
        LinkGroup group = new LinkGroup();
        group.setId(linkGroupId);
        group.setName(name);
        GetLinkGroupsMessage reply = new GetLinkGroupsMessage();
        reply.setLinkGroups(List.of(group));
        CellStub stub = mock(CellStub.class);
        when(stub.sendAndWait(any(GetLinkGroupsMessage.class))).thenReturn(reply);
        return stub;
    }

    private static FileAttributes path(String writeToken) {
        GenericStorageInfo storageInfo = new GenericStorageInfo();
        if (writeToken != null) {
            storageInfo.setKey("writeToken", writeToken);
        }
        FileAttributes attributes = new FileAttributes();
        attributes.setStorageInfo(storageInfo);
        return attributes;
    }

    private static PoolCostInfo pool(String name, long total, long free, long removable) {
        PoolCostInfo info = new PoolCostInfo(name, "regular");
        info.setSpaceUsage(total, free, 0, removable);
        return info;
    }

    static Space space(long id, long size, long used, long allocated) {
        return new Space(id, null, null, RetentionPolicy.REPLICA, AccessLatency.ONLINE, id, size,
              0, null, "test", SpaceState.RESERVED, used, allocated);
    }
}
