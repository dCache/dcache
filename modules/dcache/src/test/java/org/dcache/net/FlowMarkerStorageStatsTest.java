package org.dcache.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.MoverInfoMessage;
import dmg.cells.nucleus.CellAddressCore;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import org.dcache.net.FlowMarker.FlowMarkerBuilder;
import org.json.JSONObject;
import org.junit.Test;

public class FlowMarkerStorageStatsTest {

    private static final PnfsId PNFSID = new PnfsId("000000000000000000000000000000000001");

    @Test
    public void shouldDescribeSuccessfulUpload() {
        var info = new MoverInfoMessage(new CellAddressCore("pool_a", "poolDomain"), PNFSID);
        info.setFileCreated(true);
        info.setFileSize(4096);
        info.setResult(0, "");
        info.setTimeQueued(250);
        info.setTransferAttributes(4096, 1500, null);

        JSONObject stats = storageStats(info);

        assertEquals(0, stats.getInt("result-code"));
        assertEquals(4096L, stats.getLong("file-size"));
        assertEquals("write", stats.getString("io-mode"));
        assertEquals(250L, stats.getLong("queue-time"));
        assertEquals("pool_a", stats.getString("pool-name"));
        assertEquals(4096L, stats.getLong("bytes-transferred"));
        assertEquals(1500L, stats.getLong("connection-time"));
    }

    @Test
    public void shouldDescribeFailedDownload() {
        var info = new MoverInfoMessage(new CellAddressCore("pool_b", "poolDomain"), PNFSID);
        info.setFileCreated(false);
        info.setFileSize(8192);
        info.setResult(CacheException.DEFAULT_ERROR_CODE, "General problem");
        info.setTransferAttributes(1024, 300, null);

        JSONObject stats = storageStats(info);

        assertEquals(CacheException.DEFAULT_ERROR_CODE, stats.getInt("result-code"));
        assertEquals(8192L, stats.getLong("file-size"));
        assertEquals("read", stats.getString("io-mode"));
        assertEquals(0L, stats.getLong("queue-time"));
        assertEquals("pool_b", stats.getString("pool-name"));
        assertEquals(1024L, stats.getLong("bytes-transferred"));
    }

    @Test
    public void shouldOmitStorageStatsUnlessRequested() {
        String firefly = builder().build("end");

        assertFalse(payload(firefly).has("storage-stats"));
    }

    private static JSONObject storageStats(MoverInfoMessage info) {
        return payload(builder().withStats(info).build("end")).getJSONObject("storage-stats");
    }

    private static FlowMarkerBuilder builder() {
        return new FlowMarkerBuilder()
              .withStartedAt(Instant.now())
              .withFinishedAt(Instant.now())
              .withExperimentId(2)
              .withActivityId(1)
              .wittApplication("https")
              .withProtocol("tcp")
              .withAFI("ipv4")
              .withDestination(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234))
              .withSource(new InetSocketAddress(InetAddress.getLoopbackAddress(), 5678));
    }

    private static JSONObject payload(String firefly) {
        return new JSONObject(firefly.substring(firefly.indexOf("{")));
    }
}
