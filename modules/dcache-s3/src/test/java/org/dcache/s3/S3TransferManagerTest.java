package org.dcache.s3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.Pool;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileType;
import org.dcache.pool.assumption.Assumptions;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class S3TransferManagerTest {

    private static final FsPath PATH = FsPath.create("/data/s3/bucket/file");
    private static final PnfsId PNFSID = new PnfsId("0000A1B2C3D4E5F60718293A4B5C6D7E8F90");
    private static final byte[] DATA = "hello world".getBytes(UTF_8);

    private final List<String> ranges = new CopyOnWriteArrayList<>();
    private final List<DoorRequestInfoMessage> billing = new CopyOnWriteArrayList<>();
    private final List<PoolMoverKillMessage> kills = new CopyOnWriteArrayList<>();

    private HttpServer pool;
    private int poolStatus = 200;
    private boolean sendsRedirect = true;
    private boolean reportsCompletion = true;
    private volatile PoolDeliverFileMessage mover;
    private CacheException pnfsError;
    private CacheException poolManagerError;

    private FileAttributes attributes;
    private S3User user;
    private S3TransferManager manager;

    @Before
    public void setup() throws Exception {
        attributes = FileAttributes.of()
              .pnfsId(PNFSID)
              .fileType(FileType.REGULAR)
              .size(DATA.length)
              .creationTime(1)
              .storageInfo(new GenericStorageInfo("osm", "test:disk"))
              .storageClass("test:disk")
              .hsm("osm")
              .cacheClass(null)
              .locations(List.of("pool1"))
              .accessLatency(diskCacheV111.util.AccessLatency.ONLINE)
              .retentionPolicy(diskCacheV111.util.RetentionPolicy.REPLICA)
              .checksum(new Checksum(ChecksumType.ADLER32, "0b9e0306"))
              .build();
        attributes.getStorageInfo().setIsNew(false);

        Subject subject = new Subject();
        subject.getPrincipals().add(new UidPrincipal(1000));
        user = new S3User("AKIAEXAMPLE", subject, Restrictions.none(),
              FsPath.create("/data/s3"));

        manager = new S3TransferManager();
        manager.setCellAddress(new CellAddressCore("S3-door", "s3Domain"));
        manager.setInternalAddress(InetAddress.getLoopbackAddress());
        manager.setMoverTimeout(2, TimeUnit.SECONDS);
        manager.setPoolTimeout(5, TimeUnit.SECONDS);
        manager.setTransferConfirmationTimeout(2, TimeUnit.SECONDS);
        manager.setKillTimeout(0, TimeUnit.SECONDS);
        manager.setPnfsHandler(new PnfsHandler(pnfsStub()));
        manager.setPoolManagerStub(poolManagerStub());
        manager.setPoolStub(poolStub());
        manager.setBillingStub(billingStub());

        pool = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        pool.createContext("/", this::serve);
        pool.start();
    }

    @After
    public void tearDown() {
        pool.stop(0);
    }

    @Test
    public void shouldReadWholeFile() throws Exception {
        try (InputStream in = read(0, DATA.length)) {
            assertThat(new String(in.readAllBytes(), UTF_8), is("hello world"));
        }

        assertThat(ranges, equalTo(List.of("")));
        assertThat(kills, is(empty()));
        assertThat(billing, hasSize(1));
        assertThat(billing.get(0).getResultCode(), is(0));
        assertThat(billing.get(0).getClient(), is("192.0.2.1"));
    }

    @Test
    public void shouldReadRange() throws Exception {
        try (InputStream in = read(2, 4)) {
            assertThat(new String(in.readAllBytes(), UTF_8), is("llo "));
        }

        assertThat(ranges, equalTo(List.of("bytes=2-5")));
        assertThat(billing.get(0).getResultCode(), is(0));
    }

    @Test
    public void shouldKillMoverWhenClientStopsReading() throws Exception {
        reportsCompletion = false;

        try (InputStream in = read(0, DATA.length)) {
            in.read(new byte[3]);
        }

        assertThat(kills, hasSize(1));
        assertThat(billing, hasSize(1));
        assertThat(billing.get(0).getResultCode(), is(not(0)));
        assertThat(billing.get(0).getMessage(), containsString("Client disconnected"));
    }

    @Test
    public void shouldFailWhenPoolRefuses() {
        poolStatus = 500;

        CacheException e = assertThrows(CacheException.class,
              () -> read(0, DATA.length));

        assertThat(e.getRc(), is(CacheException.SERVICE_UNAVAILABLE));
        assertThat(kills, hasSize(1));
        assertThat(billing.get(0).getResultCode(), is(CacheException.SERVICE_UNAVAILABLE));
    }

    @Test
    public void shouldTimeOutWithoutMoverUrl() {
        sendsRedirect = false;

        assertThrows(TimeoutCacheException.class,
              () -> read(0, DATA.length));

        assertThat(kills, hasSize(1));
        assertThat(billing.get(0).getResultCode(), is(CacheException.TIMEOUT));
    }

    @Test
    public void shouldFailWhenNoPoolIsSelected() {
        poolManagerError = new CacheException(CacheException.NO_POOL_ONLINE, "No pool online");

        CacheException e = assertThrows(CacheException.class,
              () -> read(0, DATA.length));

        assertThat(e.getRc(), is(CacheException.NO_POOL_ONLINE));
        assertThat(kills, is(empty()));
        assertThat(billing.get(0).getResultCode(), is(CacheException.NO_POOL_ONLINE));
    }

    @Test
    public void shouldFailWhenReadIsDenied() {
        pnfsError = new PermissionDeniedCacheException("Permission denied");

        assertThrows(PermissionDeniedCacheException.class,
              () -> read(0, DATA.length));

        assertThat(billing.get(0).getResultCode(), is(CacheException.PERMISSION_DENIED));
    }

    private InputStream read(long offset, long length) throws Exception {
        try (var ignored = RequestOrigin.bind(InetAddress.getByName("192.0.2.1"))) {
            return manager.read(user, PATH, attributes, offset, length);
        }
    }

    // The fake cells

    private CellStub pnfsStub() {
        CellStub stub = mock(CellStub.class);
        when(stub.send(any(PnfsGetFileAttributes.class), anyLong())).thenAnswer(i -> {
            PnfsGetFileAttributes msg = i.getArgument(0);
            if (pnfsError != null) {
                msg.setFailed(pnfsError.getRc(), pnfsError.getMessage());
            } else {
                msg.setFileAttributes(attributes);
            }
            return Futures.immediateFuture(msg);
        });
        return stub;
    }

    private PoolManagerStub poolManagerStub() {
        PoolManagerStub stub = mock(PoolManagerStub.class);
        when(stub.sendAsync(any(PoolMgrSelectReadPoolMsg.class), anyLong())).thenAnswer(i -> {
            PoolMgrSelectReadPoolMsg msg = i.getArgument(0);
            if (poolManagerError != null) {
                msg.setFailed(poolManagerError.getRc(), poolManagerError.getMessage());
            } else {
                msg.setPool(new Pool("pool1", new CellAddressCore("pool1", "poolDomain"),
                      Assumptions.none()));
            }
            return Futures.immediateFuture(msg);
        });
        return stub;
    }

    /**
     * Starts movers by announcing the fake pool's URL to the door, and ends them when killed.
     */
    private CellStub poolStub() {
        CellStub stub = mock(CellStub.class);
        when(stub.send(any(CellPath.class), any(PoolDeliverFileMessage.class), anyLong()))
              .thenAnswer(i -> {
                  PoolDeliverFileMessage msg = i.getArgument(1);
                  msg.setMoverId(42);
                  mover = msg;
                  if (sendsRedirect) {
                      var protocolInfo = (HttpProtocolInfo) msg.getProtocolInfo();
                      assertThat(protocolInfo.getSocketAddress().getAddress(),
                            is(InetAddress.getLoopbackAddress()));
                      var redirect = new HttpDoorUrlInfoMessage(PNFSID.toString(),
                            "http://127.0.0.1:" + pool.getAddress().getPort()
                                  + protocolInfo.getPath() + "?dcache-http-uuid=x");
                      redirect.setId(msg.getId());
                      manager.messageArrived(redirect);
                  }
                  return Futures.immediateFuture(msg);
              });
        doAnswer(i -> {
            PoolMoverKillMessage kill = i.getArgument(1);
            kills.add(kill);
            finishMover(CacheException.DEFAULT_ERROR_CODE, "killed");
            return null;
        }).when(stub).notify(any(CellPath.class), any(PoolMoverKillMessage.class));
        return stub;
    }

    private CellStub billingStub() {
        CellStub stub = mock(CellStub.class);
        doAnswer(i -> {
            billing.add(i.getArgument(0));
            return null;
        }).when(stub).notify(any(DoorRequestInfoMessage.class));
        return stub;
    }

    /**
     * The fake mover: serves the file, honouring a Range header, and reports the end of the
     * transfer to the door.
     */
    private void serve(HttpExchange exchange) throws IOException {
        String range = exchange.getRequestHeaders().getFirst("Range");
        ranges.add(range == null ? "" : range);
        int offset = 0;
        int end = DATA.length;
        int status = poolStatus;
        if (status == 200 && range != null) {
            String[] bounds = range.substring("bytes=".length()).split("-");
            offset = Integer.parseInt(bounds[0]);
            end = Integer.parseInt(bounds[1]) + 1;
            status = 206;
        }
        try (OutputStream out = exchange.getResponseBody()) {
            if (status >= 300) {
                exchange.sendResponseHeaders(status, -1);
            } else {
                exchange.sendResponseHeaders(status, end - offset);
                out.write(DATA, offset, end - offset);
            }
        } catch (IOException e) {
            // The door closed the connection early.
        }
        if (status < 300 && reportsCompletion) {
            finishMover(0, "");
        }
    }

    private void finishMover(int rc, String error) {
        PoolDeliverFileMessage msg = mover;
        if (msg == null) {
            return;
        }
        var finished = new DoorTransferFinishedMessage(msg.getId(), PNFSID,
              msg.getProtocolInfo(), attributes, "pool1", null);
        if (rc != 0) {
            finished.setFailed(rc, error);
        }
        manager.messageArrived(finished);
    }
}
