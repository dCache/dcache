package org.dcache.chimera.nfsv41.door.proxy;

import static org.dcache.chimera.nfsv41.door.ExceptionUtils.asNfsException;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.Layout;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.NFSv41DeviceManager;
import org.dcache.nfs.v4.ff.ff_device_addr4;
import org.dcache.nfs.v4.ff.ff_ioerr4;
import org.dcache.nfs.v4.ff.ff_iostats4;
import org.dcache.nfs.v4.ff.ff_layout4;
import org.dcache.nfs.v4.ff.ff_layoutreturn4;
import org.dcache.nfs.v4.xdr.GETDEVICEINFO4args;
import org.dcache.nfs.v4.xdr.LAYOUTGET4args;
import org.dcache.nfs.v4.xdr.LAYOUTRETURN4args;
import org.dcache.nfs.v4.xdr.bitmap4;
import org.dcache.nfs.v4.xdr.count4;
import org.dcache.nfs.v4.xdr.device_addr4;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.dcache.nfs.v4.xdr.layoutiomode4;
import org.dcache.nfs.v4.xdr.layoutreturn4;
import org.dcache.nfs.v4.xdr.layoutreturn_file4;
import org.dcache.nfs.v4.xdr.layoutreturn_type4;
import org.dcache.nfs.v4.xdr.layouttype4;
import org.dcache.nfs.v4.xdr.length4;
import org.dcache.nfs.v4.xdr.netaddr4;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.offset4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.oncrpc4j.rpc.net.InetSocketAddresses;
import org.dcache.oncrpc4j.xdr.Xdr;
import org.dcache.oncrpc4j.xdr.XdrDecodingStream;
import org.dcache.util.backoff.ExponentialBackoffAlgorithmFactory;
import org.dcache.util.backoff.IBackoffAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NfsProxyIoFactory implements ProxyIoFactory {

    private static final TimeUnit MAX_CONNECT_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final long MAX_CONNECT_TIMEOUT = 15;

    private static final TimeUnit TIMEOUT_STEP_UNIT = TimeUnit.MILLISECONDS;
    private static final long TIMEOUT_STEP = 100;

    private static final Logger _log = LoggerFactory.getLogger(NfsProxyIoFactory.class);

    private final Cache<stateid4, ProxyIoAdapter> _proxyIO
          = CacheBuilder.newBuilder()
          .build();

    private final NFSv41DeviceManager deviceManager;
    private final ExponentialBackoffAlgorithmFactory backoffFactory;


    /**
     * Empty flex_files layout stats and errors used by layout return.
     */
    private static byte[] FF_EMPY_RETURN;
    static {
        try(Xdr xdr = new Xdr(512);) {
            ff_layoutreturn4 layoutReturnBody = new ff_layoutreturn4();
            layoutReturnBody.fflr_ioerr_report = new ff_ioerr4[0];
            layoutReturnBody.fflr_iostats_report = new ff_iostats4[0];

            xdr.beginEncoding();
            layoutReturnBody.xdrEncode(xdr);
            xdr.endEncoding();
            FF_EMPY_RETURN = xdr.getBytes();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize empty layout body", e);
        }
    }

    public NfsProxyIoFactory(NFSv41DeviceManager deviceManager) {
        this.deviceManager = deviceManager;
        backoffFactory = new ExponentialBackoffAlgorithmFactory();
        backoffFactory.setMinDelay(TIMEOUT_STEP);
        backoffFactory.setMinUnit(TIMEOUT_STEP_UNIT);
    }


    @Override
    public ProxyIoAdapter getOrCreateProxy(Inode inode, stateid4 stateid, CompoundContext context,
          boolean isWrite) throws IOException {
        try {
            return _proxyIO.get(stateid,
                  () -> {
                      final NFS4Client nfsClient;
                      if (context.getMinorversion() == 1) {
                          nfsClient = context.getSession().getClient();
                      } else {
                          nfsClient = context.getStateHandler().getClientIdByStateId(stateid);
                      }

                      final NFS4State state = nfsClient.state(stateid);
                      final ProxyIoAdapter adapter = createIoAdapter(inode, stateid, context,
                            isWrite);

                      state.addDisposeListener(s -> {
                          tryToClose(adapter);
                          _proxyIO.invalidate(s.stateid());
                      });

                      return adapter;
                  });
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            _log.debug("failed to create IO adapter: {}", t.getMessage());
            throw asNfsException(t, NfsIoException.class);
        }
    }

    @Override
    public ProxyIoAdapter createIoAdapter(Inode inode, stateid4 stateid, CompoundContext context,
          boolean isWrite) throws IOException {
        _log.info("creating new proxy-io adapter for {} {}", inode,
              context.getRemoteSocketAddress().getAddress().getHostAddress());

        // State to which the proxy will be bound.
        NFS4State state = context.getStateHandler().getClientIdByStateId(stateid).state(stateid).getOpenState();

        LAYOUTGET4args lgArgs = new LAYOUTGET4args();
        lgArgs.loga_iomode =
              isWrite ? layoutiomode4.LAYOUTIOMODE4_RW : layoutiomode4.LAYOUTIOMODE4_READ;
        lgArgs.loga_layout_type = layouttype4.LAYOUT4_FLEX_FILES.getValue();
        lgArgs.loga_length = new length4(nfs4_prot.NFS4_UINT64_MAX); // EOF
        lgArgs.loga_maxcount = new count4(1); // one layout segment
        lgArgs.loga_minlength = new length4(0);
        lgArgs.loga_offset = new offset4(0);
        lgArgs.loga_signal_layout_avail = false;
        lgArgs.loga_stateid = stateid;


        /*
         * 1. only flex_file layout is used
         * 2. the layout expected to consist ot of single segment (file completly on a single DS)
         * 3. the DS is a single mirror.
         */
        Layout layout = deviceManager.layoutGet(context, lgArgs);

        // we assume only one segment as dcache doesn't support striping
        ff_layout4 ffLayoutSegment = decodeLayoutId(
                layout.getLayoutSegments()[0].lo_content.loc_body);

        // we assume single mirror, so single device id.
        deviceid4 dsId = ffLayoutSegment.ffl_mirrors[0].ffm_data_servers[0].ffds_deviceid;

        GETDEVICEINFO4args gdiArgs = new GETDEVICEINFO4args();
        gdiArgs.gdia_device_id = dsId;
        gdiArgs.gdia_layout_type = layouttype4.LAYOUT4_FLEX_FILES.getValue();
        gdiArgs.gdia_maxcount = new count4(1);
        gdiArgs.gdia_notify_types = new bitmap4();

        device_addr4 deviceAddr = deviceManager.getDeviceInfo(context, gdiArgs);
        ff_device_addr4 nfs4DeviceAddr = decodeFileDevice(deviceAddr.da_addr_body);

        // prepare layour return args for later use
        LAYOUTRETURN4args lrArgs = new LAYOUTRETURN4args();
        lrArgs.lora_iomode = layoutiomode4.LAYOUTIOMODE4_ANY;
        lrArgs.lora_layout_type = layouttype4.LAYOUT4_FLEX_FILES.getValue();
        lrArgs.lora_layoutreturn = new layoutreturn4();
        lrArgs.lora_layoutreturn.lr_returntype = layoutreturn_type4.LAYOUTRETURN4_FILE;
        lrArgs.lora_layoutreturn.lr_layout = new layoutreturn_file4();
        lrArgs.lora_layoutreturn.lr_layout.lrf_stateid = layout.getStateid();
        lrArgs.lora_layoutreturn.lr_layout.lrf_length = new length4(nfs4_prot.NFS4_UINT64_MAX);
        lrArgs.lora_layoutreturn.lr_layout.lrf_offset = new offset4(0);
        lrArgs.lora_layoutreturn.lr_layout.lrf_body = FF_EMPY_RETURN;
        lrArgs.lora_reclaim = false;

        Stopwatch connectStopwatch = Stopwatch.createStarted();
        IBackoffAlgorithm backoff = backoffFactory.getAlgorithm();

        retry:
        while (true) {
            long timeout = backoff.getWaitDuration();

            if (timeout == IBackoffAlgorithm.NO_WAIT) {
                break;
            }

            // we assume that only device points only to one server
            for (netaddr4 na : nfs4DeviceAddr.ffda_netaddrs.value) {
                if (connectStopwatch.elapsed(MAX_CONNECT_TIMEOUT_UNIT) > MAX_CONNECT_TIMEOUT) {
                    break retry;
                }

                if (na.na_r_netid.equals("tcp") || na.na_r_netid.equals("tcp6")) {
                    InetSocketAddress poolSocketAddress = InetSocketAddresses.forUaddrString(
                          na.na_r_addr);
                    InetAddress address = poolSocketAddress.getAddress();
                    if (!isHostLocal(address)) {
                        try {
                            // return layout on close
                            state.addDisposeListener(s -> {
                                try {
                                    deviceManager.layoutReturn(context, lrArgs);
                                } catch (IOException e) {
                                    Throwables.propagateIfPossible(e, ChimeraNFSException.class);
                                    throw new NfsIoException("Failed to return proxy-layout", e);
                                }
                            });
                            return new NfsProxyIo(poolSocketAddress,
                                  context.getRemoteSocketAddress(), inode, ffLayoutSegment.ffl_mirrors[0].ffm_data_servers[0].ffds_stateid , timeout,
                                  TIMEOUT_STEP_UNIT);
                        } catch (IOException e) {
                            _log.warn("Failed to connect to remote mover {} : {}", address,
                                  e.getMessage());
                        }
                    }
                }
            }

            _log.warn("Failed to connect to pool {} within {}, Retrying...",
                  Arrays.toString(nfs4DeviceAddr.ffda_netaddrs.value), connectStopwatch);
        }

        _log.error("Failed to connect to pool {} within {}, Giving up!",
                Arrays.toString(nfs4DeviceAddr.ffda_netaddrs.value), connectStopwatch);

        deviceManager.layoutReturn(context, lrArgs);
        context.getStateHandler().getClientIdByStateId(stateid).releaseState(layout.getStateid());
        throw new NfsIoException("can't connect to pool");
    }

    private static boolean isHostLocal(InetAddress address) {
        return address.isAnyLocalAddress() || address.isLoopbackAddress()
              || address.isLinkLocalAddress();
    }

    // FIXME: move this into generic NFS code
    private static String toString(netaddr4[] netaddr) {
        return Arrays.stream(netaddr)
              .filter(n -> (n.na_r_netid.equals("tcp") || n.na_r_netid.equals("tcp6")))
              .map(n -> InetSocketAddresses.forUaddrString(n.na_r_addr))
              .map(a -> HostAndPort.fromParts(a.getHostString(), a.getPort()))
              .map(Object::toString)
              .collect(Collectors.joining(",", "[", "]"));
    }

    private static void tryToClose(ProxyIoAdapter adapter) {
        try {
            adapter.close();
        } catch (IOException e) {
            _log.error("failed to close io adapter: {}", e.getMessage());
        }
    }

    @Override
    public void shutdownAdapter(stateid4 stateid) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void forEach(Consumer<ProxyIoAdapter> action) {
        _proxyIO.asMap().values().forEach(action);
    }

    @Override
    public int getCount() {
        return (int) _proxyIO.size();
    }

    public static ff_layout4 decodeLayoutId(byte[] data) throws IOException {
        XdrDecodingStream xdr = new Xdr(data);
        xdr.beginDecoding();

        return new ff_layout4(xdr);
    }

    public static ff_device_addr4 decodeFileDevice(byte[] data) throws IOException {
        XdrDecodingStream xdr = new Xdr(data);
        xdr.beginDecoding();

        return new ff_device_addr4(xdr);
    }
}
