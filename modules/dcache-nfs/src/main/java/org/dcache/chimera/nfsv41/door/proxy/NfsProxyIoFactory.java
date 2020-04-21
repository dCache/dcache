package org.dcache.chimera.nfsv41.door.proxy;

import com.google.common.base.Stopwatch;
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

import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.Layout;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.NFSv41DeviceManager;
import org.dcache.nfs.v4.client.GetDeviceListStub;
import org.dcache.nfs.v4.client.LayoutgetStub;
import org.dcache.nfs.v4.xdr.GETDEVICEINFO4args;
import org.dcache.nfs.v4.xdr.LAYOUTGET4args;
import org.dcache.nfs.v4.xdr.LAYOUTRETURN4args;
import org.dcache.nfs.v4.xdr.bitmap4;
import org.dcache.nfs.v4.xdr.count4;
import org.dcache.nfs.v4.xdr.device_addr4;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.dcache.nfs.v4.xdr.layoutreturn4;
import org.dcache.nfs.v4.xdr.layoutreturn_file4;
import org.dcache.nfs.v4.xdr.layoutreturn_type4;
import org.dcache.nfs.v4.xdr.layoutiomode4;
import org.dcache.nfs.v4.xdr.layouttype4;
import org.dcache.nfs.v4.xdr.length4;
import org.dcache.nfs.v4.xdr.netaddr4;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfsv4_1_file_layout4;
import org.dcache.nfs.v4.xdr.nfsv4_1_file_layout_ds_addr4;
import org.dcache.nfs.v4.xdr.offset4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.util.backoff.ExponentialBackoffAlgorithmFactory;
import org.dcache.util.backoff.IBackoffAlgorithm;
import org.dcache.oncrpc4j.rpc.net.InetSocketAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dcache.chimera.nfsv41.door.ExceptionUtils.asNfsException;

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

    public NfsProxyIoFactory(NFSv41DeviceManager deviceManager) {
        this.deviceManager = deviceManager;
        backoffFactory = new ExponentialBackoffAlgorithmFactory();
        backoffFactory.setMinDelay(TIMEOUT_STEP);
        backoffFactory.setMinUnit(TIMEOUT_STEP_UNIT);
    }


    @Override
    public ProxyIoAdapter getOrCreateProxy(Inode inode, stateid4 stateid, CompoundContext context, boolean isWrite) throws IOException {
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
                                    final ProxyIoAdapter adapter = createIoAdapter(inode, stateid, context, isWrite);

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
    public ProxyIoAdapter createIoAdapter (Inode inode, stateid4 stateid, CompoundContext context, boolean isWrite) throws IOException {
        _log.info("creating new proxy-io adapter for {} {}", inode, context.getRemoteSocketAddress().getAddress().getHostAddress());

        LAYOUTGET4args lgArgs = new LAYOUTGET4args();
        lgArgs.loga_iomode = isWrite ? layoutiomode4.LAYOUTIOMODE4_RW : layoutiomode4.LAYOUTIOMODE4_READ;
        lgArgs.loga_layout_type = layouttype4.LAYOUT4_NFSV4_1_FILES.getValue();
        lgArgs.loga_length = new length4(nfs4_prot.NFS4_UINT64_MAX); // EOF
        lgArgs.loga_maxcount = new count4(1); // one layout segment
        lgArgs.loga_minlength = new length4(0);
        lgArgs.loga_offset = new offset4(0);
        lgArgs.loga_signal_layout_avail = false;
        lgArgs.loga_stateid = stateid;

        Layout layout = deviceManager.layoutGet(context, lgArgs);

        // we assume only one segment as dcache doesn't support striping
        nfsv4_1_file_layout4 fileLayoutSegment = LayoutgetStub.decodeLayoutId(layout.getLayoutSegments()[0].lo_content.loc_body);
        deviceid4 dsId = fileLayoutSegment.nfl_deviceid;

        GETDEVICEINFO4args gdiArgs = new GETDEVICEINFO4args();
        gdiArgs.gdia_device_id = dsId;
        gdiArgs.gdia_layout_type = layouttype4.LAYOUT4_NFSV4_1_FILES.getValue();
        gdiArgs.gdia_maxcount = new count4(1);
        gdiArgs.gdia_notify_types = new bitmap4();

        device_addr4 deviceAddr = deviceManager.getDeviceInfo(context, gdiArgs);
        nfsv4_1_file_layout_ds_addr4 nfs4DeviceAddr = GetDeviceListStub.decodeFileDevice(deviceAddr.da_addr_body);

        Stopwatch connectStopwatch = Stopwatch.createStarted();
        IBackoffAlgorithm backoff = backoffFactory.getAlgorithm();

retry:  while (true) {
            long timeout = backoff.getWaitDuration();

            if (timeout == IBackoffAlgorithm.NO_WAIT) {
                break;
            }

            // we assume that only device points only to one server
            for (netaddr4 na : nfs4DeviceAddr.nflda_multipath_ds_list[0].value) {
                if (connectStopwatch.elapsed(MAX_CONNECT_TIMEOUT_UNIT) > MAX_CONNECT_TIMEOUT) {
                    break retry;
                }

                if (na.na_r_netid.equals("tcp") || na.na_r_netid.equals("tcp6")) {
                    InetSocketAddress poolSocketAddress = InetSocketAddresses.forUaddrString(na.na_r_addr);
                    InetAddress address = poolSocketAddress.getAddress();
                    if (!isHostLocal(address)) {
                        try {
                            return new NfsProxyIo(poolSocketAddress, context.getRemoteSocketAddress(), inode, stateid, timeout, TIMEOUT_STEP_UNIT);
                        } catch (IOException e) {
                            _log.warn("Failed to connect to remote mover {} : {}", address, e.getMessage());
                        }
                    }
                 }
             }

            _log.warn("Failed to connect to pool {} within {}, Retrying...", toString(nfs4DeviceAddr.nflda_multipath_ds_list[0].value), connectStopwatch);
         }

        _log.error("Failed to connect to pool {} within {}, Giving up!", toString(nfs4DeviceAddr.nflda_multipath_ds_list[0].value), connectStopwatch);

        LAYOUTRETURN4args lrArgs = new LAYOUTRETURN4args();
        lrArgs.lora_iomode = layoutiomode4.LAYOUTIOMODE4_ANY;
        lrArgs.lora_layout_type = layouttype4.LAYOUT4_NFSV4_1_FILES.getValue();
        lrArgs.lora_layoutreturn = new layoutreturn4();
        lrArgs.lora_layoutreturn.lr_returntype = layoutreturn_type4.LAYOUTRETURN4_FILE;
        lrArgs.lora_layoutreturn.lr_layout = new layoutreturn_file4();
        lrArgs.lora_layoutreturn.lr_layout.lrf_stateid = stateid;
        lrArgs.lora_layoutreturn.lr_layout.lrf_length = new length4(nfs4_prot.NFS4_UINT64_MAX);
        lrArgs.lora_layoutreturn.lr_layout.lrf_offset = new offset4(0);
        lrArgs.lora_layoutreturn.lr_layout.lrf_body = new byte[0];
        lrArgs.lora_reclaim = false;

        deviceManager.layoutReturn(context, lrArgs);
        context.getStateHandler().getClientIdByStateId(stateid).releaseState(layout.getStateid());
        throw new NfsIoException("can't connect to pool");
    }

    private static boolean isHostLocal(InetAddress address) {
        return address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress();
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
        return (int)_proxyIO.size();
    }
}
