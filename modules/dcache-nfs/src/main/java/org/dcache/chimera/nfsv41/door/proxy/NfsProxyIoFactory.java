package org.dcache.chimera.nfsv41.door.proxy;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import diskCacheV111.util.CacheException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.status.DelayException;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.Layout;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.NFSv41DeviceManager;
import org.dcache.nfs.v4.StateDisposeListener;
import org.dcache.nfs.v4.client.GetDeviceListStub;
import org.dcache.nfs.v4.client.LayoutgetStub;
import org.dcache.nfs.v4.xdr.device_addr4;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.dcache.nfs.v4.xdr.layoutiomode4;
import org.dcache.nfs.v4.xdr.layouttype4;
import org.dcache.nfs.v4.xdr.netaddr4;
import org.dcache.nfs.v4.xdr.nfsv4_1_file_layout4;
import org.dcache.nfs.v4.xdr.nfsv4_1_file_layout_ds_addr4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.utils.net.InetSocketAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NfsProxyIoFactory implements ProxyIoFactory {

    private static final Logger _log = LoggerFactory.getLogger(NfsProxyIoFactory.class);

    private final Cache<stateid4, ProxyIoAdapter> _proxyIO
            = CacheBuilder.newBuilder()
            .build();

    private final NFSv41DeviceManager deviceManager;

    public NfsProxyIoFactory(NFSv41DeviceManager deviceManager) {
        this.deviceManager = deviceManager;
    }


    @Override
    public ProxyIoAdapter getOrCreateProxy(Inode inode, stateid4 stateid, CompoundContext context, boolean isWrite) throws IOException {
        try {
            ProxyIoAdapter adapter = _proxyIO.get(stateid,
                    new Callable<ProxyIoAdapter>() {

                        @Override
                        public ProxyIoAdapter call() throws Exception {

                            final NFS4Client nfsClient;
                            if (context.getMinorversion() == 1) {
                                nfsClient = context.getSession().getClient();
                            } else {
                                nfsClient = context.getStateHandler().getClientIdByStateId(stateid);
                            }

                            final NFS4State state = nfsClient.state(stateid);
                            final ProxyIoAdapter adapter = createIoAdapter(inode, stateid, context, isWrite);

                            state.addDisposeListener(new StateDisposeListener() {
                                @Override
                                public void notifyDisposed(NFS4State state) {
                                    tryToClose(adapter);
                                    _proxyIO.invalidate(state.stateid());
                                }
                            });

                            return adapter;
                        }
                    });

            return adapter;
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            _log.error("failed to create IO adapter: {}", t.getMessage());
            if (t instanceof ChimeraNFSException) {
                throw (ChimeraNFSException) t;
            }

            if ((t instanceof CacheException) && ((CacheException) t).getRc() != CacheException.BROKEN_ON_TAPE) {
                throw new DelayException();
            }
            throw new NfsIoException();
        }
    }

    @Override
    public ProxyIoAdapter createIoAdapter (Inode inode, stateid4 stateid, CompoundContext context, boolean isWrite) throws IOException {
        _log.info("creating new proxy-io adapter for {} {}", inode, context.getRemoteSocketAddress().getAddress().getHostAddress());
        Layout layout = deviceManager.layoutGet(context, inode,
                layouttype4.LAYOUT4_NFSV4_1_FILES,
                isWrite ? layoutiomode4.LAYOUTIOMODE4_RW : layoutiomode4.LAYOUTIOMODE4_READ, stateid);

        // we assume only one segment as dcache doesn't support striping
        nfsv4_1_file_layout4 fileLayoutSegment = LayoutgetStub.decodeLayoutId(layout.getLayoutSegments()[0].lo_content.loc_body);
        deviceid4 dsId = fileLayoutSegment.nfl_deviceid;
        device_addr4 deviceAddr = deviceManager.getDeviceInfo(context, dsId);
        nfsv4_1_file_layout_ds_addr4 nfs4DeviceAddr = GetDeviceListStub.decodeFileDevice(deviceAddr.da_addr_body);
        // we assume that device points only to one server
        for(netaddr4 na: nfs4DeviceAddr.nflda_multipath_ds_list[0].value) {
            if (na.na_r_netid.equals("tcp") || na.na_r_netid.equals("tcp6") ) {
                InetSocketAddress poolAddress = InetSocketAddresses.forUaddrString(na.na_r_addr);
                if (poolAddress.getAddress().isReachable(100) ) {
                    return new NfsProxyIo(poolAddress, context.getRemoteSocketAddress(), inode, stateid, 0);
                }
            }
        }
        throw new NfsIoException("can't connect to pool");
    }

    private static void tryToClose(ProxyIoAdapter adapter) {
        try {
            adapter.close();
        } catch (IOException e) {
            _log.error("failed to close io adapter: ", e.getMessage());
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
