package org.dcache.chimera.nfsv41.door.proxy;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import diskCacheV111.util.CacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.StateDisposeListener;
import org.dcache.nfs.v4.xdr.READ4res;
import org.dcache.nfs.v4.xdr.READ4resok;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.xdr.RpcCall;

public class ProxyIoREAD extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(ProxyIoREAD.class.getName());
    private final DcapProxyIoFactory proxyIoFactory;

    public ProxyIoREAD(nfs_argop4 args, DcapProxyIoFactory proxyIoFactory) {
        super(args, nfs_opnum4.OP_READ);
        this.proxyIoFactory = proxyIoFactory;
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result) {
        final READ4res res = result.opread;

        try {

            long offset = _args.opread.offset.value.value;
            int count = _args.opread.count.value.value;
            stateid4 stateid = _args.opread.stateid;

            ProxyIoAdapter proxyIoAdapter = getOrCreateProxy(context.currentInode(), stateid, context);
            ByteBuffer bb = ByteBuffer.allocate(count);
            int bytesReaded = proxyIoAdapter.read(bb, offset);

            res.status = nfsstat.NFS_OK;
            res.resok4 = new READ4resok();
            res.resok4.data = bb;

            if( offset + bytesReaded == proxyIoAdapter.size() ) {
                res.resok4.eof = true;
            }

            _log.debug("MOVER: {}@{} readed, {} requested.", bytesReaded, offset, _args.opread.count.value.value);

        }catch(ChimeraNFSException he) {
            res.status = he.getStatus();
            _log.debug(he.getMessage());
        }catch(IOException ioe) {
            _log.error("DSREAD: ", ioe);
            res.status = nfsstat.NFSERR_IO;
        }catch(Exception e) {
            _log.error("DSREAD: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        }
    }

    private ProxyIoAdapter getOrCreateProxy(final Inode inode, final stateid4 stateid, final CompoundContext context) throws ChimeraNFSException {

        try {
            ProxyIoAdapter adapter = _prioxyIO.get(stateid,
                    new Callable<ProxyIoAdapter>() {

                        @Override
                        public ProxyIoAdapter call() throws Exception {
                            final RpcCall call = context.getRpcCall();
                            final NFS4State state = context.getStateHandler().getClientIdByStateId(stateid).state(stateid);

                            final ProxyIoAdapter adapter = proxyIoFactory.getAdapter(inode, call.getCredential().getSubject(),
                                    call.getTransport().getRemoteSocketAddress());

                            state.addDisposeListener( new StateDisposeListener() {

                                @Override
                                public void notifyDisposed(NFS4State state) {
                                    try {
                                        adapter.close();
                                    }catch (IOException e) {
                                        _log.error("failed fo close io adapter: ", e.getMessage());
                                    }
                                    _prioxyIO.invalidate(state.stateid());
                                }
                            });

                            return adapter;
                        }
                    });

            return adapter;
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            _log.error("failed to create IO adapter: ", t.getMessage());
            if (t instanceof ChimeraNFSException) {
                throw (ChimeraNFSException)t;
            }
            int status = nfsstat.NFSERR_IO;
            if (t instanceof CacheException ) {
                status = nfsstat.NFSERR_DELAY;
            }
            throw new ChimeraNFSException(status, t.getMessage());
        }
    }

    private static final Cache<stateid4, ProxyIoAdapter> _prioxyIO=
            CacheBuilder.newBuilder()
            .build();

}
