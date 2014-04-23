package org.dcache.chimera.nfsv41.door.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import dmg.cells.nucleus.CDC;

import org.dcache.commons.util.NDC;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.OperationREAD;
import org.dcache.nfs.v4.Stateids;
import org.dcache.nfs.v4.xdr.READ4res;
import org.dcache.nfs.v4.xdr.READ4resok;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;

public class ProxyIoREAD extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(ProxyIoREAD.class);
    private final DcapProxyIoFactory proxyIoFactory;

    public ProxyIoREAD(nfs_argop4 args, DcapProxyIoFactory proxyIoFactory) {
        super(args, nfs_opnum4.OP_READ);
        this.proxyIoFactory = proxyIoFactory;
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result) {
        final READ4res res = result.opread;

        CDC cdc = CDC.reset(proxyIoFactory.getCellName(), proxyIoFactory.getCellDomainName());
        try {
	    NDC.push(context.getRpcCall().getTransport().getRemoteSocketAddress().toString());
            Inode inode = context.currentInode();
            if (!context.getFs().hasIOLayout(inode)) {
                /*
                 * if we have a special file, then fall back to regular read operation
                 */
                new OperationREAD(_args).process(context, result);
                return;
            }

            long offset = _args.opread.offset.value;
            int count = _args.opread.count.value;
            stateid4 stateid = _args.opread.stateid;

	    int bytesReaded;
	    ProxyIoAdapter proxyIoAdapter;
	    ByteBuffer bb = ByteBuffer.allocate(count);
	    if (Stateids.isStateLess(stateid)) {

		/*
		 * As there was no open, we have to check  permissions.
		 */
		if (context.getFs().access(inode, nfs4_prot.ACCESS4_READ) == 0) {
		    throw new ChimeraNFSException(nfsstat.NFSERR_ACCESS, "Permission denied.");
		}

		/*
		 * use try-with-resource as wee need to close adapter on each request
		 */
		try (ProxyIoAdapter oneUseProxyIoAdapter = proxyIoFactory.createIoAdapter(inode, context, false)) {
		    proxyIoAdapter = oneUseProxyIoAdapter;
		    bytesReaded = oneUseProxyIoAdapter.read(bb, offset);
		}
	    } else {
                if (context.getMinorversion() == 0) {
                    /*
                     *  The NFSv4.0 spec requires to update lease time as long as client
                     * needs the file. This is done through READ, WRITE and RENEW
                     * opertations. With introduction of sessions in v4.1 update of the
                     * lease time done through SEQUENCE operation.
                     */
                    context.getStateHandler().updateClientLeaseTime(stateid);
                }
		proxyIoAdapter = proxyIoFactory.getOrCreateProxy(inode, stateid, context, false);
		bytesReaded = proxyIoAdapter.read(bb, offset);
	    }

            res.status = nfsstat.NFS_OK;
            res.resok4 = new READ4resok();
            res.resok4.data = bb;

            if( offset + bytesReaded == proxyIoAdapter.size() ) {
                res.resok4.eof = true;
            }

            _log.debug("MOVER: {}@{} readed, {} requested.", bytesReaded, offset, _args.opread.count.value);

        }catch(ChimeraNFSException he) {
            res.status = he.getStatus();
            _log.debug(he.getMessage());
        }catch(IOException ioe) {
            _log.error("DSREAD: ", ioe);
            res.status = nfsstat.NFSERR_IO;
        }catch(Exception e) {
            _log.error("DSREAD: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        } finally {
            cdc.close();
        }
    }
}
