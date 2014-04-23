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
import org.dcache.nfs.v4.OperationWRITE;
import org.dcache.nfs.v4.Stateids;
import org.dcache.nfs.v4.xdr.WRITE4res;
import org.dcache.nfs.v4.xdr.WRITE4resok;
import org.dcache.nfs.v4.xdr.count4;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.xdr.stable_how4;;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.v4.xdr.verifier4;
import org.dcache.nfs.vfs.Inode;

public class ProxyIoWRITE extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(ProxyIoWRITE.class);
    private final DcapProxyIoFactory proxyIoFactory;

    public ProxyIoWRITE(nfs_argop4 args, DcapProxyIoFactory proxyIoFactory) {
        super(args, nfs_opnum4.OP_WRITE);
        this.proxyIoFactory = proxyIoFactory;
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result) {
        final WRITE4res res = result.opwrite;

        CDC cdc = CDC.reset(proxyIoFactory.getCellName(), proxyIoFactory.getCellDomainName());
        try {
	    NDC.push(context.getRpcCall().getTransport().getRemoteSocketAddress().toString());
            Inode inode = context.currentInode();
            /**
             * NOTICE, we forward v4.1 to regular open as proxy-io with pnfs is not supported
             */
            if ((context.getMinorversion() > 0) || !context.getFs().hasIOLayout(inode) ) {
                /*
                 * if we have a special file, then fall back to regular write operation
                 */
                new OperationWRITE(_args).process(context, result);
                return;
            }

            long offset = _args.opwrite.offset.value;
            ByteBuffer data = _args.opwrite.data;
            stateid4 stateid = _args.opwrite.stateid;

	    int bytesWritten;
	    if (Stateids.isStateLess(stateid)) {

		/*
		 * As there was no open, we have to check  permissions.
		 */
		if (context.getFs().access(inode, nfs4_prot.ACCESS4_MODIFY | nfs4_prot.ACCESS4_EXTEND ) == 0) {
		    throw new ChimeraNFSException(nfsstat.NFSERR_ACCESS, "Permission denied.");
		}

		/*
		 * use try-with-resource as wee need to close adapter on each request
		 */
		try (ProxyIoAdapter oneUseProxyIoAdapter = proxyIoFactory.createIoAdapter(inode, context, true)) {
		    bytesWritten = oneUseProxyIoAdapter.write(data, offset);
		}
	    } else {

                /**
                 * NOTICE, we check for minor version here, even if 4.1 requests
                 * should not reach this place. Just keep it consistent for the future.
                 * is not supported
                 */

                if (context.getMinorversion() == 0) {
                    /*
                     *  The NFSv4.0 spec requires to update lease time as long as client
                     * needs the file. This is done through READ, WRITE and RENEW
                     * opertations. With introduction of sessions in v4.1 update of the
                     * lease time done through SEQUENCE operation.
                     */
                    context.getStateHandler().updateClientLeaseTime(stateid);
                }
		ProxyIoAdapter proxyIoAdapter = proxyIoFactory.getOrCreateProxy(inode, stateid, context, true);
		bytesWritten = proxyIoAdapter.write(data, offset);
	    }

            res.status = nfsstat.NFS_OK;
            res.resok4 = new WRITE4resok();
            res.resok4.count = new count4(bytesWritten);
            res.resok4.committed = stable_how4.FILE_SYNC4;
            res.resok4.writeverf = new verifier4();
            res.resok4.writeverf.value = new byte[nfs4_prot.NFS4_VERIFIER_SIZE];

            _log.debug("MOVER: {}@{} written.", bytesWritten, offset);

        }catch(ChimeraNFSException he) {
            res.status = he.getStatus();
            _log.debug(he.getMessage());
        }catch(IOException ioe) {
            _log.error("DSWRITE: {}", ioe.getMessage());
            res.status = nfsstat.NFSERR_IO;
        }catch(Exception e) {
            _log.error("DSWRITE: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        } finally {
            cdc.close();
        }
    }
}
