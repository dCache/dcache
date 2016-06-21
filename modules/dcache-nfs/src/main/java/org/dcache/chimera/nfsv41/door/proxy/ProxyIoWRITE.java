package org.dcache.chimera.nfsv41.door.proxy;

import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.dcache.util.NDC;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.status.AccessException;
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
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.v4.xdr.verifier4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.VirtualFileSystem;

public class ProxyIoWRITE extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(ProxyIoWRITE.class);
    private final ProxyIoFactory proxyIoFactory;

    public ProxyIoWRITE(nfs_argop4 args, ProxyIoFactory proxyIoFactory) {
        super(args, nfs_opnum4.OP_WRITE);
        this.proxyIoFactory = proxyIoFactory;
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException {
        final WRITE4res res = result.opwrite;

        Inode inode = context.currentInode();
        NDC.push(context.getRpcCall().getTransport().getRemoteSocketAddress().toString());
        NDC.push(BaseEncoding.base16().upperCase().encode(inode.getFileId()));

        try {

            if (!context.getFs().hasIOLayout(inode)) {
                /*
                 * if we have a special file, then fall back to regular write operation
                 */
                new OperationWRITE(_args).process(context, result);
                return;
            }

            long offset = _args.opwrite.offset.value;
            ByteBuffer data = _args.opwrite.data;
            stateid4 stateid = _args.opwrite.stateid;

            VirtualFileSystem.WriteResult writeResult;
	    if (Stateids.isStateLess(stateid)) {

		/*
		 * As there was no open, we have to check  permissions.
		 */
		if (context.getFs().access(inode, nfs4_prot.ACCESS4_MODIFY | nfs4_prot.ACCESS4_EXTEND ) == 0) {
		    throw new AccessException();
		}

		/*
		 * use try-with-resource as wee need to close adapter on each request
		 */
		try (ProxyIoAdapter oneUseProxyIoAdapter = proxyIoFactory.createIoAdapter(inode, stateid, context, true)) {
		    writeResult = oneUseProxyIoAdapter.write(data, offset);
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
		writeResult = proxyIoAdapter.write(data, offset);
	    }

            res.status = nfsstat.NFS_OK;
            res.resok4 = new WRITE4resok();
            res.resok4.count = new count4(writeResult.getBytesWritten());
            res.resok4.committed = writeResult.getStabilityLevel().toStableHow();
            res.resok4.writeverf = new verifier4();
            res.resok4.writeverf.value = new byte[nfs4_prot.NFS4_VERIFIER_SIZE];

            _log.debug("MOVER: {}@{} written.", writeResult.getBytesWritten(), offset);

        } catch (ChimeraNFSException e) {
            // NFS server will handle them
            throw e;
        }catch(IOException ioe) {
            _log.error("DSWRITE: {}", ioe.getMessage());
            proxyIoFactory.shutdownAdapter(_args.opwrite.stateid);
            res.status = nfsstat.NFSERR_IO;
        }catch(Exception e) {
            _log.error("DSWRITE: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        } finally {
            NDC.pop();
            NDC.pop();
        }
    }
}
