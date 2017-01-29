package org.dcache.chimera.nfsv41.door.proxy;

import java.io.IOException;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.NFS4Client;
import org.dcache.nfs.v4.NFS4State;
import org.dcache.nfs.v4.Stateids;
import org.dcache.nfs.v4.xdr.CLOSE4res;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyIoClose extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(ProxyIoClose.class);

    ProxyIoClose(nfs_argop4 args) {
        super(args, nfs_opnum4.OP_CLOSE);
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result)
            throws ChimeraNFSException, IOException {
        final CLOSE4res res = result.opclose;

        Inode inode = context.currentInode();

        stateid4 stateid = Stateids.getCurrentStateidIfNeeded(context, _args.opclose.open_stateid);
        NFS4Client client;
        if (context.getMinorversion() > 0) {
            client = context.getSession().getClient();
        } else {
            client = context.getStateHandler().getClientIdByStateId(stateid);
            NFS4State nfsState = client.state(stateid);
            nfsState.getStateOwner().acceptAsNextSequence(_args.opclose.seqid);
        }

        context.getDeviceManager().layoutReturn(context, _args.opclose.open_stateid);

        // REVISIT: as we pass open-state id as layout state id, we dont need to invalidate it
        // client.releaseState(_args.opclose.open_stateid);
        client.updateLeaseTime();

        res.open_stateid = Stateids.invalidStateId();
        res.status = nfsstat.NFS_OK;

    }
}
