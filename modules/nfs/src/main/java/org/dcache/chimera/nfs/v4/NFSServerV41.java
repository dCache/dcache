/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.chimera.nfs.v4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.ExportFile;
import org.dcache.chimera.nfs.PseudoFsProvider;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.COMPOUND4args;
import org.dcache.chimera.nfs.v4.xdr.COMPOUND4res;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot_NFS4_PROGRAM_ServerStub;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.dcache.chimera.posix.AclHandler;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.xdr.RpcCall;

public class NFSServerV41 extends nfs4_prot_NFS4_PROGRAM_ServerStub {

    private final FileSystemProvider _fs;
    private final ExportFile _exportFile;
    private static final Logger _log = LoggerFactory.getLogger(NFSServerV41.class);
    private final NFSv4OperationFactory _operationFactory;
    private final NFSv41DeviceManager _deviceManager;
    private final AclHandler _aclHandler;
    private final NFSv4StateHandler _statHandler = new NFSv4StateHandler();
    private final NfsIdMapping _idMapping;
    private final ServerIdProvider _idProvider;

    private static final RequestExecutionTimeGauges<String> GAUGES = new
            RequestExecutionTimeGauges<>(NFSServerV41.class.getName());

    public NFSServerV41(NFSv4OperationFactory operationFactory,
            NFSv41DeviceManager deviceManager, AclHandler aclHandler, FileSystemProvider fs,
            NfsIdMapping idMapping,
            ExportFile exportFile, ServerIdProvider idProvider)
    {

        _deviceManager = deviceManager;
        _fs = fs;
        _exportFile = exportFile;
        _operationFactory = operationFactory;
        _aclHandler = aclHandler;
        _idMapping = idMapping;
        _idProvider = idProvider;
    }

    @Override
    public void NFSPROC4_NULL_4(RpcCall call$) {
        _log.trace("NFS PING client: {}", call$.getTransport().getRemoteSocketAddress());
    }

    @Override
    public COMPOUND4res NFSPROC4_COMPOUND_4(RpcCall call$, COMPOUND4args arg1) {


        COMPOUND4res res = new COMPOUND4res();

        try {

            _log.trace("NFS COMPOUND client: {}, tag: [{}]",
                    call$.getTransport().getRemoteSocketAddress(),
                    new String(arg1.tag.value.value));

            /*
             * here we have to checkfor utf8, but it's too much work to keep
             * spec happy.
             */
            MDC.put(NfsMdc.TAG, arg1.tag.toString());
            MDC.put(NfsMdc.CLIENT, call$.getTransport().getRemoteSocketAddress().toString());
            int minorversion = arg1.minorversion.value;
            if ( minorversion > 1) {
                 throw new ChimeraNFSException(nfsstat.NFSERR_MINOR_VERS_MISMATCH,
                     String.format("Unsupported minor version [%d]",arg1.minorversion.value) );
             }

            FileSystemProvider vfs = new PseudoFsProvider(_fs, _exportFile, call$);
            CompoundContext context = new CompoundContext(arg1.minorversion.value,
                vfs, _statHandler, _deviceManager, _aclHandler, call$, _idMapping,
                    _exportFile, _idProvider, arg1.argarray.length);

            res.status = nfsstat.NFS_OK;
            res.resarray = new ArrayList<>(arg1.argarray.length);
            res.tag = arg1.tag;

            boolean retransmit = false;
            for (nfs_argop4 op : arg1.argarray) {
                context.nextOperation();

                int position = context.getOperationPosition();
                if (minorversion > 0) {
                    checkOpPosition(op.argop, position);
                    if (position == 1) {
                        /*
                         * at this point we already have to have a session
                         */
                        List<nfs_resop4> cache = context.getCache();
                        if (cache != null) {
                            res.resarray.addAll(cache.subList(position, cache.size()));
                            res.status = statusOfLastOperation(cache);
                            retransmit = true;
                            break;
                        }
                    }
                }

                long t0 = System.currentTimeMillis();
                nfs_resop4 opResult = _operationFactory.getOperation(op).process(context);
                GAUGES.update(nfs_opnum4.toString(op.argop), System.currentTimeMillis() - t0);

                res.resarray.add(opResult);
                res.status = opResult.getStatus();
                if (res.status != nfsstat.NFS_OK) {
                    break;
                }
            }

            if (!retransmit && context.cacheThis()) {
                context.getSession().updateSlotCache(context.getSlotId(), res.resarray);
            }

            _log.trace( "OP: [{}] status: {}", res.tag, res.status);

        } catch (ChimeraNFSException e) {
            _log.debug("NFS operation failed: {}", e.getMessage());
            res.resarray = Collections.emptyList();
            res.status = e.getStatus();
            res.tag = arg1.tag;
        } catch (Exception e) {
            _log.error("Unhandled exception:", e);
            res.resarray = Collections.emptyList();
            res.status = nfsstat.NFSERR_SERVERFAULT;
            res.tag = arg1.tag;
        }finally{
            MDC.remove(NfsMdc.TAG);
            MDC.remove(NfsMdc.CLIENT);
            MDC.remove(NfsMdc.SESSION);
        }

        return res;
    }

    /**
     * Get {@link List} of currently active clients.
     * @return clients.
     */
    public List<NFS4Client> getClients() {
        return _statHandler.getClients();
    }

    /*
     *
     * from NFSv4.1 spec:
     *
     * SEQUENCE MUST appear as the first operation of any COMPOUND in which
     * it appears.  The error NFS4ERR_SEQUENCE_POS will be returned when it
     * is found in any position in a COMPOUND beyond the first.  Operations
     * other than SEQUENCE, BIND_CONN_TO_SESSION, EXCHANGE_ID,
     * CREATE_SESSION, and DESTROY_SESSION, MUST NOT appear as the first
     * operation in a COMPOUND.  Such operations MUST yield the error
     * NFS4ERR_OP_NOT_IN_SESSION if they do appear at the start of a
     * COMPOUND.
     *
     */
    private static void checkOpPosition(int opCode, int position) throws ChimeraNFSException {

        /*
         * special case of illegal operations.
         */
        if(opCode > nfs_opnum4.OP_RECLAIM_COMPLETE || opCode < nfs_opnum4.OP_ACCESS) {
            return;
        }

        if(position == 0 ) {
            switch(opCode) {
                case nfs_opnum4.OP_SEQUENCE:
                case nfs_opnum4.OP_CREATE_SESSION:
                case nfs_opnum4.OP_EXCHANGE_ID:
                case nfs_opnum4.OP_DESTROY_SESSION:
                    break;
                default:
                    throw new ChimeraNFSException(nfsstat.NFSERR_OP_NOT_IN_SESSION, "not in session");
            }
        } else {
            switch (opCode) {
                case nfs_opnum4.OP_SEQUENCE:
                    throw new ChimeraNFSException(nfsstat.NFSERR_SEQUENCE_POS, "not a first operation");
            }
        }
    }

    private static int statusOfLastOperation(List<nfs_resop4> ops) {
        return ops.get(ops.size() -1).getStatus();
    }

    public RequestExecutionTimeGauges<String> getStatistics() {
        return GAUGES;
    }
}
