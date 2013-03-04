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

import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;

public class MDSOperationFactory implements NFSv4OperationFactory {

    @Override
    public AbstractNFSv4Operation getOperation(nfs_argop4 op) {

        switch (op.argop) {
            case nfs_opnum4.OP_ACCESS:
                return new OperationACCESS(op);
            case nfs_opnum4.OP_CLOSE:
                return new OperationCLOSE(op);
            case nfs_opnum4.OP_COMMIT:
                return new OperationCOMMIT(op);
            case nfs_opnum4.OP_CREATE:
                return new OperationCREATE(op);
            case nfs_opnum4.OP_DELEGPURGE:
                return new OperationDELEGPURGE(op);
            case nfs_opnum4.OP_DELEGRETURN:
                return new OperationDELEGRETURN(op);
            case nfs_opnum4.OP_GETATTR:
                return new OperationGETATTR(op);
            case nfs_opnum4.OP_GETFH:
                return new OperationGETFH(op);
            case nfs_opnum4.OP_LINK:
                return new OperationLINK(op);
            case nfs_opnum4.OP_LOCK:
                return new OperationLOCK(op);
            case nfs_opnum4.OP_LOCKT:
                return new OperationLOCKT(op);
            case nfs_opnum4.OP_LOCKU:
                return new OperationLOCKU(op);
            case nfs_opnum4.OP_LOOKUP:
                return new OperationLOOKUP(op);
            case nfs_opnum4.OP_LOOKUPP:
                return new OperationLOOKUPP(op);
            case nfs_opnum4.OP_NVERIFY:
                return new OperationNVERIFY(op);
            case nfs_opnum4.OP_OPEN:
                return new OperationOPEN(op);
            case nfs_opnum4.OP_OPENATTR:
                return new OperationOPENATTR(op);
            case nfs_opnum4.OP_OPEN_CONFIRM:
                return new OperationOPEN_CONFIRM(op);
            case nfs_opnum4.OP_OPEN_DOWNGRADE:
                break;
            case nfs_opnum4.OP_PUTFH:
                return new OperationPUTFH(op);
            case nfs_opnum4.OP_PUTPUBFH:
                return new OperationPUTPUBFH(op);
            case nfs_opnum4.OP_PUTROOTFH:
                return new OperationPUTROOTFH(op);
            case nfs_opnum4.OP_READ:
                return new OperationREAD(op);
            case nfs_opnum4.OP_READDIR:
                return new OperationREADDIR(op);
            case nfs_opnum4.OP_READLINK:
                return new OperationREADLINK(op);
            case nfs_opnum4.OP_REMOVE:
                return new OperationREMOVE(op);
            case nfs_opnum4.OP_RENAME:
                return new OperationRENAME(op);
            case nfs_opnum4.OP_RENEW:
                return new OperationRENEW(op);
            case nfs_opnum4.OP_RESTOREFH:
                return new OperationRESTOREFH(op);
            case nfs_opnum4.OP_SAVEFH:
                return new OperationSAVEFH(op);
            case nfs_opnum4.OP_SECINFO:
                return new OperationSECINFO(op);
            case nfs_opnum4.OP_SETATTR:
                return new OperationSETATTR(op);
            case nfs_opnum4.OP_SETCLIENTID:
                return new OperationSETCLIENTID(op);
            case nfs_opnum4.OP_SETCLIENTID_CONFIRM:
                return new OperationSETCLIENTID_CONFIRM(op);
            case nfs_opnum4.OP_VERIFY:
                return new OperationVERIFY(op);
            case nfs_opnum4.OP_WRITE:
                return new OperationWRITE(op);
            /*            case nfs_opnum4.OP_RELEASE_LOCKOWNER:
            nRes.oprelease_lockowner = new RELEASE_LOCKOWNER4res();
            res = new NFSv4OperationResult(nRes, nfsstat4.NFS4ERR_NOTSUPP);
            break; */

            /**
             *  NFSv4.1 (pNFS)
             */
            case nfs_opnum4.OP_GETDEVICELIST:
                return new OperationGETDEVICELIST(op);
            case nfs_opnum4.OP_LAYOUTGET:
                return new OperationLAYOUTGET(op);
            case nfs_opnum4.OP_LAYOUTCOMMIT:
                return new OperationLAYOUTCOMMIT(op);
            case nfs_opnum4.OP_LAYOUTRETURN:
                return new OperationLAYOUTRETURN(op);
            case nfs_opnum4.OP_GETDEVICEINFO:
                return new OperationGETDEVICEINFO(op);
            case nfs_opnum4.OP_EXCHANGE_ID:
                /*
                 * By having chimera as a backend file system we need to support
                 * legacy .(xx)(xx)... commands. To allow read of those files,
                 * MDS ( nfsv41 door ) have to declare itself as a data server (DS).
                 *
                 * indicate that we are a MDS and DS at the same time.
                 */
                return new OperationEXCHANGE_ID(op, nfs4_prot.EXCHGID4_FLAG_USE_PNFS_MDS|
                        nfs4_prot.EXCHGID4_FLAG_USE_PNFS_DS);
            case nfs_opnum4.OP_CREATE_SESSION:
                return new OperationCREATE_SESSION(op);
            case nfs_opnum4.OP_DESTROY_SESSION:
                return new OperationDESTROY_SESSION(op);
            case nfs_opnum4.OP_SEQUENCE:
                return new OperationSEQUENCE(op);
            case nfs_opnum4.OP_RECLAIM_COMPLETE:
                return new OperationRECLAIM_COMPLETE(op);
            case nfs_opnum4.OP_ILLEGAL:

        }

        return new OperationILLEGAL(op);
    }
}
