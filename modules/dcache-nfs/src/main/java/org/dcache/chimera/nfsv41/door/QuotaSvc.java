package org.dcache.chimera.nfsv41.door;

import org.dcache.auth.Subjects;
import org.dcache.oncrpc4j.rpc.RpcCall;
import org.dcache.rquota.QuotaVfs;
import org.dcache.rquota.xdr.ext_getquota_args;
import org.dcache.rquota.xdr.ext_setquota_args;
import org.dcache.rquota.xdr.getquota_args;
import org.dcache.rquota.xdr.getquota_rslt;
import org.dcache.rquota.xdr.qr_status;
import org.dcache.rquota.xdr.rquotaServerStub;
import org.dcache.rquota.xdr.setquota_args;
import org.dcache.rquota.xdr.setquota_rslt;

import javax.security.auth.Subject;

public class QuotaSvc extends rquotaServerStub {

    private static final int USER_QUOTA = 0;
    private static final int GROUP_QUOTA = 1;

    private final QuotaVfs _qfs;

    public QuotaSvc(QuotaVfs _qfs) {
        this._qfs = _qfs;
    }

    @Override
    public getquota_rslt RQUOTAPROC_GETQUOTA_1(RpcCall call$, getquota_args arg1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public getquota_rslt RQUOTAPROC_GETACTIVEQUOTA_1(RpcCall call$, getquota_args arg1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public setquota_rslt RQUOTAPROC_SETQUOTA_1(RpcCall call$, setquota_args arg1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public setquota_rslt RQUOTAPROC_SETACTIVEQUOTA_1(RpcCall call$, setquota_args arg1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public getquota_rslt RQUOTAPROC_GETQUOTA_2(RpcCall call$, ext_getquota_args arg1) {
        var r = new getquota_rslt();

        if (!canQuery(call$.getCredential().getSubject(), arg1.gqa_id, arg1.gqa_type)) {
            r.status = qr_status.Q_EPERM;
            return r;
        }

        r.status = qr_status.Q_OK;
        r.gqr_rquota = _qfs.getQuota(arg1.gqa_id, arg1.gqa_type);
        return r;
    }

    @Override
    public getquota_rslt RQUOTAPROC_GETACTIVEQUOTA_2(RpcCall call$, ext_getquota_args arg1) {
        var r = new getquota_rslt();

        if (!canQuery(call$.getCredential().getSubject(), arg1.gqa_id, arg1.gqa_type)) {
            r.status = qr_status.Q_EPERM;
            return r;
        }

        r.status = qr_status.Q_OK;
        r.gqr_rquota = _qfs.getQuota(arg1.gqa_id, arg1.gqa_type);
        return r;
    }

    @Override
    public setquota_rslt RQUOTAPROC_SETQUOTA_2(RpcCall call$, ext_setquota_args arg1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public setquota_rslt RQUOTAPROC_SETACTIVEQUOTA_2(RpcCall call$, ext_setquota_args arg1) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Check if the given subject can query the quota for the given id and type.
     *
     * @param subject the subject to check
     * @param id the id to check
     * @param type the type to check
     * @return true if the subject can query the quota, false otherwise
     */
    private boolean canQuery(Subject subject, int id, int type ) {
        return Subjects.isRoot(subject) ||
                (type == USER_QUOTA && Subjects.hasUid(subject, id)) ||
                (type == GROUP_QUOTA && Subjects.hasGid(subject, id));
    }
}
