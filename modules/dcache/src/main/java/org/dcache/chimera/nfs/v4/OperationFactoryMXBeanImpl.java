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

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;

public class OperationFactoryMXBeanImpl implements OperationFactoryMXBean, NFSv4OperationFactory {

    private static final Logger _log = Logger.getLogger(OperationFactoryMXBeanImpl.class.getName());
    private final Map<Integer, AtomicLong> _counters = new ConcurrentHashMap<Integer, AtomicLong>();
    private final NFSv4OperationFactory _inner;

    /**
     * Create a new JMX bean for specified {@link NFSv4OperationFactory}.
     *
     * @param inner {@link NFSv4OperationFactory} to wrap.
     * @param name extention used to create a unique {@link ObjectName}
     */
    public OperationFactoryMXBeanImpl(NFSv4OperationFactory inner, String name) {
        _inner = inner;

        _counters.put(nfs_opnum4.OP_ACCESS, new AtomicLong());
        _counters.put(nfs_opnum4.OP_CLOSE, new AtomicLong());
        _counters.put(nfs_opnum4.OP_COMMIT, new AtomicLong());
        _counters.put(nfs_opnum4.OP_CREATE, new AtomicLong());
        _counters.put(nfs_opnum4.OP_DELEGPURGE, new AtomicLong());
        _counters.put(nfs_opnum4.OP_DELEGRETURN, new AtomicLong());
        _counters.put(nfs_opnum4.OP_GETATTR, new AtomicLong());
        _counters.put(nfs_opnum4.OP_GETFH, new AtomicLong());
        _counters.put(nfs_opnum4.OP_LINK, new AtomicLong());
        _counters.put(nfs_opnum4.OP_LOCK, new AtomicLong());
        _counters.put(nfs_opnum4.OP_LOCKT, new AtomicLong());
        _counters.put(nfs_opnum4.OP_LOCKU, new AtomicLong());
        _counters.put(nfs_opnum4.OP_LOOKUP, new AtomicLong());
        _counters.put(nfs_opnum4.OP_LOOKUPP, new AtomicLong());
        _counters.put(nfs_opnum4.OP_NVERIFY, new AtomicLong());
        _counters.put(nfs_opnum4.OP_OPEN, new AtomicLong());
        _counters.put(nfs_opnum4.OP_OPENATTR, new AtomicLong());
        _counters.put(nfs_opnum4.OP_OPEN_CONFIRM, new AtomicLong());
        _counters.put(nfs_opnum4.OP_OPEN_DOWNGRADE, new AtomicLong());
        _counters.put(nfs_opnum4.OP_PUTFH, new AtomicLong());
        _counters.put(nfs_opnum4.OP_PUTPUBFH, new AtomicLong());
        _counters.put(nfs_opnum4.OP_PUTROOTFH, new AtomicLong());
        _counters.put(nfs_opnum4.OP_READ, new AtomicLong());
        _counters.put(nfs_opnum4.OP_READDIR, new AtomicLong());
        _counters.put(nfs_opnum4.OP_READLINK, new AtomicLong());
        _counters.put(nfs_opnum4.OP_REMOVE, new AtomicLong());
        _counters.put(nfs_opnum4.OP_RENAME, new AtomicLong());
        _counters.put(nfs_opnum4.OP_RENEW, new AtomicLong());
        _counters.put(nfs_opnum4.OP_RESTOREFH, new AtomicLong());
        _counters.put(nfs_opnum4.OP_SAVEFH, new AtomicLong());
        _counters.put(nfs_opnum4.OP_SECINFO, new AtomicLong());
        _counters.put(nfs_opnum4.OP_SETATTR, new AtomicLong());
        _counters.put(nfs_opnum4.OP_SETCLIENTID, new AtomicLong());
        _counters.put(nfs_opnum4.OP_SETCLIENTID_CONFIRM, new AtomicLong());
        _counters.put(nfs_opnum4.OP_VERIFY, new AtomicLong());
        _counters.put(nfs_opnum4.OP_WRITE, new AtomicLong());
        _counters.put(nfs_opnum4.OP_RELEASE_LOCKOWNER, new AtomicLong());
        _counters.put(nfs_opnum4.OP_BACKCHANNEL_CTL, new AtomicLong());
        _counters.put(nfs_opnum4.OP_BIND_CONN_TO_SESSION, new AtomicLong());
        _counters.put(nfs_opnum4.OP_EXCHANGE_ID, new AtomicLong());
        _counters.put(nfs_opnum4.OP_CREATE_SESSION, new AtomicLong());
        _counters.put(nfs_opnum4.OP_DESTROY_SESSION, new AtomicLong());
        _counters.put(nfs_opnum4.OP_FREE_STATEID, new AtomicLong());
        _counters.put(nfs_opnum4.OP_GET_DIR_DELEGATION, new AtomicLong());
        _counters.put(nfs_opnum4.OP_GETDEVICEINFO, new AtomicLong());
        _counters.put(nfs_opnum4.OP_GETDEVICELIST, new AtomicLong());
        _counters.put(nfs_opnum4.OP_LAYOUTCOMMIT, new AtomicLong());
        _counters.put(nfs_opnum4.OP_LAYOUTGET, new AtomicLong());
        _counters.put(nfs_opnum4.OP_LAYOUTRETURN, new AtomicLong());
        _counters.put(nfs_opnum4.OP_SECINFO_NO_NAME, new AtomicLong());
        _counters.put(nfs_opnum4.OP_SEQUENCE, new AtomicLong());
        _counters.put(nfs_opnum4.OP_SET_SSV, new AtomicLong());
        _counters.put(nfs_opnum4.OP_TEST_STATEID, new AtomicLong());
        _counters.put(nfs_opnum4.OP_WANT_DELEGATION, new AtomicLong());
        _counters.put(nfs_opnum4.OP_DESTROY_CLIENTID, new AtomicLong());
        _counters.put(nfs_opnum4.OP_RECLAIM_COMPLETE, new AtomicLong());
        _counters.put(nfs_opnum4.OP_ILLEGAL, new AtomicLong());

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            String jmxName = String.format("%s:type=NFSv4.1 Operations, name=%s-%s",
                    _inner.getClass().getPackage().getName(), _inner.getClass().getSimpleName(), name);
            ObjectName mxBeanName = new ObjectName(jmxName);
            if (!server.isRegistered(mxBeanName)) {
                server.registerMBean(this, new ObjectName(jmxName));
            }
        } catch (MalformedObjectNameException ex) {
            _log.log(Level.SEVERE, ex.getMessage(), ex);
        } catch (InstanceAlreadyExistsException ex) {
            _log.log(Level.SEVERE, ex.getMessage(), ex);
        } catch (MBeanRegistrationException ex) {
            _log.log(Level.SEVERE, ex.getMessage(), ex);
        } catch (NotCompliantMBeanException ex) {
            _log.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public long getAccess() {
        return _counters.get(nfs_opnum4.OP_ACCESS).get();
    }

    public long getClose() {
        return _counters.get(nfs_opnum4.OP_CLOSE).get();
    }

    public long getCommit() {
        return _counters.get(nfs_opnum4.OP_COMMIT).get();
    }

    public long getCreate() {
        return _counters.get(nfs_opnum4.OP_CREATE).get();
    }

    public long getDelegpurge() {
        return _counters.get(nfs_opnum4.OP_DELEGPURGE).get();
    }

    public long getDelegreturn() {
        return _counters.get(nfs_opnum4.OP_DELEGRETURN).get();
    }

    public long getGetattr() {
        return _counters.get(nfs_opnum4.OP_GETATTR).get();
    }

    public long getGetfh() {
        return _counters.get(nfs_opnum4.OP_GETFH).get();
    }

    public long getLink() {
        return _counters.get(nfs_opnum4.OP_LINK).get();
    }

    public long getLock() {
        return _counters.get(nfs_opnum4.OP_LOCK).get();
    }

    public long getLockt() {
        return _counters.get(nfs_opnum4.OP_LOCKT).get();
    }

    public long getLocku() {
        return _counters.get(nfs_opnum4.OP_LOCKU).get();
    }

    public long getLookup() {
        return _counters.get(nfs_opnum4.OP_ACCESS).get();
    }

    public long getLookupp() {
        return _counters.get(nfs_opnum4.OP_LOOKUPP).get();
    }

    public long getNverify() {
        return _counters.get(nfs_opnum4.OP_NVERIFY).get();
    }

    public long getOpen() {
        return _counters.get(nfs_opnum4.OP_OPEN).get();
    }

    public long getOpenattr() {
        return _counters.get(nfs_opnum4.OP_OPENATTR).get();
    }

    public long getOpenConfirm() {
        return _counters.get(nfs_opnum4.OP_OPEN_CONFIRM).get();
    }

    public long getOpenDowngrade() {
        return _counters.get(nfs_opnum4.OP_OPEN_DOWNGRADE).get();
    }

    public long getPutfh() {
        return _counters.get(nfs_opnum4.OP_PUTFH).get();
    }

    public long getPutpubfh() {
        return _counters.get(nfs_opnum4.OP_PUTPUBFH).get();
    }

    public long getPutrootfh() {
        return _counters.get(nfs_opnum4.OP_PUTROOTFH).get();
    }

    public long getRead() {
        return _counters.get(nfs_opnum4.OP_READ).get();
    }

    public long getReaddir() {
        return _counters.get(nfs_opnum4.OP_READDIR).get();
    }

    public long getReadlink() {
        return _counters.get(nfs_opnum4.OP_READLINK).get();
    }

    public long getRemove() {
        return _counters.get(nfs_opnum4.OP_REMOVE).get();
    }

    public long getRename() {
        return _counters.get(nfs_opnum4.OP_RENAME).get();
    }

    public long getRenew() {
        return _counters.get(nfs_opnum4.OP_RENEW).get();
    }

    public long getRestorefh() {
        return _counters.get(nfs_opnum4.OP_RESTOREFH).get();
    }

    public long getSavefh() {
        return _counters.get(nfs_opnum4.OP_SAVEFH).get();
    }

    public long getSecinfo() {
        return _counters.get(nfs_opnum4.OP_SECINFO).get();
    }

    public long getSetattr() {
        return _counters.get(nfs_opnum4.OP_SETATTR).get();
    }

    public long getSetclientid() {
        return _counters.get(nfs_opnum4.OP_SETCLIENTID).get();
    }

    public long getSetclientidConfirm() {
        return _counters.get(nfs_opnum4.OP_SETCLIENTID_CONFIRM).get();
    }

    public long getVerify() {
        return _counters.get(nfs_opnum4.OP_VERIFY).get();
    }

    public long getWrite() {
        return _counters.get(nfs_opnum4.OP_WRITE).get();
    }

    public long getReleaseLockowner() {
        return _counters.get(nfs_opnum4.OP_RELEASE_LOCKOWNER).get();
    }

    public long getBackchannelCtl() {
        return _counters.get(nfs_opnum4.OP_BACKCHANNEL_CTL).get();
    }

    public long getBindConnToSession() {
        return _counters.get(nfs_opnum4.OP_BIND_CONN_TO_SESSION).get();
    }

    public long getExchangeId() {
        return _counters.get(nfs_opnum4.OP_EXCHANGE_ID).get();
    }

    public long getCreateSession() {
        return _counters.get(nfs_opnum4.OP_CREATE_SESSION).get();
    }

    public long getDestroySession() {
        return _counters.get(nfs_opnum4.OP_DESTROY_SESSION).get();
    }

    public long getFreeStateid() {
        return _counters.get(nfs_opnum4.OP_FREE_STATEID).get();
    }

    public long getGetDirDelegation() {
        return _counters.get(nfs_opnum4.OP_GET_DIR_DELEGATION).get();
    }

    public long getGetdeviceinfo() {
        return _counters.get(nfs_opnum4.OP_GETDEVICEINFO).get();
    }

    public long getGetdevicelist() {
        return _counters.get(nfs_opnum4.OP_GETDEVICELIST).get();
    }

    public long getLayoutcommit() {
        return _counters.get(nfs_opnum4.OP_LAYOUTCOMMIT).get();
    }

    public long getLayoutget() {
        return _counters.get(nfs_opnum4.OP_LAYOUTGET).get();
    }

    public long getLayoutreturn() {
        return _counters.get(nfs_opnum4.OP_LAYOUTRETURN).get();
    }

    public long getSecinfoNoName() {
        return _counters.get(nfs_opnum4.OP_SECINFO_NO_NAME).get();
    }

    public long getSequence() {
        return _counters.get(nfs_opnum4.OP_SEQUENCE).get();
    }

    public long getSetSsv() {
        return _counters.get(nfs_opnum4.OP_SET_SSV).get();
    }

    public long getTestStateid() {
        return _counters.get(nfs_opnum4.OP_TEST_STATEID).get();
    }

    public long getWantDelegation() {
        return _counters.get(nfs_opnum4.OP_WANT_DELEGATION).get();
    }

    public long getDestroyClientid() {
        return _counters.get(nfs_opnum4.OP_DESTROY_CLIENTID).get();
    }

    public long getReclaimComplete() {
        return _counters.get(nfs_opnum4.OP_RECLAIM_COMPLETE).get();
    }

    public long getIllegal() {
        return _counters.get(nfs_opnum4.OP_ILLEGAL).get();
    }

    public AbstractNFSv4Operation getOperation(nfs_argop4 op) {
        AtomicLong counter = _counters.get(op.argop);
        if (counter != null) {
            counter.incrementAndGet();
        }
        return _inner.getOperation(op);
    }
}
