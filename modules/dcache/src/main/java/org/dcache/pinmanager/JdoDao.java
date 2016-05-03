package org.dcache.pinmanager;

import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.jdo.FetchPlan;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;

import java.util.Collection;
import java.util.Date;

import diskCacheV111.util.PnfsId;

import org.dcache.pinmanager.model.Pin;

@Repository
public class JdoDao implements PinDao
{
    private static final Logger _log = LoggerFactory.getLogger(JdoDao.class);

    private PersistenceManagerFactory _pmf;

    @Required
    public void setPersistenceManagerFactory(PersistenceManagerFactory pmf)
    {
        _pmf = pmf;
    }

    @Override @Transactional
    public Pin storePin(Pin pin)
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        pin = pm.detachCopy(pm.makePersistent(pin));
        if (_log.isDebugEnabled()) {
            _log.debug(pin.toString());
        }
        return pin;
    }

    @Override @Transactional(readOnly=true)
    public Pin getPin(long id)
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        Query query =
            pm.newQuery(Pin.class,
                        "_id == :id");
        query.setUnique(true);
        Pin pin = (Pin) query.execute(id);
        return (pin == null) ? null : pm.detachCopy(pin);
    }

    @Override @Transactional(readOnly=true)
    public Pin getPin(PnfsId pnfsId, long id)
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        Query query =
            pm.newQuery(Pin.class,
                        "_pnfsId == :pnfsId && _id == :id");
        query.setUnique(true);
        Pin pin = (Pin) query.execute(pnfsId.toString(), id);
        return (pin == null) ? null : pm.detachCopy(pin);
    }

    @Override @Transactional(readOnly=true)
    public Pin getPin(long id, String sticky, Pin.State state)
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        Query query =
            pm.newQuery(Pin.class,
                        "_id == :id && _sticky == :sticky && _state == :state");
        query.setUnique(true);
        Pin pin = (Pin) query.execute(id, sticky, state);
        return (pin == null) ? null : pm.detachCopy(pin);
    }

    @Override @Transactional(readOnly=true)
    public Pin getPin(PnfsId pnfsId, String requestId)
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        Query query =
            pm.newQuery(Pin.class,
                        "_pnfsId == :pnfsId && _requestId == :requestId");
        query.setUnique(true);
        Pin pin = (Pin) query.execute(pnfsId.toString(), requestId);
        return (pin == null) ? null : pm.detachCopy(pin);
    }

    @Override @Transactional(readOnly=true)
    public Collection<Pin> getPins()
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        Query query = pm.newQuery(Pin.class);
        Collection<Pin> pins = (Collection<Pin>) query.execute();
        return pm.detachCopyAll(pins);
    }

    @Override @Transactional(readOnly=true)
    public Collection<Pin> getPins(PnfsId pnfsId)
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        Query query = pm.newQuery(Pin.class, "_pnfsId == :pnfsId");
        Collection<Pin> pins = (Collection<Pin>) query.execute(pnfsId.toString());
        return pm.detachCopyAll(pins);
    }

    @Override @Transactional(readOnly=true)
    public Collection<Pin> getPins(Pin.State state)
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        Query query = pm.newQuery(Pin.class, "_state == :state");
        Collection<Pin> pins = (Collection<Pin>) query.execute(state);
        return pm.detachCopyAll(pins);
    }

    @Override @Transactional(readOnly=true)
    public Collection<Pin> getPins(PnfsId pnfsId, String pool)
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        Query query = pm.newQuery(Pin.class, "_pnfsId == :pnfsId && _pool == :pool");
        Collection<Pin> pins = (Collection<Pin>) query.execute(pnfsId.toString(), pool);
        return pm.detachCopyAll(pins);
    }

    @Override @Transactional
    public void deletePin(PnfsId pnfsId)
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        pm.newQuery(Pin.class, "_pnfsId == :pnfsId").deletePersistentAll(pnfsId.toString());
    }

    @Override @Transactional
    public void deletePin(Pin pin)
    {
        try {
            PersistenceManager pm = _pmf.getPersistenceManager();
            pm.deletePersistent(pin);
        } catch (JDOObjectNotFoundException e) {
            /* The pin was already deleted (maybe because the file was
             * deleted). We don't care.
             */
            _log.debug("Pin deletion failed: {}", e);
        }
    }

    @Override @Transactional
    public void expirePins()
    {
        Date now = new Date();
        PersistenceManager pm = _pmf.getPersistenceManager();
        Query query = pm.newQuery(Pin.class, "_expirationTime < :now && _state != 'UNPINNING'");
        query.addExtension("datanucleus.rdbms.query.resultSetType", "scroll-insensitive");
        query.addExtension("datanucleus.query.resultCacheType", "none");
        query.getFetchPlan().setFetchSize(FetchPlan.FETCH_SIZE_OPTIMAL);
        try {
            for (Pin pin: (Collection<Pin>) query.execute(now)) {
                pin.setState(Pin.State.UNPINNING);
            }
        } finally {
            query.closeAll();
        }
    }

    @Override @Transactional
    public boolean all(Pin.State state, Predicate<Pin> f)
    {
        PersistenceManager pm = _pmf.getPersistenceManager();
        Query query = pm.newQuery(Pin.class, "_state == :state");
        query.addExtension("datanucleus.rdbms.query.resultSetType", "scroll-insensitive");
        query.addExtension("datanucleus.query.resultCacheType", "none");
        query.getFetchPlan().setFetchSize(FetchPlan.FETCH_SIZE_OPTIMAL);
        try {
            for (Pin pin: (Collection<Pin>) query.execute(state)) {
                if (!f.apply(pm.detachCopy(pin))) {
                    return false;
                }
            }
        } finally {
            query.closeAll();
        }
        return true;
    }
}
