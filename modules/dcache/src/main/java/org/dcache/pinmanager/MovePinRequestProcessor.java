package org.dcache.pinmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.InvalidMessageCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PoolSetStickyMessage;

import dmg.cells.nucleus.CellPath;

import org.dcache.auth.Subjects;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.pinmanager.model.Pin;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.services.pinmanager1.PinManagerMovePinMessage;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.dcache.pinmanager.model.Pin.State.*;
import static org.springframework.transaction.annotation.Isolation.REPEATABLE_READ;

/**
 * Processes requests to move pins.
 *
 * The stratetegy for moving a pin is the following:
 *
 *  1. Create a record in the DB for the target pool
 *  2. Create a sticky flag on the target pool
 *  3. Change the original record to point to the target pool and
 *     change the record created in step 1 to point to the old pool.
 *  4. Remove the sticky flag from the old pool.
 *
 * If the above process is aborted at any point the regular recovery
 * tasks of the pin manager will remove any stale sticky flag from
 * either the old or the new pool.
 *
 * Pin lifetime extension is treated as a special case of move (moving
 * to the same pool, but with a longer lifetime).
 */
public class MovePinRequestProcessor
    implements CellMessageReceiver
{
    private static final Logger _log =
        LoggerFactory.getLogger(MovePinRequestProcessor.class);

    private static final long POOL_LIFETIME_MARGIN = MINUTES.toMillis(30);

    private PinDao _dao;
    private CellStub _poolStub;
    private AuthorizationPolicy _pdp;
    private long _maxLifetime;
    private TimeUnit _maxLifetimeUnit;
    private PoolMonitor _poolMonitor;

    @Required
    public void setDao(PinDao dao)
    {
        _dao = dao;
    }

    @Required
    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    @Required
    public void setAuthorizationPolicy(AuthorizationPolicy pdp)
    {
        _pdp = pdp;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    @Required
    public void setMaxLifetimeUnit(TimeUnit unit)
    {
        _maxLifetimeUnit = unit;
    }

    public TimeUnit getMaxLifetimeUnit()
    {
        return _maxLifetimeUnit;
    }

    @Required
    public void setMaxLifetime(long maxLifetime)
    {
        _maxLifetime = maxLifetime;
    }

    @Required
    public long getMaxLifetime()
    {
        return _maxLifetime;
    }

    protected Pin createTemporaryPin(PnfsId pnfsId, String pool)
    {
        long now = System.currentTimeMillis();
        return _dao.create(_dao.set()
                                   .subject(Subjects.ROOT)
                                   .pnfsId(pnfsId)
                                   .state(PINNING)
                                   .pool(pool)
                                   .sticky("PinManager-" + UUID.randomUUID().toString())
                                   .expirationTime(new Date(now + 2 * _poolStub.getTimeoutInMillis())));
    }

    @Transactional(isolation=REPEATABLE_READ)
    protected Pin swapPins(Pin pin, Pin tmpPin, Date expirationTime)
        throws CacheException
    {
        Pin targetPin = _dao.get(_dao.where()
                                         .id(tmpPin.getPinId())
                                         .sticky(tmpPin.getSticky())
                                         .state(PINNING));

        if (targetPin == null) {
            /* The pin likely expired. We are now in a situation in
             * which we may or may not have a sticky flag on the
             * target pool, but no record in the database. To be on
             * the safe side we create a new record in the database
             * and then abort.
             */
            _dao.create(_dao.set()
                                .subject(Subjects.ROOT)
                                .pnfsId(tmpPin.getPnfsId())
                                .pool(tmpPin.getPool())
                                .sticky(tmpPin.getSticky())
                                .state(UNPINNING));
            throw new TimeoutCacheException("Move expired");
        }

        Pin sourcePin =
                _dao.update(_dao.where()
                                    .id(pin.getPinId())
                                    .sticky(pin.getSticky())
                                    .state(PINNED),
                            _dao.set()
                                    .pool(targetPin.getPool())
                                    .sticky(targetPin.getSticky())
                                    .expirationTime(expirationTime));

        if (sourcePin == null) {
            /* The target pin will expire by itself.
             */
            throw new CacheException("Pin no longer valid");
        }

        return _dao.update(targetPin,
                    _dao.set()
                            .pool(pin.getPool())
                            .sticky(pin.getSticky())
                            .state(UNPINNING));
    }

    private void setSticky(String poolName, PnfsId pnfsId, boolean sticky, String owner, long validTill)
            throws CacheException, InterruptedException, NoRouteToCellException
    {
        PoolSelectionUnit.SelectionPool pool = _poolMonitor.getPoolSelectionUnit().getPool(poolName);
        if (pool == null || !pool.isActive()) {
            throw new CacheException("Unable to move sticky flag because pool " + poolName + " is unavailable");
        }
        PoolSetStickyMessage msg =
            new PoolSetStickyMessage(poolName, pnfsId, sticky, owner, validTill);
        _poolStub.sendAndWait(new CellPath(pool.getAddress()), msg);
    }

    protected Pin move(Pin pin, String pool, Date expirationTime)
            throws CacheException, InterruptedException, NoRouteToCellException
    {
        Pin tmpPin = createTemporaryPin(pin.getPnfsId(), pool);
        setSticky(tmpPin.getPool(),
                  tmpPin.getPnfsId(),
                  true,
                  tmpPin.getSticky(),
                  (expirationTime == null) ? - 1 : (expirationTime.getTime() + POOL_LIFETIME_MARGIN));
        return swapPins(pin, tmpPin, expirationTime);
    }

    private boolean containsPin(Collection<Pin> pins, String sticky)
    {
        for (Pin pin: pins) {
            if (sticky.equals(pin.getSticky())) {
                return true;
            }
        }
        return false;
    }

    public PinManagerMovePinMessage
        messageArrived(PinManagerMovePinMessage message)
        throws CacheException, InterruptedException
    {
        try {
            PnfsId pnfsId = message.getPnfsId();
            String source = message.getSourcePool();
            String target = message.getTargetPool();

            Collection<Pin> pins = _dao.get(_dao.where().pnfsId(pnfsId).pool(source));

            /* Remove all stale sticky flags.
             */
            for (StickyRecord record: message.getRecords()) {
                if (!containsPin(pins, record.owner())) {
                    setSticky(source, pnfsId, false, record.owner(), 0);
                }
            }

            /* Move all pins to the target pool.
             */
            for (Pin pin: pins) {
                Pin tmpPin = move(pin, target, pin.getExpirationTime());

                setSticky(tmpPin.getPool(),
                          tmpPin.getPnfsId(),
                          false,
                          tmpPin.getSticky(),
                          0);

                _dao.delete(tmpPin);
            }

            _log.info("Moved pins for {} from {} to {}", pnfsId, source, target);
        } catch (NoRouteToCellException e) {
            throw new CacheException("Failed to move pin due to communication failure: " + e.getDestinationPath(), e);
        }

        return message;
    }

    public PinManagerExtendPinMessage
        messageArrived(PinManagerExtendPinMessage message)
        throws CacheException, InterruptedException
    {
        try {
            Pin pin = _dao.get(_dao.where().pnfsId(message.getFileAttributes().getPnfsId()).id(message.getPinId()));
            if (pin == null) {
                throw new InvalidMessageCacheException("Pin does not exist");
            } else if (!_pdp.canExtend(message.getSubject(), pin)) {
                throw new PermissionDeniedCacheException("Access denied");
            } else if (pin.getState() == PINNING) {
                throw new InvalidMessageCacheException("File is not pinned yet");
            } else if (pin.getState() == UNPINNING) {
                throw new InvalidMessageCacheException("Pin is no longer valid");
            }

            if (_maxLifetime > -1) {
                message.setLifetime(Math.min(_maxLifetimeUnit.toMillis(_maxLifetime), message.getLifetime()));
            }

            long lifetime = message.getLifetime();
            if (pin.hasRemainingLifetimeLessThan(lifetime)) {
                long now = System.currentTimeMillis();
                Date date = (lifetime == -1) ? null : new Date(now + lifetime);
                move(pin, pin.getPool(), date);
                message.setExpirationTime(date);
            } else {
                message.setExpirationTime(pin.getExpirationTime());
            }

            _log.info("Extended pin for {} ({})", pin.getPnfsId(), pin.getPinId());

            return message;
        } catch (NoRouteToCellException e) {
            throw new CacheException("Failed to extend pin due to communication failure: " + e.getDestinationPath(), e);
        }
    }
}
