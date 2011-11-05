package org.dcache.pinmanager;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import org.junit.*;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;
import diskCacheV111.poolManager.*;
import diskCacheV111.pools.*;

import org.dcache.poolmanager.*;
import org.dcache.pinmanager.model.*;
import static org.dcache.pinmanager.model.Pin.State.*;
import org.dcache.cells.*;
import org.dcache.vehicles.*;
import dmg.cells.nucleus.*;
import dmg.util.*;

import com.google.common.base.Predicate;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

public class PinManagerTests
{
    final static ProtocolInfo PROTOCOL_INFO =
        new DCapProtocolInfo("DCap", 3, 0, "localhost", 0);
    final static StorageInfo STORAGE_INFO =
        new GenericStorageInfo();

    final static PnfsId PNFS_ID1 =
        new PnfsId("0000D4CF1C3302B44095969C8216CE1E9175");
    final static PnfsId PNFS_ID2 =
        new PnfsId("00009C09780FEE044CC18940B335B2AE93E6");
    final static PnfsId PNFS_ID3 =
        new PnfsId("00003113FDF3F9284EE8992FC9F651082956");

    final static String REQUEST_ID1 = "request1";

    final static String POOL1 = "pool1";

    final static String STICKY1 = "PinManager-1";

    final static Pin PIN1 =
        new TestPin(0, 0, 0, REQUEST_ID1,
                    new Date(), new Date(now() + 30),
                    PNFS_ID1, POOL1, STICKY1, PINNED);

    private FileAttributes getAttributes(PnfsId pnfsId)
    {
        FileAttributes attributes = new FileAttributes();
        attributes.setPnfsId(pnfsId);
        attributes.setStorageInfo(STORAGE_INFO);
        attributes.setLocations(Collections.singleton(POOL1));
        return attributes;
    }

    @Test
    public void testPinning()
        throws CacheException, InterruptedException, ExecutionException
    {
        TestDao dao = new TestDao();

        PinRequestProcessor processor = new PinRequestProcessor();
        processor.setExecutor(new TestExecutor());
        processor.setDao(dao);
        processor.setPoolStub(new TestStub() {
                public PoolSetStickyMessage messageArrived(PoolSetStickyMessage msg)
                {
                    return msg;
                }
            });
        processor.setPoolManagerStub(new TestStub() {
                public PoolMgrSelectReadPoolMsg messageArrived(PoolMgrSelectReadPoolMsg msg)
                {
                    msg.setPoolName(POOL1);
                    return msg;
                }

                public PoolManagerGetPoolMonitor messageArrived(PoolManagerGetPoolMonitor msg)
                {
                    msg.setPoolMonitor(new PoolMonitorV5() {
                            public PnfsFileLocation getPnfsFileLocation(FileAttributes fileAttributes,
                                                                        ProtocolInfo protocolInfo,
                                                                        String linkGroup)
                            {
                                return new PoolMonitorV5.PnfsFileLocation(fileAttributes, protocolInfo, linkGroup) {
                                    public PoolInfo selectPinPool()
                                    {
                                        return new PoolInfo(new PoolCostInfo(POOL1),
                                                            ImmutableMap.<String,String>of());
                                    }
                                };
                            }
                        });
                    return msg;
                }
            });
        processor.setMaxLifetime(-1);
        processor.setStagePermission(new CheckStagePermission(null));
        processor.init();

        Date expiration = new Date(now() + 30);
        PinManagerPinMessage message =
            new PinManagerPinMessage(getAttributes(PNFS_ID1),
                                     PROTOCOL_INFO, REQUEST_ID1, 30);
        Date start = new Date();
        message = processor.messageArrived(message).get();
        Date stop = new Date();

        assertEquals(0, message.getReturnCode());
        assertFalse(message.getExpirationTime().before(expiration));

        Pin pin = dao.getPin(message.getPinId());
        assertEquals(PNFS_ID1, pin.getPnfsId());
        assertBetween(start, stop, pin.getCreationTime());
        assertEquals(message.getExpirationTime(), pin.getExpirationTime());
        assertEquals(0, pin.getUid());
        assertEquals(0, pin.getGid());
        assertEquals(REQUEST_ID1, pin.getRequestId());
        assertEquals(POOL1, pin.getPool());
        assertEquals(PINNED, pin.getState());
        assertValidSticky(pin.getSticky());
    }

    @Test
    public void testExtendLifetime()
        throws CacheException, InterruptedException, ExecutionException
    {
        TestDao dao = new TestDao();
        Pin pin = dao.storePin(PIN1);

        MovePinRequestProcessor processor = new MovePinRequestProcessor();
        processor.setDao(dao);
        processor.setPoolStub(new TestStub() {
                public PoolSetStickyMessage messageArrived(PoolSetStickyMessage msg)
                {
                    return msg;
                }
            });
        processor.setAuthorizationPolicy(new DefaultAuthorizationPolicy());
        processor.setMaxLifetime(-1);

        Date expiration = new Date(now() + 60);
        PinManagerExtendPinMessage message =
            new PinManagerExtendPinMessage(getAttributes(PNFS_ID1),
                                           pin.getPinId(), 60);
        message = processor.messageArrived(message);

        assertEquals(0, message.getReturnCode());
        assertFalse(message.getExpirationTime().before(expiration));

        Pin newPin = dao.getPin(pin.getPinId());
        assertEquals(PNFS_ID1, newPin.getPnfsId());
        assertEquals(pin.getCreationTime(), newPin.getCreationTime());
        assertEquals(message.getExpirationTime(), newPin.getExpirationTime());
        assertEquals(pin.getUid(), newPin.getUid());
        assertEquals(pin.getGid(), newPin.getGid());
        assertEquals(pin.getRequestId(), newPin.getRequestId());
        assertEquals(pin.getPool(), newPin.getPool());
        assertEquals(pin.getState(), newPin.getState());
        assertValidSticky(newPin.getSticky());
    }

    @Test
    public void testUnpinningByPinId()
        throws CacheException, InterruptedException, ExecutionException
    {
        TestDao dao = new TestDao();
        Pin pin = dao.storePin(PIN1);

        UnpinRequestProcessor processor = new UnpinRequestProcessor();
        processor.setDao(dao);
        processor.setAuthorizationPolicy(new DefaultAuthorizationPolicy());

        PinManagerUnpinMessage message =
            new PinManagerUnpinMessage(PNFS_ID1);
        message.setPinId(pin.getPinId());
        message = processor.messageArrived(message);

        assertEquals(0, message.getReturnCode());
        assertEquals(pin.getPinId(), message.getPinId());
        assertEquals(pin.getRequestId(), message.getRequestId());

        Pin newPin = dao.getPin(pin.getPinId());
        assertEquals(PNFS_ID1, newPin.getPnfsId());
        assertEquals(pin.getPool(), newPin.getPool());
        assertEquals(UNPINNING, newPin.getState());
        assertEquals(pin.getSticky(), newPin.getSticky());
    }

    @Test
    public void testUnpinningByRequestId()
        throws CacheException, InterruptedException, ExecutionException
    {
        TestDao dao = new TestDao();
        Pin pin = dao.storePin(PIN1);

        UnpinRequestProcessor processor = new UnpinRequestProcessor();
        processor.setDao(dao);
        processor.setAuthorizationPolicy(new DefaultAuthorizationPolicy());

        PinManagerUnpinMessage message =
            new PinManagerUnpinMessage(PNFS_ID1);
        message.setRequestId(pin.getRequestId());
        message = processor.messageArrived(message);

        assertEquals(0, message.getReturnCode());
        assertEquals(pin.getPinId(), message.getPinId());
        assertEquals(pin.getRequestId(), message.getRequestId());

        Pin newPin = dao.getPin(pin.getPinId());
        assertEquals(PNFS_ID1, newPin.getPnfsId());
        assertEquals(pin.getPool(), newPin.getPool());
        assertEquals(UNPINNING, newPin.getState());
        assertEquals(pin.getSticky(), newPin.getSticky());
    }

    <T extends Comparable<T>> void assertBetween(T lower, T upper, T actual)
    {
        String message =
            String.format("Expected between <%s> and <%s> but was <%s>",
                          lower, upper, actual);
        assertTrue(message, lower.compareTo(actual) <= 0);
        assertTrue(message, upper.compareTo(actual) >= 0);
    }

    void assertValidSticky(String sticky)
    {
        String message =
            String.format("Expected sticky to start with PinManager but was <%s>",
                          sticky);
        assertTrue(message, sticky.startsWith("PinManager"));
    }

    static long now()
    {
        return System.currentTimeMillis();
    }
}

class TestPin extends Pin
{
    public TestPin(long id, Pin pin)
    {
        this(id, pin.getUid(), pin.getGid(), pin.getRequestId(),
             pin.getCreationTime(), pin.getExpirationTime(),
             pin.getPnfsId(), pin.getPool(), pin.getSticky(), pin.getState());
    }

    public TestPin(long id, long uid, long gid, String requestId,
                   Date creationTime, Date expirationTime,
                   PnfsId pnfsId, String pool, String sticky,
                   Pin.State state)
    {
        _id = id;
        _uid = uid;
        _gid = gid;
        _requestId = requestId;
        _creationTime = creationTime;
        _expirationTime = expirationTime;
        _pnfsId = (pnfsId == null) ? null : pnfsId.toString();
        _pool = pool;
        _sticky = sticky;
        _state = state;
    }
}

class TestDao implements PinDao
{
    long _counter = 0;
    Map<Long,TestPin> _pins = new HashMap<Long,TestPin>();

    protected TestPin clone(Pin pin)
    {
        return (pin == null) ? null : new TestPin(pin.getPinId(), pin);
    }

    @Override
    public Pin storePin(Pin pin)
    {
        long id = (pin.getPinId() == 0) ? ++_counter : pin.getPinId();
        TestPin testPin = new TestPin(id, pin);
        _pins.put(id, testPin);
        return clone(testPin);
    }

    @Override
    public Pin getPin(long id)
    {
        return clone(_pins.get(id));
    }

    @Override
    public Pin getPin(PnfsId pnfsId, long id)
    {
        Pin pin = _pins.get(id);
        return (pin != null && pnfsId.equals(pin.getPnfsId())) ? clone(pin) : null;
    }

    @Override
    public Pin getPin(PnfsId pnfsId, String requestId)
    {
        for (Pin pin: _pins.values()) {
            if (pnfsId.equals(pin.getPnfsId()) && requestId.equals(pin.getRequestId())) {
                return clone(pin);
            }
        }
        return null;
    }

    @Override
    public Pin getPin(long id, String sticky, Pin.State state)
    {
        Pin pin = _pins.get(id);
        return (pin != null &&
                Objects.equal(sticky, pin.getSticky()) &&
                state == pin.getState()) ? clone(pin) : null;
    }

    @Override
    public void deletePins(String[] pnfsIds)
    {
        for (String s: pnfsIds) {
            PnfsId pnfsId = new PnfsId(s);
            Iterator<TestPin> i = _pins.values().iterator();
            while (i.hasNext()) {
                if (pnfsId.equals(i.next().getPnfsId())) {
                    i.remove();
                }
            }
        }
    }

    @Override
    public void deletePin(Pin pin)
    {
        _pins.remove(pin.getPinId());
    }

    @Override
    public Collection<Pin> getPins()
    {
        ArrayList<Pin> pins = new ArrayList();
        for (Pin pin: _pins.values()) {
            pins.add(clone(pin));
        }
        return pins;
    }

    @Override
    public Collection<Pin> getPins(PnfsId pnfsId)
    {
        ArrayList<Pin> pins = new ArrayList();
        for (Pin pin: _pins.values()) {
            if (pnfsId.equals(pin.getPnfsId())) {
                pins.add(clone(pin));
            }
        }
        return pins;
    }

    @Override
    public Collection<Pin> getPins(Pin.State state)
    {
        ArrayList<Pin> pins = new ArrayList();
        for (Pin pin: _pins.values()) {
            if (state == pin.getState()) {
                pins.add(clone(pin));
            }
        }
        return pins;
    }

    @Override
    public boolean all(Pin.State state, Predicate<Pin> f)
    {
        for (Pin pin: _pins.values()) {
            if (state == pin.getState()) {
                if (!f.apply(pin)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Collection<Pin> getPins(PnfsId pnfsId, String pool)
    {
        ArrayList<Pin> pins = new ArrayList();
        for (Pin pin: _pins.values()) {
            if (pnfsId.equals(pin.getPnfsId()) && pool.equals(pin.getPool())) {
                pins.add(clone(pin));
            }
        }
        return pins;
    }

    @Override
    public void expirePins()
    {
        Date now = new Date();
        for (Pin pin: _pins.values()) {
            if (pin.getExpirationTime() != null && pin.getExpirationTime().before(now)) {
                pin.setState(UNPINNING);
            }
        }
    }

    @Override
    public boolean hasSharedSticky(Pin aPin)
    {
        for (Pin pin: _pins.values()) {
            if (Objects.equal(pin.getPnfsId(), aPin.getPnfsId()) && pin.getPinId() != aPin.getPinId() && Objects.equal(pin.getPool(), aPin.getPool()) && Objects.equal(pin.getSticky(), aPin.getSticky()) && pin.getState() != UNPINNING) {
                return true;
            }
        }
        return false;
    }
}

class TestExecutor
    extends AbstractExecutorService implements ScheduledExecutorService
{
    @Override
    public void execute(Runnable runnable)
    {
        runnable.run();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
    {
        return false;
    }

    @Override
    public boolean isShutdown()
    {
        return false;
    }

    @Override
    public boolean isTerminated()
    {
        return false;
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        return Collections.emptyList();
    }

    @Override
    public <V> ScheduledFuture<V>
	schedule(Callable<V> callable, long delay, TimeUnit unit)
    {
        return new ImmediateScheduledFuture(submit(callable));
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
    {
        return new ImmediateScheduledFuture(submit(command));
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
    {
        return new ImmediateScheduledFuture(submit(command));
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
    {
        return new ImmediateScheduledFuture(submit(command));
    }

    class ImmediateScheduledFuture<T> implements ScheduledFuture<T>
    {
        private Future<T> _inner;

        public ImmediateScheduledFuture(Future<T> future) {
            _inner = future;
        }

        @Override
        public long getDelay(TimeUnit unit)
        {
            return 0;
        }

        @Override
        public int compareTo(Delayed o)
        {
            return (int) Long.signum(getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return _inner.cancel(mayInterruptIfRunning);
        }

        @Override
        public T get()
            throws InterruptedException,ExecutionException
        {
            return _inner.get();
        }

        @Override
        public T get(long timeout, TimeUnit unit)
            throws InterruptedException,ExecutionException, TimeoutException
        {
            return _inner.get(timeout, unit);
        }

        @Override
        public boolean isCancelled()
        {
            return _inner.isCancelled();
        }

        @Override
        public boolean isDone()
        {
            return _inner.isDone();
        }
    }
}

class TestEndpoint implements CellEndpoint
{
    protected CellMessageDispatcher _dispatcher =
        new CellMessageDispatcher("messageArrived");

    public TestEndpoint()
    {
        _dispatcher.addMessageListener(this);
    }

    public TestEndpoint(Object o)
    {
        this();
        _dispatcher.addMessageListener(o);
    }

    protected CellMessage process(CellMessage envelope)
    {
        Object result = _dispatcher.call(envelope);
        if (result == null) {
            return null;
        }

        Object o = envelope.getMessageObject();
        if (o instanceof Message) {
            Message msg = (Message)o;

            /* dCache vehicles can transport errors back to the
             * requestor, so detect if this is an error reply.
             */
            if (result instanceof CacheException) {
                CacheException e = (CacheException)result;
                msg.setFailed(e.getRc(), e.getMessage());
                result = msg;
            } else if (result instanceof IllegalArgumentException) {
                msg.setFailed(CacheException.INVALID_ARGS,
                              result.toString());
                result = msg;
            } else if (result instanceof Exception) {
                msg.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                              result);
                result = msg;
            }
        }

        envelope.revertDirection();
        envelope.setMessageObject(result);
        return envelope;
    }

    @Override
    public void sendMessage(CellMessage envelope)
        throws SerializationException,
               NoRouteToCellException
    {
        process(envelope);
    }

    @Override
    public void sendMessage(CellMessage envelope,
                            CellMessageAnswerable callback,
                            long timeout)
        throws SerializationException
    {
        CellMessage answer = process(envelope);
        if (answer != null) {
            callback.answerArrived(envelope, answer);
        } else {
            callback.answerTimedOut(envelope);
        }
    }

    @Override
    public CellMessage sendAndWait(CellMessage envelope, long timeout)
        throws SerializationException,
               NoRouteToCellException,
               InterruptedException
    {
        return process(envelope);
    }

    @Override
    public CellMessage sendAndWaitToPermanent(CellMessage envelope, long timeout)
        throws SerializationException,
               InterruptedException
    {
        return process(envelope);
    }

    @Override
    public CellInfo getCellInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String,Object> getDomainContext()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Args getArgs()
    {
        throw new UnsupportedOperationException();
    }
}

class TestStub extends CellStub
{
    public TestStub()
    {
        setDestination("dummy");
        setCellEndpoint(new TestEndpoint(this));
    }
}
