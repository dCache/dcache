package org.dcache.pinmanager;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.security.auth.Subject;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;

import diskCacheV111.poolManager.Pool;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.SerializationException;

import org.dcache.auth.Subjects;
import org.dcache.cells.CellMessageDispatcher;
import org.dcache.cells.CellStub;
import org.dcache.pinmanager.model.Pin;
import org.dcache.pool.classic.IoQueueManager;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.vehicles.FileAttributes;

import static java.util.stream.Collectors.toList;
import static org.dcache.pinmanager.model.Pin.State.PINNED;
import static org.dcache.pinmanager.model.Pin.State.UNPINNING;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PinManagerTests
{
    final static ProtocolInfo PROTOCOL_INFO =
            new DCapProtocolInfo("DCap", 3, 0,
            new InetSocketAddress("127.0.0.1", 17));
    final static StorageInfo STORAGE_INFO =
        new GenericStorageInfo("osm", "default");

    final static PnfsId PNFS_ID1 =
        new PnfsId("0000D4CF1C3302B44095969C8216CE1E9175");
    final static PnfsId PNFS_ID2 =
        new PnfsId("00009C09780FEE044CC18940B335B2AE93E6");
    final static PnfsId PNFS_ID3 =
        new PnfsId("00003113FDF3F9284EE8992FC9F651082956");

    final static String REQUEST_ID1 = "request1";

    final static String POOL1 = "pool1";

    final static String STICKY1 = "PinManager-1";

    private FileAttributes getAttributes(PnfsId pnfsId)
    {
        FileAttributes attributes = new FileAttributes();
        attributes.setPnfsId(pnfsId);
        attributes.setLocations(Collections.singleton(POOL1));
        StorageInfos.injectInto(STORAGE_INFO, attributes);

        // Required attributes, but the values are not relevant
        // when pool manager and pool are stubbed out.
        attributes.setSize(0L);
        attributes.setAccessLatency(StorageInfo.DEFAULT_ACCESS_LATENCY);
        attributes.setRetentionPolicy(StorageInfo.DEFAULT_RETENTION_POLICY);
        attributes.setChecksums(Collections.emptySet());
        attributes.setCacheClass(null);
        attributes.setHsm("osm");
        attributes.setFlags(Collections.emptyMap());
        return attributes;
    }

    @Test
    public void testPinning()
        throws CacheException, InterruptedException, ExecutionException
    {
        TestDao dao = new TestDao();

        PinRequestProcessor processor = new PinRequestProcessor();
        processor.setScheduledExecutor(new TestExecutor());
        processor.setExecutor(MoreExecutors.directExecutor());
        processor.setDao(dao);
        processor.setPoolStub(new TestStub(new CellAddressCore("PinManager")) {
                public PoolSetStickyMessage messageArrived(PoolSetStickyMessage msg)
                {
                    return msg;
                }
            });
        processor.setPoolManagerStub(new TestStub(new CellAddressCore("PinManager")) {
                public PoolMgrSelectReadPoolMsg messageArrived(PoolMgrSelectReadPoolMsg msg)
                {
                    msg.setPoolName(POOL1);
                    msg.setPoolAddress(new CellAddressCore(POOL1));
                    return msg;
                }
            });
        processor.setMaxLifetime(-1);
        processor.setStagePermission(new CheckStagePermission(null));
        processor.setPoolMonitor(new PoolMonitorV5() {
            @Override
            public PoolSelector getPoolSelector(FileAttributes fileAttributes,
                                                ProtocolInfo protocolInfo,
                                                String linkGroup)
            {
                return new PoolMonitorV5.PnfsFileLocation(fileAttributes, protocolInfo, linkGroup) {
                    @Override
                    public PoolInfo selectPinPool()
                    {
                        return new PoolInfo(
                                new CellAddressCore(POOL1),
                                new PoolCostInfo(POOL1, IoQueueManager.DEFAULT_QUEUE),
                                ImmutableMap.<String,String>of());
                    }
                };
            }
        });

        Date expiration = new Date(now() + 30);
        PinManagerPinMessage message =
            new PinManagerPinMessage(getAttributes(PNFS_ID1),
                                     PROTOCOL_INFO, REQUEST_ID1, 30);
        Date start = new Date();
        message = processor.messageArrived(message).get();
        Date stop = new Date();

        assertEquals(0, message.getReturnCode());
        assertFalse(message.getExpirationTime().before(expiration));

        Pin pin = dao.get(dao.where().id(message.getPinId()));
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
        Pin pin = dao.create(dao.set()
                                     .subject(Subjects.ROOT)
                                     .requestId(REQUEST_ID1)
                                     .expirationTime(new Date(now() + 30))
                                     .pnfsId(PNFS_ID1)
                                     .pool(POOL1)
                                     .sticky(STICKY1)
                                     .state(PINNED));

        Pool pool = new Pool(POOL1);
        pool.setActive(true);
        pool.setAddress(new CellAddressCore(POOL1));
        PoolMonitor poolMonitor = mock(PoolMonitor.class, RETURNS_DEEP_STUBS);
        when(poolMonitor.getPoolSelectionUnit().getPool(POOL1)).thenReturn(pool);

        MovePinRequestProcessor processor = new MovePinRequestProcessor();
        processor.setDao(dao);
        processor.setPoolStub(new TestStub(new CellAddressCore("PinManager")) {
                public PoolSetStickyMessage messageArrived(PoolSetStickyMessage msg)
                {
                    return msg;
                }
            });
        processor.setAuthorizationPolicy(new DefaultAuthorizationPolicy());
        processor.setMaxLifetime(-1);
        processor.setPoolMonitor(poolMonitor);

        Date expiration = new Date(now() + 60);
        PinManagerExtendPinMessage message =
            new PinManagerExtendPinMessage(getAttributes(PNFS_ID1),
                                           pin.getPinId(), 60);
        message = processor.messageArrived(message);

        assertEquals(0, message.getReturnCode());
        assertFalse(message.getExpirationTime().before(expiration));

        Pin newPin = dao.get(dao.where().id(pin.getPinId()));
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
        Pin pin = dao.create(dao.set()
                                     .subject(Subjects.ROOT)
                                     .requestId(REQUEST_ID1)
                                     .expirationTime(new Date(now() + 30))
                                     .pnfsId(PNFS_ID1)
                                     .pool(POOL1)
                                     .sticky(STICKY1)
                                     .state(PINNED));

        UnpinRequestProcessor processor = new UnpinRequestProcessor();
        processor.setDao(dao);
        processor.setAuthorizationPolicy(new DefaultAuthorizationPolicy());

        PinManagerUnpinMessage message =
            new PinManagerUnpinMessage(PNFS_ID1);
        message.setPinId(pin.getPinId());
        message = processor.messageArrived(message);

        assertEquals(0, message.getReturnCode());
        assertEquals(pin.getPinId(), (long) message.getPinId());
        assertEquals(pin.getRequestId(), message.getRequestId());

        Pin newPin = dao.get(dao.where().id(pin.getPinId()));
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
        Pin pin = dao.create(dao.set()
                                     .subject(Subjects.ROOT)
                                     .requestId(REQUEST_ID1)
                                     .expirationTime(new Date(now() + 30))
                                     .pnfsId(PNFS_ID1)
                                     .pool(POOL1)
                                     .sticky(STICKY1)
                                     .state(PINNED));

        UnpinRequestProcessor processor = new UnpinRequestProcessor();
        processor.setDao(dao);
        processor.setAuthorizationPolicy(new DefaultAuthorizationPolicy());

        PinManagerUnpinMessage message =
            new PinManagerUnpinMessage(PNFS_ID1);
        message.setRequestId(pin.getRequestId());
        message = processor.messageArrived(message);

        assertEquals(0, message.getReturnCode());
        assertEquals(pin.getPinId(), (long) message.getPinId());
        assertEquals(pin.getRequestId(), message.getRequestId());

        Pin newPin = dao.get(dao.where().id(pin.getPinId()));
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

@ParametersAreNonnullByDefault
class TestDao implements PinDao
{
    long _counter;
    Map<Long,Pin> _pins = new HashMap<>();

    @Override
    public PinCriterion where()
    {
        return new TestCriterion();
    }

    @Override
    public PinUpdate set()
    {
        return new TestUpdate();
    }

    @Override
    public Pin create(PinUpdate update)
    {
        Pin pin = ((TestUpdate) update).createPin(_counter++);
        _pins.put(pin.getPinId(), pin);
        return pin;
    }

    @Override
    public List<Pin> get(PinCriterion criterion)
    {
        return _pins.values().stream().filter(((TestCriterion) criterion)::matches).collect(toList());
    }

    @Override
    public List<Pin> get(PinCriterion criterion, int limit)
    {
        return _pins.values().stream().filter(((TestCriterion) criterion)::matches).limit(limit).collect(toList());
    }

    @Override
    public Pin get(UniquePinCriterion criterion)
    {
        return _pins.values().stream().filter(((TestCriterion) criterion)::matches).findFirst().orElse(null);
    }

    @Override
    public int count(PinCriterion criterion)
    {
        return (int) _pins.values().stream().filter(((TestCriterion) criterion)::matches).count();
    }

    @Override
    public Pin update(UniquePinCriterion criterion, PinUpdate update)
    {
        Pin pin = get(criterion);
        if (pin == null) {
            return null;
        }
        update((PinCriterion) criterion, update);
        return ((TestUpdate) update).apply(pin);
    }

    @Override
    public int update(PinCriterion criterion, PinUpdate update)
    {
        TestUpdate u = (TestUpdate) update;
        int cnt = 0;
        for (Map.Entry<Long, Pin> e : _pins.entrySet()) {
            if (((TestCriterion) criterion).matches(e.getValue())) {
                cnt++;
                e.setValue(u.apply(e.getValue()));
            }
        }
        return cnt;
    }

    @Override
    public int delete(PinCriterion criterion)
    {
        int cnt = 0;
        Iterator<Pin> iterator = _pins.values().iterator();
        while (iterator.hasNext()) {
            Pin pin = iterator.next();
            if (((TestCriterion) criterion).matches(pin)) {
                iterator.remove();
                cnt++;
            }
        }
        return cnt;
    }

    @Override
    public void foreach(PinCriterion criterion, InterruptibleConsumer<Pin> f) throws InterruptedException
    {
        for (Pin pin : _pins.values()) {
            if (((TestCriterion) criterion).matches(pin)) {
                f.accept(pin);
            }
        }
    }

    private static class TestCriterion implements UniquePinCriterion
    {
        private final List<Predicate<Pin>> predicates = new ArrayList<>();
        private Long id;
        private PnfsId pnfsId;
        private String requestId;

        protected TestCriterion add(Predicate<Pin> predicate)
        {
            predicates.add(predicate);
            return this;
        }

        @Override
        public TestCriterion id(long id)
        {
            this.id = id;
            return add(p -> p.getPinId() == id);
        }

        @Override
        public TestCriterion pnfsId(PnfsId id)
        {
            pnfsId = id;
            return add(p -> Objects.equals(p.getPnfsId(), id));
        }

        @Override
        public TestCriterion requestId(String requestId)
        {
            this.requestId = requestId;
            return add(p -> Objects.equals(p.getRequestId(), requestId));
        }

        @Override
        public TestCriterion expirationTimeBefore(Date date)
        {
            return add(p -> p.getExpirationTime().before(date));
        }

        @Override
        public TestCriterion state(Pin.State state)
        {
            return add(p -> p.getState() == state);
        }

        @Override
        public TestCriterion stateIsNot(Pin.State state)
        {
            return add(p -> p.getState() != state);
        }

        @Override
        public TestCriterion pool(String pool)
        {
            return add(p -> Objects.equals(p.getPool(), pool));
        }

        @Override
        public TestCriterion sticky(String sticky)
        {
            return add(p -> Objects.equals(p.getSticky(), sticky));
        }

        @Override
        public TestCriterion sameIdAs(UniquePinCriterion criterion)
        {
            TestCriterion c = (TestCriterion) criterion;
            if (c.id != null) {
                return id(c.id);
            } else {
                return pnfsId(c.pnfsId).requestId(c.requestId);
            }
        }

        boolean matches(Pin pin)
        {
            return predicates.stream().allMatch(p -> p.test(pin));
        }
    }

    private static class TestUpdate implements PinUpdate
    {
        private final List<Function<Pin, Pin>> updates = new ArrayList<>();

        private PinUpdate add(Function<Pin,Pin> update)
        {
            updates.add(update);
            return this;
        }

        @Override
        public PinUpdate expirationTime(Date date)
        {
            return add(p -> new Pin(p.getPinId(), p.getPnfsId(), p.getRequestId(), p.getCreationTime(), date,
                                    p.getUid(), p.getGid(), p.getState(), p.getPool(), p.getSticky()));

        }

        @Override
        public PinUpdate pool(String pool)
        {
            return add(p -> new Pin(p.getPinId(), p.getPnfsId(), p.getRequestId(), p.getCreationTime(),
                                    p.getExpirationTime(), p.getUid(), p.getGid(), p.getState(),
                                    pool, p.getSticky()));
        }

        @Override
        public PinUpdate requestId(String requestId)
        {
            return add(p -> new Pin(p.getPinId(), p.getPnfsId(), requestId, p.getCreationTime(),
                                    p.getExpirationTime(), p.getUid(), p.getGid(), p.getState(),
                                    p.getPool(), p.getSticky()));
        }

        @Override
        public PinUpdate state(Pin.State state)
        {
            return add(p -> new Pin(p.getPinId(), p.getPnfsId(), p.getRequestId(), p.getCreationTime(),
                                    p.getExpirationTime(), p.getUid(), p.getGid(), state,
                                    p.getPool(), p.getSticky()));
        }

        @Override
        public PinUpdate sticky(String sticky)
        {
            return add(p -> new Pin(p.getPinId(), p.getPnfsId(), p.getRequestId(), p.getCreationTime(),
                                    p.getExpirationTime(), p.getUid(), p.getGid(), p.getState(),
                                    p.getPool(), sticky));
        }

        @Override
        public PinUpdate subject(Subject subject)
        {
            return add(p -> {
                long uid = Subjects.getUid(subject);
                long gid = Subjects.getPrimaryGid(subject);
                return new Pin(p.getPinId(), p.getPnfsId(), p.getRequestId(), p.getCreationTime(),
                               p.getExpirationTime(), uid, gid, p.getState(),
                               p.getPool(), p.getSticky());
            });
        }

        @Override
        public PinUpdate pnfsId(PnfsId pnfsId)
        {
            return add(p -> new Pin(p.getPinId(), pnfsId, p.getRequestId(), p.getCreationTime(),
                                    p.getExpirationTime(), p.getUid(), p.getGid(), p.getState(),
                                    p.getPool(), p.getSticky()));
        }

        Pin apply(Pin pin)
        {
            for (Function<Pin,Pin> u : updates) {
                pin = u.apply(pin);
            }
            return pin;
        }

        public Pin createPin(long id)
        {
            return apply(new Pin(id));
        }
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
            return Longs.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
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

class TestEndpoint implements CellEndpoint, CellMessageReceiver
{
    private final CellAddressCore _address;
    protected CellMessageDispatcher _dispatcher =
        new CellMessageDispatcher("messageArrived");

    public TestEndpoint(CellAddressCore address)
    {
        _address = address;
        _dispatcher.addMessageListener(this);
    }

    public TestEndpoint(CellAddressCore address, CellMessageReceiver o)
    {
        this(address);
        _dispatcher.addMessageListener(o);
    }

    protected CellMessage process(CellMessage envelope)
    {
        Object result = _dispatcher.call(envelope);
        if (result == null) {
            return null;
        }

        Serializable o = envelope.getMessageObject();
        if (o instanceof Message) {
            Message msg = (Message)o;

            /* dCache vehicles can transport errors back to the
             * requester, so detect if this is an error reply.
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
                        (Exception) result);
                result = msg;
            }
        }

        envelope.revertDirection();
        envelope.setMessageObject((Serializable) result);
        return envelope;
    }

    @Override
    public void sendMessage(CellMessage envelope)
        throws SerializationException
    {
        envelope.addSourceAddress(_address);
        process(envelope);
    }

    @Override
    public void sendMessage(CellMessage envelope,
                            CellMessageAnswerable callback,
                            Executor executor,
                            long timeout)
        throws SerializationException
    {
        envelope.addSourceAddress(_address);
        CellMessage answer = process(envelope);
        if (answer != null) {
            callback.answerArrived(envelope, answer);
        } else {
            callback.answerTimedOut(envelope);
        }
    }

    @Override
    public void sendMessageWithRetryOnNoRouteToCell(CellMessage envelope, CellMessageAnswerable callback,
                                                    Executor executor, long timeout)
            throws SerializationException
    {
        sendMessage(envelope, callback, executor, timeout);
    }

    @Override
    public Map<String,Object> getDomainContext()
    {
        throw new UnsupportedOperationException();
    }
}

class TestStub extends CellStub implements CellMessageReceiver
{
    public TestStub(CellAddressCore address)
    {
        setDestination("dummy");
        setCellEndpoint(new TestEndpoint(address, this));
    }
}
