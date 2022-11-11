package org.dcache.qos.util;

import static org.dcache.mock.CacheEntryBuilder.aCacheEntry;
import static org.dcache.pool.repository.StickyRecord.NON_EXPIRING;
import static org.dcache.qos.util.MessageGuardTest.TEST_PNFSID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageReply;
import org.dcache.pool.qos.QoSMessageHandler;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.vehicles.qos.ReplicaStatusMessage;
import org.junit.Before;
import org.junit.Test;

public class RepositoryReplicaVerifierTest {

    static final PnfsId ALT_REPLICA = new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD01");

    class QoSMessageHandlerBuilder {

        private final CellAddressCore address;

        private PnfsId replica;
        private ReplicaState state;
        private String stickyOwner;
        private long stickyDuration;

        QoSMessageHandlerBuilder(String address) {
            this.address = new CellAddressCore(address);
        }

        QoSMessageHandler build() throws Exception {
            QoSMessageHandler handler = new QoSMessageHandler();
            handler.setExecutionService(service);
            handler.setRepository(buildRepository());
            return handler;
        }

        QoSMessageHandlerBuilder thatHasAReplicaOf(PnfsId replica) {
            this.replica = replica;
            return this;
        }

        QoSMessageHandlerBuilder withState(ReplicaState state) {
            this.state = state;
            return this;
        }

        QoSMessageHandlerBuilder withStickyBitOwnerAndDuration(String stickyOwner,
              long stickyDuration) {
            this.stickyOwner = stickyOwner;
            this.stickyDuration = stickyDuration;
            return this;
        }

        private Repository buildRepository() throws CacheException, InterruptedException {
            Repository repository = mock(Repository.class);
            if (replica == null || replica.equals(ALT_REPLICA)) {
                when(repository.getEntry(TEST_PNFSID)).thenThrow(FileNotInCacheException.class);
            } else {
                when(repository.getEntry(TEST_PNFSID)).thenReturn(aCacheEntryFor(replica));
            }
            return repository;
        }

        private CacheEntry aCacheEntryFor(PnfsId id) throws CacheException {
            if (stickyOwner != null) {
                return aCacheEntry().withPnfsId(id).withState(state)
                      .withStickyRecord(stickyOwner, stickyDuration).build();
            }
            return aCacheEntry().withPnfsId(id).withState(state).build();
        }
    }

    class TestStub extends CellStub {

        @Override
        public <T> ListenableFuture<T> send(
              CellPath destination, Serializable message, Class<T> type,
              CellEndpoint.SendFlag... flags) {
            if (message instanceof ReplicaStatusMessage) {
                try {
                    return getResponse(((ReplicaStatusMessage) message).getPool());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            assertFalse("Test does not support this message type: " + type, true);
            return null; // compiler placeholder
        }
    }

    Map<String, QoSMessageHandlerBuilder> pools;
    Map<String, QoSMessageHandler> handlers;
    Collection<ReplicaStatusMessage> messages;
    ExecutorService service;
    CellStub stub;

    int count;
    String address;

    @Before
    public void setup() {
        pools = new HashMap<>();
        handlers = new HashMap<>();
        stub = new TestStub();
        service = Executors.newSingleThreadExecutor();
    }

    @Test
    public void shouldVerifyNoReplicasCorrectly() throws Exception {
        whenVerifyIsCalled();
        verifyThat(0).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(0).replicasAreReadable();
        verifyThat(0).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(0).replicasAreSticky();
    }


    @Test
    public void shouldVerifyFromClientCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.FROM_CLIENT).withStickyBitOwnerAndDuration("testOwner",
                    TimeUnit.DAYS.toMillis(1));
        whenVerifyIsCalled();
        verifyThat(1).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(0).replicasAreReadable();
        verifyThat(0).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(0).replicasAreSticky();
    }

    @Test
    public void shouldVerifyFromPoolCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.FROM_POOL).withStickyBitOwnerAndDuration("testOwner",
                    TimeUnit.DAYS.toMillis(1));
        whenVerifyIsCalled();
        verifyThat(1).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(0).replicasAreReadable();
        verifyThat(0).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(0).replicasAreSticky();
    }

    @Test
    public void shouldVerifyFromStoreCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.FROM_STORE).withStickyBitOwnerAndDuration("testOwner",
                    TimeUnit.DAYS.toMillis(1));
        whenVerifyIsCalled();
        verifyThat(1).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(0).replicasAreReadable();
        verifyThat(0).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(0).replicasAreSticky();
    }

    @Test
    public void shouldVerifyNewCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.NEW).withStickyBitOwnerAndDuration("testOwner",
                    TimeUnit.DAYS.toMillis(1));
        whenVerifyIsCalled();
        verifyThat(1).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(0).replicasAreReadable();
        verifyThat(0).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(0).replicasAreSticky();
    }

    @Test
    public void shouldVerifyBrokenCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.BROKEN).withStickyBitOwnerAndDuration("testOwner",
                    TimeUnit.DAYS.toMillis(1));
        whenVerifyIsCalled();
        verifyThat(1).replicasExists();
        verifyThat(1).replicasAreBroken();
        verifyThat(0).replicasAreReadable();
        verifyThat(1).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(0).replicasAreSticky();
    }

    @Test
    public void shouldVerifyCachedCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED).withStickyBitOwnerAndDuration("testOwner",
                    TimeUnit.DAYS.toMillis(1));
        whenVerifyIsCalled();
        verifyThat(1).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(1).replicasAreReadable();
        verifyThat(1).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(0).replicasAreSticky();
    }

    @Test
    public void shouldVerifyRemovedCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.REMOVED).withStickyBitOwnerAndDuration("testOwner",
                    TimeUnit.DAYS.toMillis(1));
        whenVerifyIsCalled();
        verifyThat(1).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(0).replicasAreReadable();
        verifyThat(0).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(0).replicasAreSticky();
    }

    @Test
    public void shouldVerifyDestroyedCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.DESTROYED).withStickyBitOwnerAndDuration("testOwner",
                    TimeUnit.DAYS.toMillis(1));
        whenVerifyIsCalled();
        verifyThat(1).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(0).replicasAreReadable();
        verifyThat(0).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(0).replicasAreSticky();
    }

    @Test
    public void shouldVerifySystemStickyCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED).withStickyBitOwnerAndDuration("system",
                    NON_EXPIRING);
        whenVerifyIsCalled();
        verifyThat(1).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(1).replicasAreReadable();
        verifyThat(1).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(1).replicasAreSticky();
    }

    @Test
    public void shouldVerifyPreciousCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.PRECIOUS).withStickyBitOwnerAndDuration("testOwner",
                    TimeUnit.DAYS.toMillis(1));
        whenVerifyIsCalled();
        verifyThat(1).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(1).replicasAreReadable();
        verifyThat(0).replicasAreRemovable();
        verifyThat(1).replicasArePrecious();
    }

    @Test
    public void shouldVerifyMultipleCachedReplicasCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED);
        givenPoolWithAddress("test2@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED);
        givenPoolWithAddress("test3@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.NEW);
        whenVerifyIsCalled();
        verifyThat(3).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(2).replicasAreReadable();
        verifyThat(2).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(0).replicasAreSticky();
    }

    @Test
    public void shouldVerifyCachedPlusPreciousReplicasCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED);
        givenPoolWithAddress("test2@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED).withStickyBitOwnerAndDuration("system", NON_EXPIRING);
        givenPoolWithAddress("test3@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.PRECIOUS)
              .withStickyBitOwnerAndDuration("system", NON_EXPIRING);
        whenVerifyIsCalled();
        verifyThat(3).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(3).replicasAreReadable();
        verifyThat(2).replicasAreRemovable();
        verifyThat(1).replicasArePrecious();
        verifyThat(2).replicasAreSticky();
    }

    @Test
    public void shouldVerifyCachedPlusBrokenReplicasCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED).withStickyBitOwnerAndDuration("system", NON_EXPIRING);
        givenPoolWithAddress("test2@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED).withStickyBitOwnerAndDuration("system", NON_EXPIRING);
        givenPoolWithAddress("test3@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.BROKEN).withStickyBitOwnerAndDuration("system", NON_EXPIRING);
        whenVerifyIsCalled();
        verifyThat(3).replicasExists();
        verifyThat(1).replicasAreBroken();
        verifyThat(2).replicasAreReadable();
        verifyThat(3).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(3).replicasAreSticky();
    }

    @Test
    public void shouldVerifyPartialReplicasCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(ALT_REPLICA)
              .withState(ReplicaState.CACHED).withStickyBitOwnerAndDuration("system", NON_EXPIRING);
        givenPoolWithAddress("test2@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED).withStickyBitOwnerAndDuration("system", NON_EXPIRING);
        givenPoolWithAddress("test3@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED).withStickyBitOwnerAndDuration("system", NON_EXPIRING);
        whenVerifyIsCalled();
        verifyThat(2).replicasExists();
        verifyThat(0).replicasAreBroken();
        verifyThat(2).replicasAreReadable();
        verifyThat(2).replicasAreRemovable();
        verifyThat(0).replicasArePrecious();
        verifyThat(2).replicasAreSticky();
    }

    @Test
    public void shouldVerifyIndividualReplicasCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED);
        givenPoolWithAddress("test2@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED).withStickyBitOwnerAndDuration("system", NON_EXPIRING);
        givenPoolWithAddress("test3@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.PRECIOUS);
        whenVerifyIsCalled();
        verifyThatReplicaOnPool("test1@poolDomain").exists();
        verifyThatReplicaOnPool("test1@poolDomain").isNotBroken();
        verifyThatReplicaOnPool("test1@poolDomain").isReadable();
        verifyThatReplicaOnPool("test1@poolDomain").isRemovable();
        verifyThatReplicaOnPool("test1@poolDomain").isNotSticky();
        verifyThatReplicaOnPool("test2@poolDomain").exists();
        verifyThatReplicaOnPool("test2@poolDomain").isNotBroken();
        verifyThatReplicaOnPool("test2@poolDomain").isReadable();
        verifyThatReplicaOnPool("test2@poolDomain").isRemovable();
        verifyThatReplicaOnPool("test2@poolDomain").isSticky();
        verifyThatReplicaOnPool("test3@poolDomain").exists();
        verifyThatReplicaOnPool("test3@poolDomain").isNotBroken();
        verifyThatReplicaOnPool("test3@poolDomain").isReadable();
        verifyThatReplicaOnPool("test3@poolDomain").isPrecious();
        verifyThatReplicaOnPool("test3@poolDomain").isNotRemovable();
        verifyThatReplicaOnPool("test3@poolDomain").isNotSticky();
    }

    @Test
    public void shouldVerifyIndividualUnreadableReplicasCorrectly() throws Exception {
        givenPoolWithAddress("test1@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.NEW);
        givenPoolWithAddress("test2@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.BROKEN).withStickyBitOwnerAndDuration("system", NON_EXPIRING);
        givenPoolWithAddress("test3@poolDomain").thatHasAReplicaOf(TEST_PNFSID)
              .withState(ReplicaState.CACHED);
        whenVerifyIsCalled();
        verifyThatReplicaOnPool("test1@poolDomain").exists();
        verifyThatReplicaOnPool("test1@poolDomain").isNotBroken();
        verifyThatReplicaOnPool("test1@poolDomain").isNotReadable();
        verifyThatReplicaOnPool("test1@poolDomain").isNotRemovable();
        verifyThatReplicaOnPool("test1@poolDomain").isNotSticky();
        verifyThatReplicaOnPool("test2@poolDomain").exists();
        verifyThatReplicaOnPool("test2@poolDomain").isBroken();
        verifyThatReplicaOnPool("test2@poolDomain").isNotReadable();
        verifyThatReplicaOnPool("test2@poolDomain").isRemovable();
        verifyThatReplicaOnPool("test2@poolDomain").isSticky();
        verifyThatReplicaOnPool("test3@poolDomain").exists();
        verifyThatReplicaOnPool("test3@poolDomain").isNotBroken();
        verifyThatReplicaOnPool("test3@poolDomain").isReadable();
        verifyThatReplicaOnPool("test3@poolDomain").isNotPrecious();
        verifyThatReplicaOnPool("test3@poolDomain").isRemovable();
        verifyThatReplicaOnPool("test3@poolDomain").isNotSticky();
    }

    private ListenableFuture getResponse(String address) throws InterruptedException, ExecutionException {
        QoSMessageHandler handler = handlers.get(address);
        assertNotNull("No handler for " + address + "!", handler);

        MessageReply<ReplicaStatusMessage> reply
              = (MessageReply<ReplicaStatusMessage>) handler.messageArrived(
              new ReplicaStatusMessage(address, TEST_PNFSID));
        return Futures.immediateFuture(reply.get());
    }

    private QoSMessageHandlerBuilder givenPoolWithAddress(String address) {
        QoSMessageHandlerBuilder builder = new QoSMessageHandlerBuilder(address);
        pools.put(address, builder);
        return builder;
    }

    private void doesNotExist() {
        assertFalse("replica exists on " + address,
              RepositoryReplicaVerifier.exists(address, messages));
    }

    private void exists() {
        assertTrue("replica does not exist on " + address,
              RepositoryReplicaVerifier.exists(address, messages));
    }

    private void isNotBroken() {
        assertFalse("replica is broken on " + address,
              RepositoryReplicaVerifier.isBroken(address, messages));
    }

    private void isNotPrecious() {
        assertFalse("replica is precious on " + address,
              RepositoryReplicaVerifier.isPrecious(address, messages));
    }

    private void isNotReadable() {
        assertFalse("replica is readable on " + address,
              RepositoryReplicaVerifier.isReadable(address, messages));
    }

    private void isNotRemovable() {
        assertFalse("replica is removable on " + address,
              RepositoryReplicaVerifier.isRemovable(address, messages));
    }

    private void isNotSticky() {
        assertFalse("replica is sticky on " + address,
              RepositoryReplicaVerifier.isSticky(address, messages));
    }

    private void isBroken() {
        assertTrue("replica is not broken on " + address,
              RepositoryReplicaVerifier.isBroken(address, messages));
    }

    private void isPrecious() {
        assertTrue("replica is not precious on " + address,
              RepositoryReplicaVerifier.isPrecious(address, messages));
    }

    private void isReadable() {
        assertTrue("replica is not readable on " + address,
              RepositoryReplicaVerifier.isReadable(address, messages));
    }

    private void isRemovable() {
        assertTrue("replica is not removable on " + address,
              RepositoryReplicaVerifier.isRemovable(address, messages));
    }

    private void isSticky() {
        assertTrue("replica is not sticky on " + address,
              RepositoryReplicaVerifier.isSticky(address, messages));
    }

    private void replicasAreBroken() {
        assertEquals("wrong number of broken replicas", count,
              RepositoryReplicaVerifier.getBroken(messages).size());
    }

    private void replicasExists() {
        assertEquals("wrong number of replicas", count,
              RepositoryReplicaVerifier.getExists(messages).size());
    }

    private void replicasAreReadable() {
        assertEquals("wrong number of readable replicas", count,
              RepositoryReplicaVerifier.getReadable(messages).size());
    }

    private void replicasArePrecious() {
        assertEquals("wrong number of precious replicas", count,
              RepositoryReplicaVerifier.getPrecious(messages).size());
    }

    private void replicasAreRemovable() {
        assertEquals("wrong number of removable replicas", count,
              RepositoryReplicaVerifier.getRemovable(messages).size());
    }

    public void replicasAreSticky() {
        assertEquals("wrong number of sticky replicas", count,
              RepositoryReplicaVerifier.getSticky(messages).size());
    }

    private RepositoryReplicaVerifierTest verifyThat(int count) {
        this.count = count;
        return this;
    }

    private RepositoryReplicaVerifierTest verifyThatReplicaOnPool(String address) {
        this.address = address;
        return this;
    }

    private void whenVerifyIsCalled() throws Exception {
        for (Entry<String, QoSMessageHandlerBuilder> e : pools.entrySet()) {
            synchronized (handlers) {
                QoSMessageHandler handler = e.getValue().build();
                handlers.put(e.getKey(), handler);
            }
        }
        messages = RepositoryReplicaVerifier.verifyLocations(TEST_PNFSID, pools.keySet(), stub);
    }
}
