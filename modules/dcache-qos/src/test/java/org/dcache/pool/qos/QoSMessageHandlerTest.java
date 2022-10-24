/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.pool.qos;

import static org.dcache.mock.CacheEntryBuilder.aCacheEntry;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.Reply;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.dcache.cells.MessageReply;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.qos.ChangePreciousBitMessage;
import org.dcache.vehicles.qos.ChangeStickyBitMessage;
import org.dcache.vehicles.qos.ReplicaStatusMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class QoSMessageHandlerTest {

    private static final PnfsId TEST_PNFSID = new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD00");
    private static final String TEST_POOL = "aTestPool";

    @Mock
    Repository repository;
    QoSMessageHandler qoSMessageHandler;
    ReplicaStatusMessage replicaStatusMessage;
    ChangePreciousBitMessage changePreciousBitMessage;
    ChangeStickyBitMessage changeStickyBitMessage;

    @Before
    public void setup() {
        repository = mock(Repository.class);
        qoSMessageHandler = new QoSMessageHandler();
        qoSMessageHandler.setRepository(repository);
        qoSMessageHandler.setExecutionService(MoreExecutors.newDirectExecutorService());
    }

    @Test
    public void shouldChangeStickyBitToTrue() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).build());
        whenHandlerReceives(aSetStickyBitMessage());
        verify(repository).setSticky(eq(TEST_PNFSID), eq("system"), eq(StickyRecord.NON_EXPIRING),
              eq(true));
    }

    @Test
    public void shouldChangeStickyBitToFalse() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).build());
        whenHandlerReceives(anUnsetStickyBitMessage());
        verify(repository).setSticky(eq(TEST_PNFSID), eq("system"), eq(0L),
              eq(true));
    }

    @Test
    public void shouldChangePreciousBitToCached() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.PRECIOUS).build());
        whenHandlerReceives(anUnsetPreciousBitMessage());
        verify(repository).setState(eq(TEST_PNFSID), eq(ReplicaState.CACHED), anyString());
    }

    @Test
    public void shouldReportWaitingIfFromClient() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_CLIENT).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(true, replicaStatusMessage.isWaiting());
    }

    @Test
    public void shouldReportWaitingIfFromPool() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_POOL).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(true, replicaStatusMessage.isWaiting());
    }

    @Test
    public void shouldReportWaitingIfFromStore() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_STORE).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(true, replicaStatusMessage.isWaiting());
    }

    @Test
    public void shouldNotReportWaitingIfCached() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.CACHED).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isWaiting());
    }

    @Test
    public void shouldNotReportWaitingIfPrecious() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.PRECIOUS).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isWaiting());
    }

    @Test
    public void shouldNotReportWaitingIfBroken() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.BROKEN).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isWaiting());
    }

    @Test
    public void shouldReportReadableIfCached() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.CACHED).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(true, replicaStatusMessage.isReadable());
    }

    @Test
    public void shouldReportReadableIfPrecious() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.PRECIOUS).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(true, replicaStatusMessage.isReadable());
    }

    @Test
    public void shouldNotReportReadableIfBroken() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.BROKEN).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isReadable());
    }

    @Test
    public void shouldNotReportReadableIfFromClient() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_CLIENT).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isReadable());
    }

    @Test
    public void shouldNotReportReadableIfFromPool() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_POOL).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isReadable());
    }

    @Test
    public void shouldNotReportReadableIfFromStore() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_STORE).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isReadable());
    }

    @Test
    public void shouldReportRemovableIfCached() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.CACHED).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(true, replicaStatusMessage.isRemovable());
    }

    @Test
    public void shouldReportRemovableIfBroken() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.BROKEN).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(true, replicaStatusMessage.isRemovable());
    }

    @Test
    public void shouldNotReportRemovableIfFromClient() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_CLIENT).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isRemovable());
    }

    @Test
    public void shouldNotReportRemovableIfPrecious() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.PRECIOUS).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isRemovable());
    }

    @Test
    public void shouldNotReportRemovableIfFromPool() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_POOL).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isRemovable());
    }

    @Test
    public void shouldNotReportRemovableIfFromStore() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_STORE).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isRemovable());
    }

    @Test
    public void shouldReportPreciousIfPrecious() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.PRECIOUS).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(true, replicaStatusMessage.isPrecious());
    }

    @Test
    public void shouldNotReportPreciousIfCached() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.CACHED).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isPrecious());
    }

    @Test
    public void shouldNotReportPreciousIfBroken() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.BROKEN).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isPrecious());
    }

    @Test
    public void shouldNotReportPreciousIfFromClient() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_CLIENT).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isPrecious());
    }

    @Test
    public void shouldNotReportPreciousIfFromPool() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_POOL).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isPrecious());
    }

    @Test
    public void shouldNotReportPreciousIfFromStore() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.FROM_STORE).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isPrecious());
    }

    @Test
    public void shouldReportStickyIfSystemOwnsNonExpiringPin() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.CACHED)
              .withStickyRecord("system", StickyRecord.NON_EXPIRING).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(true, replicaStatusMessage.isSystemSticky());
    }


    public void shouldNotReportStickyIfSystemDoesNotOwnNonExpiringPin() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.CACHED)
              .withStickyRecord("arossi", StickyRecord.NON_EXPIRING).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isSystemSticky());
    }

    public void shouldNotReportStickyIfSystemOwnsExpiringPin() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.CACHED)
              .withStickyRecord("system", TimeUnit.HOURS.toMillis(1)).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isSystemSticky());
    }

    public void shouldNotReportStickyIfOnlyPinnedByUser() throws Exception {
        given(aCacheEntry().withPnfsId(TEST_PNFSID).withState(ReplicaState.CACHED)
              .withStickyRecord("arossi", TimeUnit.HOURS.toMillis(1)).build());
        whenHandlerReceives(aReplicaStatusMessage());
        assertEquals(false, replicaStatusMessage.isSystemSticky());
    }

    private void given(CacheEntry entry) throws CacheException, InterruptedException {
        try {
            when(repository.getEntry(TEST_PNFSID)).thenReturn(entry);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private ReplicaStatusMessage aReplicaStatusMessage() {
        return new ReplicaStatusMessage(TEST_POOL, TEST_PNFSID);
    }

    private ChangePreciousBitMessage anUnsetPreciousBitMessage() {
        return new ChangePreciousBitMessage(TEST_POOL, TEST_PNFSID);
    }

    private ChangeStickyBitMessage aSetStickyBitMessage() {
        return new ChangeStickyBitMessage(TEST_POOL, TEST_PNFSID, true);
    }


    private ChangeStickyBitMessage anUnsetStickyBitMessage() {
        return new ChangeStickyBitMessage(TEST_POOL, TEST_PNFSID, false);
    }

    private void whenHandlerReceives(ChangePreciousBitMessage message)
          throws ExecutionException, InterruptedException {
        Reply reply = qoSMessageHandler.messageArrived(message);
        changePreciousBitMessage = ((MessageReply<ChangePreciousBitMessage>) reply).get();
    }

    private void whenHandlerReceives(ChangeStickyBitMessage message)
          throws ExecutionException, InterruptedException {
        Reply reply = qoSMessageHandler.messageArrived(message);
        changeStickyBitMessage = ((MessageReply<ChangeStickyBitMessage>) reply).get();
    }

    private void whenHandlerReceives(ReplicaStatusMessage message)
          throws ExecutionException, InterruptedException {
        Reply reply = qoSMessageHandler.messageArrived(message);
        replicaStatusMessage = ((MessageReply<ReplicaStatusMessage>) reply).get();
    }
}
