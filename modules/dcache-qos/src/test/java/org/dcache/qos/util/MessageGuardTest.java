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
package org.dcache.qos.util;

import static junit.framework.TestCase.assertTrue;
import static org.dcache.qos.util.MessageGuard.Status.DISABLED;
import static org.dcache.qos.util.MessageGuard.Status.EXTERNAL;
import static org.dcache.qos.util.MessageGuard.Status.QOS;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import java.util.UUID;
import org.dcache.qos.util.MessageGuard.Status;
import org.junit.Before;
import org.junit.Test;

public class MessageGuardTest {
    static final PnfsId TEST_PNFSID = new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD00");

    class MessageReceiver implements CellMessageReceiver {
        void messageArrived(CellMessage msg) {
            CDC.setMessageContext(msg);
        }
    }

    BackloggedMessageHandler backlogHandler;
    MessageReceiver handler;
    MessageGuard guard;
    CellMessage msg;
    PnfsAddCacheLocationMessage pnfsMessage;

    @Before
    public void setUp() {
        guard = new MessageGuard();
        backlogHandler = mock(BackloggedMessageHandler.class);
        guard.setBacklogHandler(backlogHandler);
        handler = new MessageReceiver();
        msg = new CellMessage();
        msg.getSourcePath().add(new CellPath("Foo", "bar"));
        pnfsMessage = new PnfsAddCacheLocationMessage(TEST_PNFSID, "aPool");
        msg.setMessageObject(pnfsMessage);
    }

    @Test
    public void shouldReportRandomSessionIdAsExternalMessage() {
        givenAMessageWithRandomSessionId();
        givenGuardIsEnabled();
        whenMessageArrives();
        verifyThatMessageStatusIs(EXTERNAL);
    }

    @Test
    public void shouldReportQoSSessionIdAsQoSMessage() {
        givenAQoSGeneratedMessage();
        givenGuardIsEnabled();
        whenMessageArrives();
        verifyThatMessageStatusIsNot(EXTERNAL);
        verifyThatMessageStatusIs(QOS);
    }

    @Test
    public void shouldSaveMessageWhenDisabled() {
        givenAMessageWithRandomSessionId();
        whenMessageArrives();
        verifyThatMessageStatusIs(DISABLED);
        verifyThatPnfsMessageWasSaved();
    }

    @Test
    public void shouldDropMessageWhenDisabledWithDropTrue() {
        givenAMessageWithRandomSessionId();
        givenGuardIsDisabledWithDropTrue();
        whenMessageArrives();
        verifyThatMessageStatusIs(DISABLED);
        verifyThatPnfsMessageWasNotSaved();
    }

    @Test
    public void shouldRedeliverMessagesWhenReenabled() throws Exception {
        givenGuardIsDisabled();
        givenGuardIsEnabled();
        verifyThatBacklogIsHandled();
    }

    private void givenAMessageWithRandomSessionId() {
        msg.setSession(UUID.randomUUID().toString());
    }

    private void givenAQoSGeneratedMessage() {
        msg.getSourcePath().add(new CellPath("Foo", "bar"));
        msg.setSession(MessageGuard.QOS_ID);
    }

    private void givenGuardIsDisabled() {
        guard.disable(false);
    }

    private void givenGuardIsDisabledWithDropTrue() {
        guard.disable(true);
    }

    private void givenGuardIsEnabled() {
        guard.enable();
    }

    private void verifyThatBacklogIsHandled() {
        verify(backlogHandler).handleBacklog();
    }

    private void verifyThatMessageStatusIs(Status status) {
        assertTrue(status == guard.getStatus("verifyThatMessageStatusIs", msg));
    }

    private void verifyThatMessageStatusIsNot(Status status) {
        assertFalse(status == guard.getStatus("verifyThatMessageStatusIsNot", msg));
    }

    private void verifyThatPnfsMessageWasSaved() {
        verify(backlogHandler).saveToBacklog(pnfsMessage);
    }

    private void verifyThatPnfsMessageWasNotSaved() {
        verify(backlogHandler, never()).saveToBacklog(pnfsMessage);
    }

    private void whenMessageArrives() {
        handler.messageArrived(msg);
    }
}
