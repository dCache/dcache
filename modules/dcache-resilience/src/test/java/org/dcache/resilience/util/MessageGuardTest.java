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
package org.dcache.resilience.util;

import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import org.dcache.resilience.util.MessageGuard.Status;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public final class MessageGuardTest {
    class MessageReceiver implements CellMessageReceiver {
        void messageArrived(CellMessage msg) {
            CDC.setMessageContext(msg);
        }
    }

    BackloggedMessageHandler backlogHandler;
    MessageReceiver handler;
    MessageGuard guard;
    CellMessage msg;

    @Before
    public void setUp() {
        guard = new MessageGuard();
        backlogHandler = mock(BackloggedMessageHandler.class);
        guard.setBacklogHandler(backlogHandler);
        handler = new MessageReceiver();
        msg = new CellMessage();
    }

    @Test
    public void shouldAcceptMessage() throws Exception {
        msg.setSession(UUID.randomUUID().toString());
        msg.getSourcePath().add(new CellPath("Foo", "bar"));
        givenGuardIsEnabled();
        handler.messageArrived(msg);

        assertTrue(Status.EXTERNAL == guard.getStatus("test", msg));
    }

    @Test
    public void shouldRejectMessage() throws Exception {
        msg.setSession(MessageGuard.RESILIENCE_ID);
        msg.getSourcePath().add(new CellPath("Foo", "bar"));
        givenGuardIsEnabled();
        handler.messageArrived(msg);

        assertFalse(Status.EXTERNAL == guard.getStatus("test", msg));
    }

    @Test
    public void shouldSaveMessage() throws Exception {
        msg.setSession(UUID.randomUUID().toString());
        msg.getSourcePath().add(new CellPath("Foo", "bar"));
        handler.messageArrived(msg);

        assertTrue(Status.DISABLED == guard.getStatus("test", msg));
        verify(backlogHandler).saveToBacklog(msg);
    }

    @Test
    public void shouldDropMessage() throws Exception {
        msg.setSession(UUID.randomUUID().toString());
        msg.getSourcePath().add(new CellPath("Foo", "bar"));
        givenGuardIsDisabledWithDropTrue();
        handler.messageArrived(msg);

        assertTrue(Status.DISABLED == guard.getStatus("test", msg));
        verify(backlogHandler, never()).saveToBacklog(msg);
    }

    @Test
    public void shouldRedeliverMessages() throws Exception {
        givenGuardIsDisabled();
        givenGuardIsEnabled();
        verify(backlogHandler).handleBacklog();
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
}
