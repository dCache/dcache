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

import static org.dcache.qos.util.MessageGuardTest.TEST_PNFSID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import diskCacheV111.vehicles.Message;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.dcache.mock.TestMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SerializingBacklogHandlerTest implements Consumer<Message>  {

    SerializingBacklogHandler handler;
    Set<Message> sent;
    Set<Message> received;
    File backupDir;

    @Override
    public void accept(Message message) {
        received.add(message);
    }

    @Before
    public void setup() {
        sent = new HashSet<>();
        received = new HashSet<>();
        backupDir = new File("/tmp/", UUID.randomUUID().toString());
        handler = new SerializingBacklogHandler(backupDir.getAbsolutePath());
        handler.setReceivers(Set.of(this));
    }

    @After
    public void shutdown() {
        Arrays.stream(backupDir.listFiles()).forEach(File::delete);
        backupDir.deleteOnExit();
    }

    @Test
    public void shouldStoreAllBackloggedMessages() throws Exception {
        givenTheNumberOfArrivingMessagesReaches(10);
        verifyBackupFileCountIsNow(10);
    }

    @Test
    public void shouldDeliverEntireBacklog() throws Exception {
        givenTheNumberOfArrivingMessagesReaches(10);
        whenBacklogIsProcessed();
        verifyAllBackloggedMessagesHaveBeenReceived();
        verifyBackupFileCountIsNow(0);
    }

    private void givenTheNumberOfArrivingMessagesReaches(int number) throws InterruptedException {
        for (int i = 0; i < number; ++i) {
            TestMessage msg = new TestMessage(TEST_PNFSID, "pool" + i);
            sent.add(msg);
            handler.saveToBacklog(msg);
        }

        handler.consume();
    }

    private void verifyAllBackloggedMessagesHaveBeenReceived() {
        assertTrue(Sets.difference(sent, received).isEmpty());
        assertTrue(Sets.difference(received, sent).isEmpty());
    }

    private void verifyBackupFileCountIsNow(int count) {
        assertEquals(backupDir.getAbsoluteFile() + " incorrect number of files", count,
              backupDir.listFiles().length);
    }

    private void whenBacklogIsProcessed() throws InterruptedException {
        handler.reload();
    }
}
