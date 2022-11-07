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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import diskCacheV111.util.PnfsId;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class QoSHistoryTest {
    static final String[] ENTRIES = {
          "0000B64FF6C247D84D42BE5ACE9CB688AD10",
          "0000B64FF6C247D84D42BE5ACE9CB688AD11",
          "0000B64FF6C247D84D42BE5ACE9CB688AD12",
          "0000B64FF6C247D84D42BE5ACE9CB688AD13",
          "0000B64FF6C247D84D42BE5ACE9CB688AD14",
          "0000B64FF6C247D84D42BE5ACE9CB688AD15",
          "0000B64FF6C247D84D42BE5ACE9CB688AD16",
          "0000B64FF6C247D84D42BE5ACE9CB688AD17",
          "0000B64FF6C247D84D42BE5ACE9CB688AD18",
          "0000B64FF6C247D84D42BE5ACE9CB688AD19",
          "0000B64FF6C247D84D42BE5ACE9CB688AD20"
    };

    QoSHistory qoSHistory;
    String history;
    List<String> errors;
    int capacity;

    @Before
    public void setup() {
        qoSHistory = new QoSHistory();
    }

    @Test
    public void shouldDropTheEarliestEntryWithAscending() {
        givenCapacityIs(10);
        afterCapacityIsReached();
        whenAnotherEntryIsAdded();
        whenFullAscendingHistoryIsRequested();
        verifyThatHistoryDoesNotContain("0000B64FF6C247D84D42BE5ACE9CB688AD10");
        verifyThatHistoryContains("0000B64FF6C247D84D42BE5ACE9CB688AD20");
    }

    @Test
    public void shouldDropTheEarliestEntryWithDescending() {
        givenCapacityIs(10);
        afterCapacityIsReached();
        whenAnotherEntryIsAdded();
        whenFullDescendingHistoryIsRequested();
        verifyThatHistoryDoesNotContain("0000B64FF6C247D84D42BE5ACE9CB688AD10");
        verifyThatHistoryContains("0000B64FF6C247D84D42BE5ACE9CB688AD20");
    }

    @Test
    public void shouldDisplayEarliestFirst() {
        givenCapacityIs(3);
        afterCapacityIsReached();
        whenFullAscendingHistoryIsRequested();
        verifyThatHistoryStartsWith("0000B64FF6C247D84D42BE5ACE9CB688AD10");
        verifyThatHistoryEndsWith("0000B64FF6C247D84D42BE5ACE9CB688AD12");
    }

    @Test
    public void shouldDisplayEarliestUpToLimit() {
        givenCapacityIs(10);
        afterCapacityIsReached();
        whenAscendingHistoryIsRequestedWithLimit(4);
        verifyThatHistoryStartsWith("0000B64FF6C247D84D42BE5ACE9CB688AD10");
        verifyThatHistoryEndsWith("0000B64FF6C247D84D42BE5ACE9CB688AD13");
    }

    @Test
    public void shouldDisplayLatestFirst() {
        givenCapacityIs(3);
        afterCapacityIsReached();
        whenFullDescendingHistoryIsRequested();
        verifyThatHistoryStartsWith("0000B64FF6C247D84D42BE5ACE9CB688AD12");
        verifyThatHistoryEndsWith("0000B64FF6C247D84D42BE5ACE9CB688AD10");
    }

    @Test
    public void shouldDisplayLatestUpToLimit() {
        givenCapacityIs(10);
        afterCapacityIsReached();
        whenDescendingHistoryIsRequestedWithLimit(4);
        verifyThatHistoryStartsWith("0000B64FF6C247D84D42BE5ACE9CB688AD19");
        verifyThatHistoryEndsWith("0000B64FF6C247D84D42BE5ACE9CB688AD16");
    }

    @Test
    public void shouldDisplayOnlyErrorsWhenErrorsRequested() {
        givenCapacityIs(10);
        afterCapacityIsReached();
        whenAnErrorEntryIsAdded();
        whenErrorHistoryIsRequested();
        verifyThatHistoryStartsWith("0000B64FF6C247D84D42BE5ACE9CB688AD20");
        verifyThatHistoryEndsWith("0000B64FF6C247D84D42BE5ACE9CB688AD20");
    }

    @Test
    public void shouldDisplayErrorsWhenHistoryRequested() {
        givenCapacityIs(10);
        afterCapacityIsReached();
        whenAnErrorEntryIsAdded();
        whenFullAscendingHistoryIsRequested();
        verifyThatHistoryContains("0000B64FF6C247D84D42BE5ACE9CB688AD20");
    }

    @Test
    public void shouldClearErrorsWhenGetAndClearRequested() {
        givenCapacityIs(10);
        afterCapacityIsReached();
        whenAnErrorEntryIsAdded();
        whenGetAndClearErrorsRequested();
        whenErrorHistoryIsRequested();
        verifyThatErrorsContain("0000B64FF6C247D84D42BE5ACE9CB688AD20");
        verifyThatHistoryIsEmpty();
    }

    private void afterCapacityIsReached() {
        for (int i = 0; i < capacity; ++i) {
            qoSHistory.add(new PnfsId(ENTRIES[i]), ENTRIES[i], false);
        }
    }

    private void givenCapacityIs(int capacity) {
        this.capacity = capacity;
        qoSHistory.setCapacity(capacity);
        qoSHistory.initialize();
    }

    private void verifyThatErrorsContain(String string) {
        assertTrue(errors.contains(string));
    }

    private void verifyThatHistoryContains(String string) {
        assertTrue(history.contains(string));
    }

    private void verifyThatHistoryDoesNotContain(String string) {
        assertFalse(history.contains(string));
    }

    private void verifyThatHistoryEndsWith(String string) {
        assertTrue(history.stripTrailing().endsWith(string));
    }

    private void verifyThatHistoryIsEmpty() {
        assertTrue(history.strip().isEmpty());
    }

    private void verifyThatHistoryStartsWith(String string) {
        assertTrue(history.stripLeading().startsWith(string));
    }

    private void whenAnErrorEntryIsAdded() {
        qoSHistory.add(new PnfsId(ENTRIES[capacity]), ENTRIES[capacity], true);
    }

    private void whenAnotherEntryIsAdded() {
        qoSHistory.add(new PnfsId(ENTRIES[capacity]), ENTRIES[capacity], false);
    }

    private void whenAscendingHistoryIsRequestedWithLimit(int limit) {
        history = qoSHistory.ascending(false, limit);
    }

    private void whenDescendingHistoryIsRequestedWithLimit(int limit) {
        history = qoSHistory.descending(false, limit);
    }

    private void whenErrorHistoryIsRequested() {
        history = qoSHistory.ascending(true);
    }

    private void whenFullAscendingHistoryIsRequested() {
        history = qoSHistory.ascending(false);
    }

    private void whenFullDescendingHistoryIsRequested() {
        history = qoSHistory.descending(false);
    }

    private void whenGetAndClearErrorsRequested() {
        errors = qoSHistory.getAndClearErrorPnfsids();
    }
}
