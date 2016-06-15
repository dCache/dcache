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
package org.dcache.resilience.data;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import diskCacheV111.pools.PoolV2Mode;
import org.dcache.resilience.data.PoolOperation.NextAction;
import org.dcache.resilience.data.PoolOperation.State;

import static org.dcache.resilience.data.PoolOperation.NextAction.DOWN_TO_UP;
import static org.dcache.resilience.data.PoolOperation.NextAction.NOP;
import static org.dcache.resilience.data.PoolOperation.NextAction.UP_TO_DOWN;
import static org.dcache.resilience.data.PoolStatusForResilience.DOWN;
import static org.dcache.resilience.data.PoolStatusForResilience.ENABLED;
import static org.dcache.resilience.data.PoolStatusForResilience.READ_ONLY;
import static org.dcache.resilience.data.PoolStatusForResilience.UNINITIALIZED;
import static org.junit.Assert.assertEquals;

/**
 * <p>Tests the Cartesian product of mode changes for correct action transition.</p>
 */
public final class PoolStatusTransitionTest {

    static final List<TestOracle> oracles = new ArrayList<>();

    static {
        generateOracles();
    }

    static class TestOracle {
        final PoolStatusForResilience last;
        final PoolStatusForResilience incoming;
        final PoolStatusForResilience curr;
        final NextAction              action;

        TestOracle(PoolStatusForResilience last,
                   PoolStatusForResilience incoming,
                   PoolStatusForResilience curr,
                   NextAction action) {
            this.last = last;
            this.incoming = incoming;
            this.curr = curr;
            this.action = action;
        }
    }

    static void generateOracles() {
        oracles.add(new TestOracle(DOWN, DOWN, DOWN, NOP));
        oracles.add(new TestOracle(UNINITIALIZED, DOWN, DOWN, NOP));
        oracles.add(new TestOracle(UNINITIALIZED, DOWN, ENABLED, NOP));
        oracles.add(new TestOracle(DOWN, READ_ONLY, READ_ONLY, DOWN_TO_UP));
        oracles.add(new TestOracle(DOWN, ENABLED, ENABLED, DOWN_TO_UP));
        oracles.add(new TestOracle(READ_ONLY, DOWN, DOWN, UP_TO_DOWN));
        oracles.add(new TestOracle(UNINITIALIZED, DOWN, DOWN, NOP));
        oracles.add(new TestOracle(UNINITIALIZED, READ_ONLY, ENABLED, NOP));
        oracles.add(new TestOracle(READ_ONLY, READ_ONLY, READ_ONLY, NOP));
        oracles.add(new TestOracle(READ_ONLY, ENABLED, ENABLED, NOP));
        oracles.add(new TestOracle(ENABLED, DOWN, DOWN, UP_TO_DOWN));
        oracles.add(new TestOracle(UNINITIALIZED, DOWN, DOWN, NOP));
        oracles.add(new TestOracle(UNINITIALIZED, ENABLED, ENABLED, NOP));
        oracles.add(new TestOracle(ENABLED, READ_ONLY, READ_ONLY, NOP));
        oracles.add(new TestOracle(ENABLED, ENABLED, ENABLED, NOP));
        oracles.add(new TestOracle(UNINITIALIZED, DOWN, DOWN, UP_TO_DOWN));
        oracles.add(new TestOracle(UNINITIALIZED, DOWN, DOWN, NOP));
        oracles.add(new TestOracle(UNINITIALIZED, ENABLED, ENABLED, NOP));
        oracles.add(new TestOracle(UNINITIALIZED, READ_ONLY, READ_ONLY, NOP));
        oracles.add(new TestOracle(UNINITIALIZED, ENABLED, ENABLED, NOP));
    }

    PoolV2Mode    mode;
    NextAction    action;
    PoolOperation operation;

    @Test
    public void case00ActionShouldBeNOPForDownFollowedByDown() {
        givenUpdateWithPoolModeEqualToDown();
        givenUpdateWithPoolModeEqualToDown();
        assertThatTestCaseObeysOracle(0);
    }

    @Test
    public void case01ActionShouldBeNOPForDownFollowedByDownResilienceOff() {
        givenUpdateWithPoolModeEqualToDown();
        givenUpdateWithPoolModeEqualToDownButResilienceOff();
        assertThatTestCaseObeysOracle(1);
    }

    @Test
    public void case02ActionShouldBeNOPForDownFollowedByUpResilienceOff() {
        givenUpdateWithPoolModeEqualToDown();
        givenUpdateWithPoolModeEnabledButResilienceOff();
        assertThatTestCaseObeysOracle(2);
    }

    @Test
    public void case03ActionShouldBeDownToUpForDownFollowedByReadable() {
        givenUpdateWithPoolModeEqualToDown();
        givenUpdateWithPoolModeReadOnly();
        assertThatTestCaseObeysOracle(3);
    }

    @Test
    public void case04ActionShouldBeDownToUpForDownFollowedByWritable() {
        givenUpdateWithPoolModeEqualToDown();
        givenUpdateWithPoolModeEnabled();
        assertThatTestCaseObeysOracle(4);
    }

    @Test
    public void case05ActionShouldBeUpToDownForReadableFollowedByDown() {
        givenUpdateWithPoolModeReadOnly();
        givenUpdateWithPoolModeEqualToDown();
        assertThatTestCaseObeysOracle(5);
    }

    @Test
    public void case06ActionShouldBeNOPForReadableFollowedByDownResilienceOff() {
        givenUpdateWithPoolModeReadOnly();
        givenUpdateWithPoolModeEqualToDownButResilienceOff();
        assertThatTestCaseObeysOracle(6);
    }

    @Test
    public void case07ActionShouldBeNOPForReadableFollowedByUpResilienceOff() {
        givenUpdateWithPoolModeReadOnly();
        givenUpdateWithPoolModeEnabledButResilienceOff();
        assertThatTestCaseObeysOracle(7);
    }

    @Test
    public void case08ActionShouldBeNOPForReadableFollowedByReadable() {
        givenUpdateWithPoolModeReadOnly();
        givenUpdateWithPoolModeReadOnly();
        assertThatTestCaseObeysOracle(8);
    }

    @Test
    public void case09ActionShouldBeNOPForReadableFollowedByWritable() {
        givenUpdateWithPoolModeReadOnly();
        givenUpdateWithPoolModeEnabled();
        assertThatTestCaseObeysOracle(9);
    }

    @Test
    public void case10ActionShouldBeUpToDownForWritableFollowedByDown() {
        givenUpdateWithPoolModeEnabled();
        givenUpdateWithPoolModeEqualToDown();
        assertThatTestCaseObeysOracle(10);
    }

    @Test
    public void case11ActionShouldBeNOPForWritableFollowedByDownResilienceOff() {
        givenUpdateWithPoolModeEnabled();
        givenUpdateWithPoolModeEqualToDownButResilienceOff();
        assertThatTestCaseObeysOracle(11);
    }

    @Test
    public void case12ActionShouldBeNOPForWritableFollowedByUpResilienceOff() {
        givenUpdateWithPoolModeEnabled();
        givenUpdateWithPoolModeEnabledButResilienceOff();
        assertThatTestCaseObeysOracle(12);
    }

    @Test
    public void case13ActionShouldBeNOPForWritableFollowedByReadable() {
        givenUpdateWithPoolModeEnabled();
        givenUpdateWithPoolModeReadOnly();
        assertThatTestCaseObeysOracle(13);
    }

    @Test
    public void case14ActionShouldBeNOPForWritableFollowedByWritable() {
        givenUpdateWithPoolModeEnabled();
        givenUpdateWithPoolModeEnabled();
        assertThatTestCaseObeysOracle(14);
    }

    @Test
    public void case15ActionShouldBeUpToDownForUninitializedFollowedByDown() {
        givenUpdateWithPoolModeEqualToDown();
        assertThatTestCaseObeysOracle(15);
    }

    @Test
    public void case16ActionShouldBeNOPForUninitializedFollowedByDownResilienceOff() {
        givenUpdateWithPoolModeEqualToDownButResilienceOff();
        assertThatTestCaseObeysOracle(16);
    }

    @Test
    public void case17ActionShouldBeNOPForUninitializedFollowedByUpResilienceOff() {
        givenUpdateWithPoolModeEnabledButResilienceOff();
        assertThatTestCaseObeysOracle(17);
    }

    @Test
    public void case18ActionShouldBeNOPForUninitializedFollowedByReadable() {
        givenUpdateWithPoolModeReadOnly();
        assertThatTestCaseObeysOracle(18);
    }

    @Test
    public void case19ActionShouldBeNOPForUninitializedFollowedByWritable() {
        givenUpdateWithPoolModeEnabled();
        assertThatTestCaseObeysOracle(19);
    }

    @Before
    public void setUp() {
        operation = new PoolOperation();
    }

    private void assertThatTestCaseObeysOracle(int i) {
        TestOracle oracle = oracles.get(i);
        assertEquals(oracle.last, operation.lastStatus);
        assertEquals(oracle.curr, operation.currStatus);
        assertEquals(oracle.action, action);
    }

    private void givenPoolModeOf(int mode, boolean enabled) {
        this.mode = new PoolV2Mode(mode);
        operation.state = enabled ? State.WAITING : State.EXCLUDED;
    }

    private void givenUpdateWithPoolModeEnabled() {
        givenPoolModeOf(PoolV2Mode.ENABLED, true);
        whenTransitionIsComputed();
    }

    private void givenUpdateWithPoolModeEnabledButResilienceOff() {
        givenPoolModeOf(PoolV2Mode.ENABLED, false);
        whenTransitionIsComputed();
    }

    private void givenUpdateWithPoolModeEqualToDown() {
        givenPoolModeOf(PoolV2Mode.DISABLED_STRICT, true);
        whenTransitionIsComputed();
    }

    private void givenUpdateWithPoolModeEqualToDownButResilienceOff() {
        givenPoolModeOf(PoolV2Mode.DISABLED_STRICT, false);
        whenTransitionIsComputed();
    }

    private void givenUpdateWithPoolModeReadOnly() {
        givenPoolModeOf(PoolV2Mode.DISABLED_RDONLY, true);
        whenTransitionIsComputed();
    }

    private void whenTransitionIsComputed() {
        action = operation.getNextAction(
                        PoolStatusForResilience.getStatusFor(mode));
    }
}
