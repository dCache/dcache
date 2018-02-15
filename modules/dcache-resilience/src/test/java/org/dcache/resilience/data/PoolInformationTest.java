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

import org.junit.Test;

import diskCacheV111.pools.PoolV2Mode;

import static org.junit.Assert.assertEquals;

/**
 * <p>Tests the mode changes for correct readable and writable classification.</p>
 */
public class PoolInformationTest {

    PoolInformation poolInformation;
    PoolV2Mode poolV2Mode;

    @Test
    public void shouldBeReadableWhenEnabled() {
        whenPoolModeIs(PoolV2Mode.ENABLED);
        assertThatReadableIs(true);
    }

    @Test
    public void shouldBeWritableWhenEnabled() {
        whenPoolModeIs(PoolV2Mode.ENABLED);
        assertThatWritableIs(true);
    }

    @Test
    public void shouldNotBeReadableWhenDisabled() {
        whenPoolModeIs(PoolV2Mode.DISABLED);
        assertThatReadableIs(false);
    }

    @Test
    public void shouldNotBeWritableWhenDisabled() {
        whenPoolModeIs(PoolV2Mode.DISABLED);
        assertThatWritableIs(false);
    }

    @Test
    public void shouldNotBeReadableWhenDisabledFetch() {
        whenPoolModeIs(PoolV2Mode.DISABLED_FETCH);
        assertThatReadableIs(false);
    }

    @Test
    public void shouldBeWritableWhenDisabledFetch() {
        whenPoolModeIs(PoolV2Mode.DISABLED_FETCH);
        assertThatWritableIs(true);
    }

    @Test
    public void shouldBeReadableWhenDisabledStore() {
        whenPoolModeIs(PoolV2Mode.DISABLED_STORE);
        assertThatReadableIs(true);
    }

    @Test
    public void shouldBeWritableWhenDisabledStore() {
        whenPoolModeIs(PoolV2Mode.DISABLED_STORE);
        assertThatWritableIs(true);
    }

    @Test
    public void shouldBeReadableWhenDisabledStage() {
        whenPoolModeIs(PoolV2Mode.DISABLED_STAGE);
        assertThatReadableIs(true);
    }

    @Test
    public void shouldBeWritableWhenDisabledStage() {
        whenPoolModeIs(PoolV2Mode.DISABLED_STAGE);
        assertThatWritableIs(true);
    }

    @Test
    public void shouldBeReadableWhenDisabledP2pClient() {
        whenPoolModeIs(PoolV2Mode.DISABLED_P2P_CLIENT);
        assertThatReadableIs(true);
    }

    @Test
    public void shouldNotBeWritableWhenDisabledP2pClient() {
        whenPoolModeIs(PoolV2Mode.DISABLED_P2P_CLIENT);
        assertThatWritableIs(false);
    }

    @Test
    public void shouldNotBeReadableWhenDisabledP2pServer() {
        whenPoolModeIs(PoolV2Mode.DISABLED_P2P_SERVER);
        assertThatReadableIs(false);
    }

    @Test
    public void shouldBeWritableWhenDisabledP2pServer() {
        whenPoolModeIs(PoolV2Mode.DISABLED_P2P_SERVER);
        assertThatWritableIs(true);
    }

    @Test
    public void shouldNotBeReadableWhenDisabledDead() {
        whenPoolModeIs(PoolV2Mode.DISABLED_DEAD);
        assertThatReadableIs(false);
    }

    @Test
    public void shouldNotBeWritableWhenDisabledDead() {
        whenPoolModeIs(PoolV2Mode.DISABLED_DEAD);
        assertThatWritableIs(false);
    }

    @Test
    public void shouldNotBeReadableWhenDisabledStrict() {
        whenPoolModeIs(PoolV2Mode.DISABLED_STRICT);
        assertThatReadableIs(false);
    }

    @Test
    public void shouldNotBeWritableWhenDisabledStrict() {
        whenPoolModeIs(PoolV2Mode.DISABLED_STRICT);
        assertThatWritableIs(false);
    }

    @Test
    public void shouldBeReadableWhenDisabledRdOnly() {
        whenPoolModeIs(PoolV2Mode.DISABLED_RDONLY);
        assertThatReadableIs(true);
    }

    @Test
    public void shouldNotBeWritableWhenDisabledRdOnly() {
        whenPoolModeIs(PoolV2Mode.DISABLED_RDONLY);
        assertThatWritableIs(false);
    }

    private void whenPoolModeIs(int mode) {
        poolV2Mode = new PoolV2Mode(mode);
        poolInformation = new PoolInformation("test", -1, poolV2Mode);
    }

    private void assertThatReadableIs(boolean b) {
        assertEquals(b, poolInformation.canRead());
    }

    private void assertThatWritableIs(boolean b) {
        assertEquals(b, poolInformation.canWrite());
    }
}
