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
package org.dcache.alarms.dao;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.dcache.alarms.IAlarms;
import org.dcache.alarms.Severity;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Some simple correctness tests.
 *
 * @author arossi
 */
public class AlarmEntryTest {

    @Test
    public void shouldBeEqualForIdenticalKey() {
        AlarmEntry e1 = new AlarmEntry();
        AlarmEntry e2 = new AlarmEntry();
        givenEqualKeysButDifferentTimestamps(e1, e2);
        assertThat(e1.equals(e2), is(true));
    }

    @Test
    public void shouldNotBeEqualForDifferentKeys() {
        AlarmEntry e1 = new AlarmEntry();
        AlarmEntry e2 = new AlarmEntry();
        givenDifferentKeys(e1, e2);
        assertThat(e1.equals(e2), is(false));
    }

    @Test
    public void shouldHaveSameFieldValuesAsMessageNames() throws JSONException {
        JSONObject config = givenMessageWithAllFieldsDefined();
        JSONObject original = new JSONObject(config.toString());
        AlarmEntry e = givenAlarmEntryConfiguredFrom(config);
        assertThat(e.getKey(), is(original.get(IAlarms.KEY_TAG)));
        assertThat(e.getType(), is(original.get(IAlarms.TYPE_TAG)));
        assertThat(e.getHost(), is(original.get(IAlarms.HOST_TAG)));
        assertThat(e.getDomain(), is(original.get(IAlarms.DOMAIN_TAG)));
        assertThat(e.getService(), is(original.get(IAlarms.SERVICE_TAG)));
        assertThat(e.getTimestamp(), is(original.get(IAlarms.TIMESTAMP_TAG)));
        assertThat(e.getSeverity(),
                        is(Severity.valueOf(
                                        original.getString(IAlarms.SEVERITY_TAG)).ordinal()));
        assertThat(e.getInfo(), is(original.get(IAlarms.MESSAGE_TAG)));
    }

    private JSONObject givenMessageWithAllFieldsDefined() throws JSONException {
        JSONObject config = new JSONObject();
        config.put(IAlarms.KEY_TAG, UUID.randomUUID().toString());
        config.put(IAlarms.TYPE_TAG, "test");
        config.put(IAlarms.HOST_TAG, "host.domain");
        config.put(IAlarms.DOMAIN_TAG, "testDomain");
        config.put(IAlarms.SERVICE_TAG, "testCell");
        config.put(IAlarms.TIMESTAMP_TAG, System.currentTimeMillis());
        config.put(IAlarms.SEVERITY_TAG, Severity.HIGH.toString());
        config.put(IAlarms.MESSAGE_TAG, "PROP_A=VALUE_A;PROP_B=VALUE_B");
        return config;
    }

    private AlarmEntry givenAlarmEntryConfiguredFrom(JSONObject config)
                    throws JSONException {
        AlarmEntry e = new AlarmEntry(config);
        return e;
    }

    private void givenEqualKeysButDifferentTimestamps(AlarmEntry e1,
                    AlarmEntry e2) {
        e1.setKey("non-unique");
        e2.setKey("non-unique");
        long t = System.currentTimeMillis();
        e1.setTimestamp(t + TimeUnit.MINUTES.toMillis(1));
        e2.setTimestamp(t + TimeUnit.MINUTES.toMillis(2));
    }

    private void givenDifferentKeys(AlarmEntry e1, AlarmEntry e2) {
        e1.setKey("key1");
        e2.setKey("key2");
        long t = System.currentTimeMillis();
        e1.setTimestamp(t);
        e2.setTimestamp(t);
    }
}
