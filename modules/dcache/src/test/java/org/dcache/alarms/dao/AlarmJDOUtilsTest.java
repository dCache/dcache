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

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.dcache.alarms.LogEntry;
import org.dcache.alarms.dao.AlarmJDOUtils.AlarmDAOFilter;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Some simple filter consistency tests.
 *
 * @author arossi
 */
public class AlarmJDOUtilsTest {

    private AlarmDAOFilter filter;
    private List<LogEntry> alarms;

    @Before
    public void setup() throws Exception {
        alarms = new ArrayList<>();
    }

    @Test
    public void shouldContainThreeClausesForThreeParams() {
        Date after = new Date(System.currentTimeMillis());
        Date before = new Date(System.currentTimeMillis());
        givenFilterParametersAre(after, before, "CHECKSUM");
        assertThat(filter.getFilter().startsWith("&&"), is(false));
        assertThat(filter.getFilter().endsWith("&&"), is(false));
        assertThat(occurrencesOf("&&", filter.getFilter()), is(2));
        assertThat(filter.getParameters().startsWith(","), is(false));
        assertThat(filter.getParameters().endsWith(","), is(false));
        assertThat(occurrencesOf(",", filter.getParameters()), is(2));
        assertThat(filter.getValues().length, is(3));
        assertThat((Long) filter.getValues()[0], is(after.getTime()));
        assertThat((Long) filter.getValues()[1], is(before.getTime()));
        assertThat((String) filter.getValues()[2], is("CHECKSUM"));
    }

    @Test
    public void shouldContainNoClauseForNoParams() {
        givenFilterParametersAre(null, null, null);
        assertThat(occurrencesOf("&&", filter.getFilter()), is(0));
        assertThat(occurrencesOf(",", filter.getParameters()), is(0));
    }

    @Test
    public void shouldContainOneKeyForListOfSize1() {
        shouldContainNKeysForListOfSizeN(1);
    }

    @Test
    public void shouldContainThreeKeysForListOfSize3() {
        shouldContainNKeysForListOfSizeN(3);
    }

    @Test
    public void shouldContainTwoClausesForTwoParams() {
        Date before = new Date(System.currentTimeMillis());
        givenFilterParametersAre(null, before, "CHECKSUM");
        assertThat(filter.getFilter().startsWith("&&"), is(false));
        assertThat(filter.getFilter().endsWith("&&"), is(false));
        assertThat(occurrencesOf("&&", filter.getFilter()), is(1));
        assertThat(filter.getParameters().startsWith(","), is(false));
        assertThat(filter.getParameters().endsWith(","), is(false));
        assertThat(occurrencesOf(",", filter.getParameters()), is(1));
        assertThat(filter.getValues().length, is(2));
        assertThat((Long) filter.getValues()[0], is(before.getTime()));
        assertThat((String) filter.getValues()[1], is("CHECKSUM"));
    }

    @Test
    public void shouldReturnEmptyFilterIfAllParamsAreNull() {
        givenFilterParametersAre(null, null, null);
        assertThat(filter.getFilter(), is((String) null));
        assertThat(filter.getParameters(), is((String) null));
        assertThat(filter.getValues(), is((Object) null));
    }

    @Test
    public void shouldReturnEmptyFilterIfListIsEmpty() {
        shouldContainNKeysForListOfSizeN(0);
    }

    private void givenFilterParametersAre(Date after, Date before, String type) {
        filter = AlarmJDOUtils.getFilter(after, before, type,
                        null, null, null);
    }

    private void givenSizeOfLogEntryListIs(int size) {
        for (int i = 0; i < size; i++) {
            LogEntry alarm = new LogEntry();
            alarm.setFirstArrived(System.currentTimeMillis());
            alarm.setKey("" + i);
            alarms.add(alarm);
        }
        filter = AlarmJDOUtils.getIdFilter(alarms);
    }

    private int occurrencesOf(String subsequence, String sequence) {
        if (sequence == null) {
            return 0;
        }
        int occurrences = 0;
        int index = 0;
        while (true) {
            index = sequence.indexOf(subsequence);
            if (index == -1) {
                break;
            }
            occurrences++;
            sequence = sequence.substring(index + subsequence.length());
        }
        return occurrences;
    }

    private void shouldContainNKeysForListOfSizeN(int N) {
        givenSizeOfLogEntryListIs(N);
        if (N > 0) {
            assertThat(occurrencesOf("||", filter.getFilter()), is(N - 1));
            assertThat(occurrencesOf(",", filter.getParameters()), is(N - 1));
            assertThat(filter.getValues().length, is(N));
            for (int i = 0; i < N; i++) {
                assertThat((String) filter.getValues()[i], is("" + i));
            }
            if (N > 1) {
                assertThat(filter.getFilter().startsWith("||"), is(false));
                assertThat(filter.getFilter().endsWith("||"), is(false));
                assertThat(filter.getParameters().startsWith(","), is(false));
                assertThat(filter.getParameters().endsWith(","), is(false));
            }
        } else {
            assertThat(filter.getFilter(), is((String) null));
            assertThat(filter.getParameters(), is((String) null));
            assertThat(filter.getValues(), is((Object) null));
        }
    }
}
