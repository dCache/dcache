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

import org.junit.Test;

import java.util.List;
import java.util.NoSuchElementException;

import org.dcache.util.NonReindexableList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public final class NonReindexableListTest {
    private List<String> testList;

    @Test
    public void shouldFailToInsert() {
        givenListWithNulls();
        whenListContains("A", "B", "C", "D", "E");
        assertThatInsertFails(2, "CC");
    }

    @Test
    public void shouldFailToModify() {
        givenListWithNulls();
        whenListContains("A", "B", "C", "D", "E");
        assertThatModifyFails(2, "CC");
    }

    @Test
    public void shouldReAssignANewIndexToRemovedAndReaddedElement() {
        givenListWithNulls();
        whenListContains("A", "B", "C", "D", "E");
        Integer before = testList.indexOf("B");
        whenRemoved("B");
        whenAdded("B");
        Integer after = testList.indexOf("B");
        assertNotEquals(before, after);
    }

    @Test
    public void shouldMaintainIndicesForRemainingElementsAfterRemoval() {
        givenListWithNulls();
        whenListContains("A", "B", "C", "D", "E");
        Integer before = testList.indexOf("E");
        whenRemoved("B", "D");
        Integer after = testList.indexOf("E");
        assertEquals(before, after);
    }

    @Test
    public void shouldReturnNullForRemovedElementWhenListContainsNulls() {
        givenListWithNulls();
        whenListContains("A", "B", "C", "D", "E");
        Integer before = testList.indexOf("B");
        whenRemoved("B");
        assertNull(testList.get(before));
    }

    @Test
    public void shouldThrowExceptionForRemovedElementWhenListContainsNoNulls() {
        givenListWithNoNulls();
        whenListContains("A", "B", "C", "D", "E");
        Integer before = testList.indexOf("B");
        whenRemoved("B");
        assertNoSuchElementExceptionIsThrownOnGet(before);
    }

    private void assertNoSuchElementExceptionIsThrownOnGet(Integer index) {
        try {
            testList.get(index);
        } catch (Exception e) {
            assertEquals(e.getClass(), NoSuchElementException.class);
        }
    }

    private void assertThatInsertFails(int i, String cc) {
        try {
            testList.add(i, cc);
        } catch (Exception e) {
            assertEquals(e.getClass(), UnsupportedOperationException.class);
        }
    }

    private void assertThatModifyFails(int i, String cc) {
        try {
            testList.set(i, cc);
        } catch (Exception e) {
            assertEquals(e.getClass(), UnsupportedOperationException.class);
        }
    }

    private void givenListWithNoNulls() {
        NonReindexableList list = new NonReindexableList<>();
        list.setIncludeNulls(false);
        testList = list;
    }

    private void givenListWithNulls() {
        NonReindexableList list = new NonReindexableList<>();
        list.setIncludeNulls(true);
        testList = list;
    }

    private void whenAdded(String... values) {
        for (String value : values) {
            testList.add(value);
        }
    }

    private void whenListContains(String... values) {
        for (String value : values) {
            testList.add(value);
        }
    }

    private void whenRemoved(String... values) {
        for (String value : values) {
            testList.remove(value);
        }
    }
}
