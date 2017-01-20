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

import diskCacheV111.util.CacheException;
import org.dcache.resilience.util.ForegroundBackgroundAllocator.ForegroundBackgroundAllocation;

import static org.junit.Assert.assertEquals;

public class QueueAllocatorTest {
    private static final String FORMAT =
                    "total threads %s, running %s, foreground queue %s, "
                                    + "background queue %s, max %s: incorrect %s result";

    private static final long[][] EDGE_CASES = {
                    { 200, 200000, 200000, 0, 0 },
                    { 0, 0, 0, 0, 0 },
                    { 0, 200000, 0, 200, 0 },
                    { 0, 0, 200000, 0, 200 },
                    { 0, 200000, 1, 199, 1 },
                    { 0, 1, 200000, 1, 199 },
                    { 199, 200000, 1, 1, 0 },
                    { 199, 1, 200000, 1, 0 },
                    { 198, 1, 200000, 1, 1 },
                    { 198, 200000, 1, 1, 1 },
                    { 100, 200000, 1, 99, 1 },
                    { 100, 1, 200000, 1, 99 },
                    { 2, 200000, 1, 197, 1 },
                    { 2, 1, 200000, 1, 197 },
                    { 1, 200000, 1, 198, 1 },
                    { 1, 1, 200000, 1, 198 },
                    { 0, 200000, 200000, 100, 100 },
                    { 199, 200000, 200000, 1, 0 },
                    { 198, 200000, 200000, 1, 1 },
                    { 100, 200000, 200000, 50, 50 },
                    { 2, 200000, 200000, 99, 99 },
                    { 1, 200000, 200000, 100, 99 },
                    };

    private static final long NUM_THREADS = 200L;

    ForegroundBackgroundAllocator allocator;

    @Before
    public void setUp() throws CacheException {
        allocator = new StandardForegroundBackgroundAllocator();
    }

    @Test
    public void testEdgeCasesWithMax50Percent() {
        testEdgeCases(0.5);
    }

    @Test
    public void testEdgeCasesWithMax80Percent() {
        testEdgeCases(0.8);
    }

    @Test
    public void testProportionGreaterThanMaxForBg() {
        testCase(100, 100, 500, 0.8, 20, 80);
    }

    @Test
    public void testProportionGreaterThanMaxForFg() {
        testCase(100, 500, 100, 0.8, 80, 20);
    }

    @Test
    public void testProportionLessThanMaxForBg() {
        testCase(100, 100, 500, 0.9, 17, 83);
    }

    @Test
    public void testProportionLessThanMaxForFg() {
        testCase(100, 500, 100, 0.9, 83, 17);
    }

    private void testCase(long running,
                          long foreground,
                          long background,
                          double maxAllocation,
                          long correctFgResult,
                          long correctBgResult) {
        ForegroundBackgroundAllocation allocation = allocator.allocate(
                        NUM_THREADS, running, foreground, background,
                        maxAllocation);

        assertEquals(String.format(FORMAT, NUM_THREADS, running, foreground,
                                   background, maxAllocation, "foreground"),
                     correctFgResult, allocation.getForeground());

        assertEquals(String.format(FORMAT, NUM_THREADS, running, foreground,
                                   background, maxAllocation, "background"),
                     correctBgResult, allocation.getBackground());
    }

    private void testEdgeCases(double maxAllocation) {
        for (int i = 0; i < EDGE_CASES.length; ++i) {
            testCase(EDGE_CASES[i][0],
                     EDGE_CASES[i][1],
                     EDGE_CASES[i][2],
                     maxAllocation,
                     EDGE_CASES[i][3],
                     EDGE_CASES[i][4]);
        }
    }
}
