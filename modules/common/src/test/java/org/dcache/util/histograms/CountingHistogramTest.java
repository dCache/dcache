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
the Government and in its own protected capacity and is not acting on
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
package org.dcache.util.histograms;

import org.apache.commons.math3.util.FastMath;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public final class CountingHistogramTest extends HistogramModelTest {
    @Test
    public void binUnitShouldBe1ForMaxValue50Days()
                    throws Exception {
        givenCountingHistogram();
        givenFilelifetimeValuesFor(50);
        givenBinCountOf(51);
        givenBinUnitOf((double) TimeUnit.DAYS.toMillis(1));
        givenBinLabelOf(TimeUnit.DAYS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("File Lifetime Count");
        whenConfigureIsCalled();
        assertThatBuildSucceeded();
        assertThatBinWidthIs(1);
    }

    @Test
    public void binUnitShouldBe2ForMaxValue100Days()
                    throws Exception {
        givenCountingHistogram();
        givenFilelifetimeValuesFor(100);
        givenBinCountOf(51);
        givenBinUnitOf((double) TimeUnit.DAYS.toMillis(1));
        givenBinLabelOf(TimeUnit.DAYS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("File Lifetime Count");
        whenConfigureIsCalled();
        assertThatBuildSucceeded();
        assertThatBinWidthIs(2);
    }

    @Test
    public void binUnitShouldBe3ForMaxValue101Days()
                    throws Exception {
        givenCountingHistogram();
        givenFilelifetimeValuesFor(101);
        givenBinCountOf(51);
        givenBinUnitOf((double) TimeUnit.DAYS.toMillis(1));
        givenBinLabelOf(TimeUnit.DAYS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("File Lifetime Count");
        whenConfigureIsCalled();
        assertThatBuildSucceeded();
        assertThatBinWidthIs(3);
    }

    @Test
    public void buildShouldFailWhenNoCountGivenToCounting()
                    throws Exception {
        givenCountingHistogram();
        givenFilelifetimeValuesFor(150);
        givenBinUnitOf((double) TimeUnit.DAYS.toMillis(1));
        givenBinLabelOf(TimeUnit.DAYS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("File Lifetime Count");
        whenConfigureIsCalled();
        assertThatBuildFailed();
    }

    @Test
    public void buildShouldFailWhenNoDataGivenToCountingHistogram()
                    throws Exception {
        givenCountingHistogram();
        whenConfigureIsCalled();
        assertThatBuildFailed();
    }

    @Test
    public void buildShouldFailWhenNoUnitGivenToCountingHistogram()
                    throws Exception {
        givenCountingHistogram();
        givenFilelifetimeValuesFor(150);
        givenBinCountOf(51);
        givenBinLabelOf(TimeUnit.DAYS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("File Lifetime Count");
        whenConfigureIsCalled();
        assertThatBuildFailed();
    }

    private void assertThatBinWidthIs(int numUnits) {
        assertEquals("wrong binWidth",
                     numUnits, (int) model.binWidth);
    }

    private void assertThatBuildFailed() {
        assertNotNull("Build succeeded but should have failed", error);
        model = null;
    }

    private List<Double> getRawLifetimes(int days) {
        List<Double> lifetimes = new ArrayList<>();
        long radix = TimeUnit.DAYS.toMillis(days);
        /*
         *  Ensure that the max is there so
         *  the assertion always works, regardless
         *  of the other random values.
         */
        lifetimes.add((double) radix);
        for (int i = 1; i < days; ++i) {
            long duration = FastMath.abs(RANDOM.nextLong()) % radix;
            lifetimes.add((double) duration);
        }
        return lifetimes;
    }

    private void givenCountingHistogram() {
        if (model == null) {
            model = new CountingHistogram();
        } else {
            model = new CountingHistogram((CountingHistogram) model);
        }
    }

    private void givenFilelifetimeValuesFor(int units) {
        model.setData(getRawLifetimes(units));
    }
}
