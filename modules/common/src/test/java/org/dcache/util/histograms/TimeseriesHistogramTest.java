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

import com.google.gson.GsonBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.dcache.util.histograms.TimeFrame.BinType;
import org.dcache.util.histograms.TimeFrame.Type;

import static junit.framework.TestCase.assertEquals;
import static org.dcache.util.ByteUnit.GiB;
import static org.junit.Assert.assertTrue;

public final class TimeseriesHistogramTest extends HistogramModelTest {

    TimeseriesHistogram timeseriesHistogram;
    HistogramModel      originalModel;
    long                now;

    @Test
    public void buildShouldSucceedForTimeseriesHistogramWithFileSizeValues()
                    throws Exception {
        givenTimeseriesHistogram();
        givenFileSizeValuesFor(24);
        givenValidTimeframeSpecification();
        whenConfigureIsCalled();
        assertThatBuildSucceeded();
    }

    @Test
    public void buildShouldSucceedForTimeseriesHistogramWithoutData()
                    throws Exception {
        givenTimeseriesHistogram();
        givenBinUnitOf((double) TimeUnit.HOURS.toMillis(1));
        givenBinCountOf(48);
        givenBinLabelOf(TimeUnit.HOURS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("Queued Movers");
        givenHighestBinOf(getHoursInThePastFromNow(0));
        whenConfigureIsCalled();
        assertThatBuildSucceeded();
    }

    @Test
    public void buildShouldSucceedForTimeseriesHistogramWithoutFileSizeValues()
                    throws Exception {
        givenTimeseriesHistogram();
        givenValidTimeframeSpecification();
        whenConfigureIsCalled();
        assertThatBuildSucceeded();
    }

    @Test
    public void rebuiltTimeseriesHistogramShouldBeTheSameAsOriginal()
                    throws Exception {
        givenTimeseriesHistogram();
        givenQueueCountValuesFor(48);
        givenBinUnitOf((double) TimeUnit.HOURS.toMillis(1));
        givenBinCountOf(48);
        givenBinLabelOf(TimeUnit.HOURS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("Queued Movers");
        givenHighestBinOf(getHoursInThePastFromNow(0));
        whenConfigureIsCalled();
        whenHistogramIsStored();
        givenTimeseriesHistogram();
        whenConfigureIsCalled();
        assertThatOriginalHistogramEqualsStored();
    }

    @Test
    public void serializedTimeseriesHistogramShouldDeserializeCorrectly()
                    throws Exception {
        givenTimeseriesHistogram();
        givenQueueCountValuesFor(48);
        givenBinUnitOf((double) TimeUnit.HOURS.toMillis(1));
        givenBinCountOf(48);
        givenBinLabelOf(TimeUnit.HOURS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("Queued Movers");
        givenHighestBinOf(getHoursInThePastFromNow(0));
        whenConfigureIsCalled();
        whenHistogramIsSerialized();
        whenHistogramIsDeserialized();
        whenHistogramIsSerializedAgain();
        assertThatSerializationsAreEqual();
    }

    @Test
    public void updateOnTimeseriesHistogramShouldAverageLastValue()
                    throws Exception {
        givenTimeseriesHistogram();
        givenQueueCountValuesFor(48);
        givenBinUnitOf((double) TimeUnit.HOURS.toMillis(1));
        givenBinCountOf(48);
        givenBinLabelOf(TimeUnit.HOURS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("Queued Movers");
        givenHighestBinOf(getHoursInThePastFromNow(0));
        whenConfigureIsCalled();
        assertThatUpdateAveragesLastValue();
    }

    @Test
    public void updateOnTimeseriesHistogramShouldReplaceLastValue()
                    throws Exception {
        givenTimeseriesHistogram();
        givenQueueCountValuesFor(48);
        givenBinUnitOf((double) TimeUnit.HOURS.toMillis(1));
        givenBinCountOf(48);
        givenBinLabelOf(TimeUnit.HOURS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("Queued Movers");
        givenHighestBinOf(getHoursInThePastFromNow(0));
        whenConfigureIsCalled();
        assertThatUpdateReplacesLastValue();
    }

    @Test
    public void updateOnTimeseriesHistogramShouldRotateBuffer()
                    throws Exception {
        givenTimeseriesHistogram();
        givenQueueCountValuesFor(48);
        givenBinUnitOf((double) TimeUnit.HOURS.toMillis(1));
        givenBinCountOf(48);
        givenBinLabelOf(TimeUnit.HOURS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("Queued Movers");
        givenHighestBinOf(getHoursInThePastFromNow(3));
        whenConfigureIsCalled();
        assertThatUpdateRotatesBuffer(3);
    }

    @Test
    public void updateOnTimeseriesHistogramShouldRotateBufferToMaximum()
                    throws Exception {
        givenTimeseriesHistogram();
        givenQueueCountValuesFor(48);
        givenBinUnitOf((double) TimeUnit.HOURS.toMillis(1));
        givenBinCountOf(48);
        givenBinLabelOf(TimeUnit.HOURS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("Queued Movers");
        givenHighestBinOf(getHoursInThePastFromNow(53));
        whenConfigureIsCalled();
        assertThatUpdateRotatesBuffer(53);
    }

    @Test
    public void updateOnTimeseriesHistogramShouldSumLastValue()
                    throws Exception {
        givenTimeseriesHistogram();
        givenQueueCountValuesFor(48);
        givenBinUnitOf((double) TimeUnit.HOURS.toMillis(1));
        givenBinCountOf(48);
        givenBinLabelOf(TimeUnit.HOURS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("Queued Movers");
        givenHighestBinOf(getHoursInThePastFromNow(0));
        whenConfigureIsCalled();
        assertThatUpdateSumsLastValue();
    }

    private void assertThatOriginalHistogramEqualsStored() {
        String original = new GsonBuilder().setPrettyPrinting()
                                           .disableHtmlEscaping()
                                           .create().toJson(originalModel);
        String restored = new GsonBuilder().setPrettyPrinting()
                                           .disableHtmlEscaping()
                                           .create().toJson(model);
        assertEquals("object strings are not the same",
                     original,
                     restored);
    }

    private void assertThatSerializationsAreEqual() {
        assertEquals("serializations are not equal",
                     serialized1,
                     serialized2);
    }

    private void assertThatUpdateAveragesLastValue() {
        Double[][] values = values();
        double lastValue = values[values.length - 1][1];
        double newValue = lastValue + 1.0;
        double average = (lastValue + newValue) / 2;
        timeseriesHistogram.average(newValue, now);
        values = values();
        assertTrue("value was not replaced",
                   lastValue != values[values.length - 1][1]);
        assertTrue("last value was replaced with faulty value",
                   average == values[values.length - 1][1]);
    }

    private void assertThatUpdateReplacesLastValue() {
        Double[][] values = values();
        double lastValue = values[values.length - 1][1];
        double newValue = lastValue + 1.0;
        timeseriesHistogram.replace(newValue, now);
        values = values();
        assertTrue("value was not replaced",
                   lastValue != values[values.length - 1][1]);
        assertTrue("last value was replaced with faulty value",
                   newValue == values[values.length - 1][1]);
    }

    private void assertThatUpdateRotatesBuffer(int units) {
        Double[][] originals = values();

        timeseriesHistogram.average(2.53, now);
        timeseriesHistogram.average(5.71, now);
        Double[][] rotated = values();
        assertTrue("new buffer has wrong size",
                   originals.length == rotated.length);

        int r = 0;

        for (int i = units; i < originals.length; ++i) {
            assertTrue("rotated value " + r + " is incorrect",
                       originals[i][1] == rotated[r][1]);
            ++r;
        }

        for (; r < rotated.length - 1; ++r) {
            assertTrue("rotated value " + r + " is incorrect",
                       null == rotated[r][1]);
        }

        assertTrue("last rotated value is incorrect",
                   rotated[rotated.length - 1][1] == 4.12);

        HistogramMetadata metadata = model.metadata;
        int[] counts = metadata.getBinCounts();
        assertTrue("last rotated count is incorrect",
                   counts[metadata.rotatedIndex(rotated.length - 1)] == 2);
    }

    private void assertThatUpdateSumsLastValue() {
        Double[][] values = values();
        double lastValue = values[values.length - 1][1];
        double newValue = lastValue + 1.0;
        timeseriesHistogram.add(newValue, now);
        values = values();
        assertTrue("value was not replaced",
                   lastValue != values[values.length - 1][1]);
        assertTrue("last value was replaced with faulty value",
                   newValue + lastValue == values[values.length - 1][1]);
    }

    private double getHoursInThePastFromNow(int hours) {
        Calendar calendar = Calendar.getInstance();
        long diff = TimeUnit.HOURS.toMillis(hours);
        calendar.setTimeInMillis(now - diff);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return (double) calendar.getTimeInMillis();
    }

    private List<Double> getQueueCountsFor(int units) {
        List<Double> queue = new ArrayList<>();
        for (int i = 0; i < units; ++i) {
            queue.add(0.0);
        }
        return queue;
    }

    private List<Double> getRawBytesFor(int units) {
        List<Double> bytes = new ArrayList<>();
        double size = GiB.toBytes(1.0);
        for (int i = 0; i < units; ++i) {
            bytes.add(size);
        }
        return bytes;
    }

    private void givenFileSizeValuesFor(int units) {
        model.setData(getRawBytesFor(units));
    }

    private void givenHighestBinOf(double bin) {
        model.setHighestBin(bin);
    }

    private void givenQueueCountValuesFor(int units) {
        model.setData(getQueueCountsFor(units));
    }

    private void givenTimeseriesHistogram() {
        if (timeseriesHistogram == null) {
            timeseriesHistogram = new TimeseriesHistogram();
        } else {
            timeseriesHistogram = new TimeseriesHistogram(timeseriesHistogram);
        }

        model = timeseriesHistogram;
        now = System.currentTimeMillis();
    }

    private void givenValidTimeframeSpecification() {
        Calendar high = TimeFrame.computeHighTimeFromNow(BinType.HOUR);
        TimeFrame timeFrame = new TimeFrame(high.getTimeInMillis());
        timeFrame.setTimebin(BinType.HOUR);
        timeFrame.setTimeframe(Type.DAY);
        ((TimeseriesHistogram) model).withTimeFrame(timeFrame);
        model.setDataUnitLabel("BYTES");
    }

    private Double[][] values() {
        Histogram h = model.toHistogram();
        return h.getValues();
    }

    private void whenHistogramIsDeserialized() {
        model = new GsonBuilder().setPrettyPrinting()
                                 .disableHtmlEscaping()
                                 .create()
                                 .fromJson(serialized1, model.getClass());
    }

    private void whenHistogramIsSerialized() {
        serialized1 = new GsonBuilder().setPrettyPrinting()
                                       .disableHtmlEscaping()
                                       .create()
                                       .toJson(model);
    }

    private void whenHistogramIsSerializedAgain() {
        serialized2 = new GsonBuilder().setPrettyPrinting()
                                       .disableHtmlEscaping()
                                       .create()
                                       .toJson(model);
    }

    private void whenHistogramIsStored() throws Exception {
        originalModel = model;
    }
}
