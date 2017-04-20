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
package org.dcache.util.histograms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.math3.util.FastMath;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.dcache.util.histograms.HistogramModel.UpdateOperation;
import org.dcache.util.histograms.TimeFrame.BinType;
import org.dcache.util.histograms.TimeFrame.Type;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertTrue;

public class HistogramModelTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    HistogramModelTest.class);

    private static final Random RANDOM = new Random(System.currentTimeMillis());

    private HistogramModel   model;
    private HistogramModel   originalModel;
    private Exception        error;
    private String           serialized1;
    private String           serialized2;

    @Test
    public void binUnitShouldBe1ForMaxValue50Days()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
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
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
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
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
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
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
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
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenCountingHistogram();
        whenConfigureIsCalled();
        assertThatBuildFailed();
    }

    @Test
    public void buildShouldFailWhenNoUnitGivenToCountingHistogram()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenCountingHistogram();
        givenFilelifetimeValuesFor(150);
        givenBinCountOf(51);
        givenBinLabelOf(TimeUnit.DAYS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("File Lifetime Count");
        whenConfigureIsCalled();
        assertThatBuildFailed();
    }

    @Test
    public void buildShouldSucceedForTimeframeHistogramWithoutData()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenTimeframeHistogram();
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
    public void buildShouldSucceedForTimeframeHistogramWithoutFileSizeValues()
                    throws JsonProcessingException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException,
                    InvocationTargetException {
        givenTimeframeHistogram();
        givenValidTimeframeSpecification();
        whenConfigureIsCalled();
        assertThatBuildSucceeded();
    }

    @Test
    public void buildShouldSucceedForTimeframeHistogramWithFileSizeValues()
                    throws JsonProcessingException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException,
                    InvocationTargetException {
        givenTimeframeHistogram();
        givenFileSizeValuesFor(24);
        givenValidTimeframeSpecification();
        whenConfigureIsCalled();
        assertThatBuildSucceeded();
    }

    @After
    public void printDiagnostics() throws JsonProcessingException {
        if (model != null) {
            LOGGER.info(new ObjectMapper().writerWithDefaultPrettyPrinter()
                                          .writeValueAsString(model));
            LOGGER.info(new ObjectMapper().writerWithDefaultPrettyPrinter()
                                          .writeValueAsString(model.toHistogram()));
        }
    }

    @Test
    public void rebuiltTimeframeHistogramShouldBeTheSameAsOriginal()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenTimeframeHistogram();
        givenQueueCountValuesFor(48);
        givenBinUnitOf((double) TimeUnit.HOURS.toMillis(1));
        givenBinCountOf(48);
        givenBinLabelOf(TimeUnit.HOURS.name());
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("Queued Movers");
        givenHighestBinOf(getHoursInThePastFromNow(0));
        whenConfigureIsCalled();
        whenHistogramIsStored();
        givenTimeframeHistogram();
        whenConfigureIsCalled();
        assertThatOriginalHistogramEqualsStored();
    }

    @Test
    public void serializedTimeframeHistogramShouldDeserializeCorrectly()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenTimeframeHistogram();
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
    public void updateOnTimeframeHistogramShouldAverageLastValue()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenTimeframeHistogram();
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
    public void updateOnTimeframeHistogramShouldReplaceLastValue()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenTimeframeHistogram();
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
    public void updateOnTimeframeHistogramShouldRotateBuffer()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenTimeframeHistogram();
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
    public void updateOnTimeframeHistogramShouldRotateBufferToMaximum()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenTimeframeHistogram();
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
    public void updateOnTimeframeHistogramShouldSumLastValue()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenTimeframeHistogram();
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

    @Test
    public void updateShouldFailOnCountingHistogram()
                    throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        givenCountingHistogram();
        givenFilelifetimeValuesFor(150);
        givenBinCountOf(51);
        givenBinUnitOf((double) TimeUnit.DAYS.toMillis(1));
        givenBinLabelOf(TimeUnit.DAYS.name());
        givenDataLabelOf("Count");
        givenDataLabelOf("COUNT");
        givenHistogramTypeOf("File Lifetime Count");
        whenConfigureIsCalled();
        assertThatBuildSucceeded();
        assertThatUpdateFails();
    }

    private void assertThatBinWidthIs(int numUnits) {
        assertEquals("wrong binWidth",
                     numUnits, (int) model.binWidth);
    }

    private void assertThatBuildFailed() {
        assertNotNull("Build succeeded but should have failed", error);
        model = null;
    }

    private void assertThatBuildSucceeded() {
        String msg = error == null ? null : error.getMessage();
        assertNull(msg, error);
    }

    private void assertThatOriginalHistogramEqualsStored() {
        try {
            assertTrue(model.getClass().equals(
                            originalModel.getClass()));
            String original = new ObjectMapper()
                            .writeValueAsString(originalModel);
            String restored = new ObjectMapper()
                            .writeValueAsString(model);
            assertEquals("object strings are not the same",
                         original,
                         restored);
        } catch (JsonProcessingException e) {
            assertNull(e.getMessage(), e);
        }
    }

    private void assertThatSerializationsAreEqual() {
        assertEquals("serializations are not equal",
                     serialized1,
                     serialized2);
    }

    private void assertThatUpdateAveragesLastValue() {
        Double[][] values = values();
        Double[] last = values[values.length - 1];
        double lastValue = last[1];
        double newValue = lastValue * 3;
        double average = (lastValue + newValue) / 2;
        model.update(newValue,
                     UpdateOperation.AVERAGE,
                     System.currentTimeMillis());
        values = values();
        assertTrue("value was not replaced",
                   lastValue != values[values.length - 1][1]);
        assertTrue("last value was replaced with faulty value",
                   average == values[values.length - 1][1]);
    }

    private void assertThatUpdateFails() {
        Exception thrown = null;

        try {
            model.update(1.243,
                         UpdateOperation.REPLACE,
                         System.currentTimeMillis());
        } catch (UnsupportedOperationException e) {
            thrown = e;
        }

        assertNotNull(thrown);
        assertTrue(thrown instanceof UnsupportedOperationException);
    }

    private void assertThatUpdateReplacesLastValue() {
        Double[][] values = values();
        Double[] last = values[values.length - 1];
        double lastValue = last[1];
        double newValue = lastValue * 3;
        model.update(newValue,
                     UpdateOperation.REPLACE,
                     System.currentTimeMillis());
        values = values();
        assertTrue("value was not replaced",
                   lastValue != values[values.length - 1][1]);
        assertTrue("last value was replaced with faulty value",
                   newValue == values[values.length - 1][1]);
    }

    private void assertThatUpdateRotatesBuffer(int units) {
        Double[][] originals = values();
        long now = System.currentTimeMillis();

        model.update(2.53,
                     UpdateOperation.AVERAGE, now);
        model.update(5.71,
                     UpdateOperation.AVERAGE, now);
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
        int [] counts = metadata.getBinCounts();
        assertTrue("last rotated count is incorrect",
                   counts[metadata.rotatedIndex(rotated.length - 1)] == 2);
    }

    private void assertThatUpdateSumsLastValue() {
        Double[][] values = values();
        Double[] last = values[values.length - 1];
        double lastValue = last[1];
        double newValue = lastValue * 3;
        model.update(newValue,
                     UpdateOperation.SUM,
                     System.currentTimeMillis());
        values = values();
        assertTrue("value was not replaced",
                   lastValue != values[values.length - 1][1]);
        assertTrue("last value was replaced with faulty value",
                   newValue + lastValue == values[values.length - 1][1]);
    }

    private double getHoursInThePastFromNow(int hours) {
        Calendar calendar = Calendar.getInstance();
        long diff = TimeUnit.HOURS.toMillis(hours);
        calendar.setTimeInMillis(System.currentTimeMillis() - diff);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return (double) calendar.getTimeInMillis();
    }

    private List<Double> getQueueCountsFor(int units) {
        List<Double> queue = new ArrayList<>();
        int radix = 1000;

        for (int i = 0; i < units; ++i) {
            long count = FastMath.abs(RANDOM.nextLong()) % radix;
            queue.add((double) count);
        }

        return queue;
    }

    private List<Double> getRawBytesFor(int units) {
        List<Double> bytes = new ArrayList<>();
        int radix = 1024;

        for (int i = 0; i < units; ++i) {
            long size = FastMath.abs(RANDOM.nextLong()) % radix;
            size *= 1024 * 1024 * 1024;
            bytes.add((double) size);
        }

        return bytes;
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

    private void givenBinCountOf(int count) {
        model.setBinCount(count);
    }

    private void givenBinLabelOf(String binType) {
        model.setBinUnitLabel(binType);
    }

    private void givenBinUnitOf(double unit) {
        model.setBinUnit(unit);
    }

    private void givenTimeframeHistogram()
                    throws InvocationTargetException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException {
        withHistogramModel(TimeseriesHistogram.class);
    }

    private void givenCountingHistogram()
                    throws InvocationTargetException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException {
        withHistogramModel(CountingHistogram.class);
    }

    private void givenDataLabelOf(String label) {
        model.setDataUnitLabel(label);
    }

    private void givenFileSizeValuesFor(int units) {
        model.setData(getRawBytesFor(units));
    }

    private void givenFilelifetimeValuesFor(int units) {
        model.setData(getRawLifetimes(units));
    }

    private void givenHighestBinOf(double bin) {
        model.setHighestBin(bin);
    }

    private void givenHistogramTypeOf(String type) {
        model.setIdentifier(type);
    }

    private void givenQueueCountValuesFor(int units) {
        model.setData(getQueueCountsFor(units));
    }

    private void givenValidTimeframeSpecification() {
        Calendar high = TimeFrame.computeHighTimeFromNow(BinType.HOUR);
        TimeFrame timeFrame = new TimeFrame(high.getTimeInMillis());
        timeFrame.setTimebin(BinType.HOUR);
        timeFrame.setTimeframe(Type.DAY);
        ((TimeseriesHistogram)model).withTimeFrame(timeFrame);
        model.setDataUnitLabel("BYTES");
    }

    private Double[][] values() {
        Histogram h = model.toHistogram();
        return h.getValues();
    }

    private void whenConfigureIsCalled() {
        try {
            model.setIdentifier("test-histogram");
            model.configure();
        } catch (Exception e) {
            error = e;
        }
    }

    private void whenHistogramIsDeserialized() {
        try {
            model = new ObjectMapper().readValue(serialized1,
                                                 model.getClass());
        } catch (IOException e) {
            assertNull(e.toString(), e);
        }
    }

    private void whenHistogramIsSerialized() {
        try {
            serialized1 = new ObjectMapper().writerWithDefaultPrettyPrinter()
                                            .writeValueAsString(model);
        } catch (JsonProcessingException e) {
            assertNull(e.toString(), e);
        }
    }

    private void whenHistogramIsSerializedAgain() {
        try {
            serialized2 = new ObjectMapper().writerWithDefaultPrettyPrinter()
                                            .writeValueAsString(model);
        } catch (JsonProcessingException e) {
            assertNull(e.toString(), e);
        }
    }

    private void whenHistogramIsStored()
                    throws InvocationTargetException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException {
        originalModel = model;
        withHistogramModel(model.getClass());
    }

    private <T extends HistogramModel> void withHistogramModel(Class<T> clzz)
                    throws IllegalAccessException, InstantiationException,
                    NoSuchMethodException, InvocationTargetException {
        if (model == null) {
            model = clzz.newInstance();
        } else {
            model = clzz.getConstructor(clzz).newInstance(clzz.cast(model));
        }
    }
}