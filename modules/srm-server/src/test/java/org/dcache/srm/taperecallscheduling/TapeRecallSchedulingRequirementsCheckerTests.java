/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.srm.taperecallscheduling;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.OptionalLong;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BringOnlineFileRequest.class})
@PowerMockIgnore({"com.sun.org.apache.xerces.*",
      "javax.xml.*", "org.xml.*", "org.w3c.*", "jdk.xml.*"})
public class TapeRecallSchedulingRequirementsCheckerTests {

    /**
     * time safety margin in milliseconds
     */
    private static final long TIME_SAFETY_MARGIN = 20;
    TapeRecallSchedulingRequirementsChecker requirementsChecker;

    private long getNewCtime() {
        return System.currentTimeMillis() - TIME_SAFETY_MARGIN;
    }

    private long getExpiredCtime() {
        long age =
              requirementsChecker.maxJobWaitingTime() + SECONDS.toMillis(1) + TIME_SAFETY_MARGIN;
        long res = System.currentTimeMillis() - age;
        return res;
    }

    private long getExpiredTapeinfolessCtime() {
        OptionalLong tapeinfolessTime = requirementsChecker.tapeinfolessJobWaitingTime();
        if (tapeinfolessTime.isEmpty()) {
            return getExpiredCtime();
        }
        long age = tapeinfolessTime.getAsLong() + SECONDS.toMillis(1) + TIME_SAFETY_MARGIN;
        long res = System.currentTimeMillis() - age;
        return res;
    }

    @Before
    public void setup() {
        requirementsChecker = new TapeRecallSchedulingRequirementsChecker();
        requirementsChecker.setMaxActiveTapes(1);
        requirementsChecker.setMinTapeRecallPercentage(80);
        requirementsChecker.setMinNumberOfRequestsForTapeSelection(1);
        requirementsChecker.setMinJobWaitingTime(Duration.ofMinutes(0));
        requirementsChecker.setMaxJobWaitingTime(Duration.ofHours(1));
    }

    @Test
    public void shouldHandleMissingTapeInfoAsInsufficientIfNotZeroPercentRequired() {
        requirementsChecker.setMinTapeRecallPercentage(80);
        SchedulingInfoTape sit = new SchedulingInfoTape();

        assertFalse(requirementsChecker.isTapeRecallVolumeSufficient(sit, 30L));
        assertFalse(requirementsChecker.isTapeRecallVolumeSufficient(sit, 80L));
    }

    @Test
    public void shouldHandleMissingTapeInfoAsSufficientIfZeroPercentRequired() {
        requirementsChecker.setMinTapeRecallPercentage(0);
        SchedulingInfoTape sit = new SchedulingInfoTape();

        assertTrue(requirementsChecker.isTapeRecallVolumeSufficient(sit, 30L));
        assertTrue(requirementsChecker.isTapeRecallVolumeSufficient(sit, 80L));
    }

    @Test
    public void evaluatesSufficientRelativeTapeRecallVolumeCorrectly() {
        requirementsChecker.setMinTapeRecallPercentage(80);

        SchedulingInfoTape sit = new SchedulingInfoTape();
        sit.addTapeInfo(100, 40);

        assertFalse(requirementsChecker.isTapeRecallVolumeSufficient(sit, 30L));
        assertTrue(requirementsChecker.isTapeRecallVolumeSufficient(sit, 40L));
        assertTrue(requirementsChecker.isTapeRecallVolumeSufficient(sit, 80L));
    }

    @Test
    public void evaluatesSufficientTapeRecallVolumeCorrectly() {
        requirementsChecker.setMinTapeRecallPercentage(80);

        SchedulingInfoTape sit = new SchedulingInfoTape();
        sit.addTapeInfo(100, 100);

        assertFalse(requirementsChecker.isTapeRecallVolumeSufficient(sit, 70L));
        assertTrue(requirementsChecker.isTapeRecallVolumeSufficient(sit, 81L));
    }

    @Test
    public void testRequestCountSufficient() {
        requirementsChecker.setMinNumberOfRequestsForTapeSelection(1);
        assertTrue(requirementsChecker.isRequestCountSufficient(10));
    }

    @Test
    public void testRequestCountInsufficient() {
        requirementsChecker.setMinNumberOfRequestsForTapeSelection(100);
        assertFalse(requirementsChecker.isRequestCountSufficient(10));
    }

    @Test
    public void assessJobExpiry() {
        requirementsChecker.setMaxJobWaitingTime(Duration.ofSeconds(50));

        SchedulingItemJob j1 = new SchedulingItemJob(10, "/tape/file10.txt",
              getExpiredCtime());
        SchedulingItemJob j2 = new SchedulingItemJob(11, "/tape/file11.txt",
              getNewCtime());

        assertTrue(requirementsChecker.isJobExpired(j1));
        assertFalse(requirementsChecker.isJobExpired(j2));
    }

    @Test
    public void assessTapeinfolessJobExpiry() {
        requirementsChecker.setMaxJobWaitingTime(Duration.ofSeconds(50));
        requirementsChecker.setTapeinfolessJobWaitingTime(Duration.ofSeconds(5));

        SchedulingItemJob j1 = new SchedulingItemJob(10, "/tape/file10.txt",
              getExpiredTapeinfolessCtime());
        SchedulingItemJob j2 = new SchedulingItemJob(11, "/tape/file11.txt",
              getNewCtime());

        assertTrue(requirementsChecker.isTapeinfolessJobExpired(j1));
        assertFalse(requirementsChecker.isTapeinfolessJobExpired(j2));
    }

    @Test
    public void assessTapeinfolessJobExpiryWhenDisabled() {
        requirementsChecker.setTapeinfolessJobWaitingTime(Duration.ofSeconds(-1));
        requirementsChecker.setMaxJobWaitingTime(Duration.ofSeconds(50));

        SchedulingItemJob j1 = new SchedulingItemJob(10, "/tape/file10.txt",
              getExpiredCtime());
        SchedulingItemJob j2 = new SchedulingItemJob(11, "/tape/file11.txt",
              getNewCtime());

        assertTrue(requirementsChecker.isTapeinfolessJobExpired(j1));
        assertFalse(requirementsChecker.isTapeinfolessJobExpired(j2));
    }

    @Test
    public void returnOldestTapeJobNotExpiredWhenNoOldestForTapeExists() {
        requirementsChecker.setMaxJobWaitingTime(Duration.ofSeconds(50));
        SchedulingInfoTape sit = new SchedulingInfoTape();

        assertFalse(requirementsChecker.isOldestTapeJobExpired(sit));
    }

    @Test
    public void returnNewestTapeJobNotExpiredWhenNoNewestForTapeExists() {
        requirementsChecker.setMaxJobWaitingTime(Duration.ofSeconds(50));
        SchedulingInfoTape sit = new SchedulingInfoTape();

        assertFalse(requirementsChecker.isNewestTapeJobOldEnough(sit));
    }

    @Test
    public void returnOldestTapeJobExpired() {
        requirementsChecker.setMaxJobWaitingTime(Duration.ofSeconds(50));
        SchedulingInfoTape sit = new SchedulingInfoTape();
        sit.setOldestJobArrival(getExpiredCtime());

        assertTrue(requirementsChecker.isOldestTapeJobExpired(sit));
    }

    @Test
    public void returnNewestTapeJobExpired() {
        requirementsChecker.setMaxJobWaitingTime(Duration.ofSeconds(50));
        SchedulingInfoTape sit = new SchedulingInfoTape();
        sit.setNewestJobArrival(getExpiredCtime());

        assertTrue(requirementsChecker.isNewestTapeJobOldEnough(sit));
    }

    @Test
    public void shouldCompareOldestTapeRequestAgeIfOneIsNotSet() {
        SchedulingInfoTape tapeInfo1 = new SchedulingInfoTape();
        tapeInfo1.addTapeInfo(100, 100);
        tapeInfo1.setOldestJobArrival(getNewCtime());

        SchedulingInfoTape tapeInfo2 = new SchedulingInfoTape();
        tapeInfo2.addTapeInfo(100, 100);

        assertEquals(1, requirementsChecker.compareOldestTapeRequestAge(tapeInfo1, tapeInfo2));
    }

    @Test
    public void shouldCompareOldestTapeRequestAgeIfBothAreNotSet() {
        SchedulingInfoTape tapeInfo1 = new SchedulingInfoTape();
        tapeInfo1.addTapeInfo(100, 100);

        SchedulingInfoTape tapeInfo2 = new SchedulingInfoTape();
        tapeInfo2.addTapeInfo(100, 100);

        assertEquals(0, requirementsChecker.compareOldestTapeRequestAge(tapeInfo1, tapeInfo2));
    }

}
