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
package org.dcache.srm.scheduler;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.scheduler.strategy.TapeRecallSchedulingStrategy;
import org.dcache.srm.taperecallscheduling.TapeInfo;
import org.dcache.srm.taperecallscheduling.TapeInformant;
import org.dcache.srm.taperecallscheduling.TapeRecallSchedulingRequirementsChecker;
import org.dcache.srm.taperecallscheduling.TapefileInfo;
import org.dcache.srm.taperecallscheduling.spi.TapeInfoProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BringOnlineFileRequest.class})
@PowerMockIgnore({"com.sun.org.apache.xerces.*",
      "javax.xml.*", "org.xml.*", "org.w3c.*", "jdk.xml.*"})
public class TapeRecallSchedulingStrategyTests {

    /**
     * time safety margin in milliseconds
     */
    private static final long TIME_SAFETY_MARGIN = 20;

    TapeRecallSchedulingStrategy strategy;
    TapeRecallSchedulingRequirementsChecker requirementsChecker;
    LocalTapeInfoProvider tapeInfoProvider;

    class LocalTapeInfoProvider implements TapeInfoProvider {

        private HashMap<String, TapeInfo> tapeInfo = new HashMap<>();
        private HashMap<String, TapefileInfo> tapeFileInfo = new HashMap<>();

        public void addTapeInfo(String tape, TapeInfo tapeInfo) {
            this.tapeInfo.put(tape, tapeInfo);
        }

        public void addTapeFileInfo(String file, TapefileInfo tapeFileInfo) {
            this.tapeFileInfo.put(file, tapeFileInfo);
        }

        @Override
        public Map<String, TapeInfo> getTapeInfos(List<String> tapes) {
            return tapeInfo.entrySet().stream()
                  .filter(e -> tapes.contains(e.getKey()))
                  .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        }

        @Override
        public Map<String, TapefileInfo> getTapefileInfos(List<String> fileids) {
            return tapeFileInfo.entrySet()
                  .stream()
                  .filter(e -> fileids.contains(e.getKey()))
                  .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        }

        @Override
        public String describe() {
            return null;
        }

        @Override
        public boolean reload() {
            return false;
        }

    }

    private BringOnlineFileRequest createJob(long jobid, String file, long ctime)
          throws URISyntaxException {
        BringOnlineFileRequest job = PowerMockito.mock(BringOnlineFileRequest.class);
        given(job.getId()).willReturn(Long.valueOf(jobid));
        given(job.getSurl()).willReturn(new URI("srm://localhost:8443" + file));
        given(job.getCreationTime()).willReturn(ctime);
        return job;
    }

    private long getNewCtime() {
        return System.currentTimeMillis() - TIME_SAFETY_MARGIN;
    }

    private long getExpiredCtime() {
        long age =
              requirementsChecker.maxJobWaitingTime() + SECONDS.toMillis(1) + TIME_SAFETY_MARGIN;
        long res = System.currentTimeMillis() - age;
        return res;
    }

    private long getCooldownedCtime() {
        long age = requirementsChecker.minJobWaitingTime() + TIME_SAFETY_MARGIN;
        return System.currentTimeMillis() - age;
    }

    @Before
    public void setup() {
        strategy = new TapeRecallSchedulingStrategy();

        requirementsChecker = new TapeRecallSchedulingRequirementsChecker();
        requirementsChecker.setMaxActiveTapes(1);
        requirementsChecker.setMinTapeRecallPercentage(80);
        requirementsChecker.setMinNumberOfRequestsForTapeSelection(1);
        requirementsChecker.setMinJobWaitingTime(Duration.ofMinutes(0));
        requirementsChecker.setMaxJobWaitingTime(Duration.ofHours(1));

        tapeInfoProvider = new LocalTapeInfoProvider();
        tapeInfoProvider.addTapeInfo("tape1", new TapeInfo(100, 40));
        tapeInfoProvider.addTapeInfo("tape2", new TapeInfo(100, 100));
        tapeInfoProvider.addTapeInfo("tape3", new TapeInfo(100, 70));

        strategy.setRequirementsChecker(requirementsChecker);
        TapeInformant tapeInformant = new TapeInformant();
        tapeInformant.setTapeInfoProvider(tapeInfoProvider);
        strategy.setTapeInformant(tapeInformant);
    }

    @Test
    public void shouldReturnNullWhenQueueIsEmpty() {
        requirementsChecker.setMinJobWaitingTime(Duration.ofMinutes(0));

        assertEquals(0, strategy.size());
        assertEquals(null, strategy.remove());
    }

    @Test
    public void shouldReturnNullWhenOnlyUnexpiredNewJobsExist() throws Exception {
        requirementsChecker.setMinJobWaitingTime(Duration.ofMinutes(2));
        tapeInfoProvider.addTapeFileInfo("/tape/file10.txt", new TapefileInfo(10, "tape1"));

        assertEquals(0, strategy.size());
        strategy.add(createJob(10, "/tape/file10.txt", getNewCtime()));

        assertEquals(1, strategy.size());
        assertEquals(null, strategy.remove());
        assertEquals(1, strategy.size());
    }

    @Test
    public void shouldAddAndRemoveSingleTapeJobCorrectly() throws Exception {
        tapeInfoProvider.addTapeFileInfo("/tape/file10.txt", new TapefileInfo(10, "tape1"));

        assertEquals(0, strategy.size());
        strategy.add(createJob(10, "/tape/file10.txt", getNewCtime()));
        assertEquals(1, strategy.size());

        assertEquals(Long.valueOf(10), strategy.remove());
        assertEquals(0, strategy.size());
    }

    @Test
    public void shouldReturnJobsByLongestTapeQueue() throws Exception {
        tapeInfoProvider.addTapeFileInfo("/tape/file10.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file20.txt", new TapefileInfo(10, "tape2"));
        tapeInfoProvider.addTapeFileInfo("/tape/file21.txt", new TapefileInfo(10, "tape2"));

        assertEquals(0, strategy.size());
        strategy.add(createJob(21, "/tape/file21.txt", getNewCtime()));
        strategy.add(createJob(10, "/tape/file10.txt", getNewCtime()));
        strategy.add(createJob(20, "/tape/file20.txt", getNewCtime()));
        assertEquals(3, strategy.size());

        assertEquals(Long.valueOf(21), strategy.remove());
        assertEquals(Long.valueOf(20), strategy.remove());
        assertEquals(Long.valueOf(10), strategy.remove());

        assertEquals(0, strategy.size());
    }

    @Test
    public void shouldReturnJobsByLongestTwoTapeQueuesInParallel() throws Exception {
        requirementsChecker.setMaxActiveTapes(2);
        tapeInfoProvider.addTapeFileInfo("/tape/file10.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file11.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file12.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file20.txt", new TapefileInfo(10, "tape2"));
        tapeInfoProvider.addTapeFileInfo("/tape/file30.txt", new TapefileInfo(10, "tape3"));
        tapeInfoProvider.addTapeFileInfo("/tape/file31.txt", new TapefileInfo(10, "tape3"));

        assertEquals(0, strategy.size());
        strategy.add(createJob(10, "/tape/file10.txt", getNewCtime()));
        strategy.add(createJob(12, "/tape/file12.txt", getNewCtime()));
        strategy.add(createJob(11, "/tape/file11.txt", getNewCtime()));
        strategy.add(createJob(20, "/tape/file20.txt", getNewCtime()));
        strategy.add(createJob(31, "/tape/file31.txt", getNewCtime()));
        strategy.add(createJob(30, "/tape/file30.txt", getNewCtime()));
        assertEquals(6, strategy.size());

        assertEquals(Long.valueOf(10), strategy.remove());
        assertEquals(Long.valueOf(31), strategy.remove());

        assertEquals(Long.valueOf(12), strategy.remove());
        assertEquals(Long.valueOf(30), strategy.remove());

        assertEquals(Long.valueOf(11), strategy.remove());
        assertEquals(Long.valueOf(20), strategy.remove());

        assertEquals(0, strategy.size());
    }

    @Test
    public void shouldReturnJobsByLongestThreeTapeQueuesInParallel() throws Exception {
        requirementsChecker.setMaxActiveTapes(3);
        tapeInfoProvider.addTapeFileInfo("/tape/file10.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file11.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file12.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file20.txt", new TapefileInfo(10, "tape2"));
        tapeInfoProvider.addTapeFileInfo("/tape/file30.txt", new TapefileInfo(10, "tape3"));
        tapeInfoProvider.addTapeFileInfo("/tape/file31.txt", new TapefileInfo(10, "tape3"));

        assertEquals(0, strategy.size());
        strategy.add(createJob(10, "/tape/file10.txt", getNewCtime()));
        strategy.add(createJob(12, "/tape/file12.txt", getNewCtime()));
        strategy.add(createJob(11, "/tape/file11.txt", getNewCtime()));
        strategy.add(createJob(20, "/tape/file20.txt", getNewCtime()));
        strategy.add(createJob(31, "/tape/file31.txt", getNewCtime()));
        strategy.add(createJob(30, "/tape/file30.txt", getNewCtime()));
        assertEquals(6, strategy.size());

        assertEquals(Long.valueOf(10), strategy.remove());
        assertEquals(Long.valueOf(20), strategy.remove());
        assertEquals(Long.valueOf(31), strategy.remove());

        assertEquals(Long.valueOf(12), strategy.remove());
        assertEquals(Long.valueOf(30), strategy.remove());

        assertEquals(Long.valueOf(11), strategy.remove());

        assertEquals(0, strategy.size());
    }

    @Test
    public void shouldReturnOnlyExpiredNewJob() throws Exception {
        requirementsChecker.setMinNumberOfRequestsForTapeSelection(100);
        requirementsChecker.setMinJobWaitingTime(Duration.ofMinutes(2));

        assertEquals(0, strategy.size());
        strategy.add(createJob(10, "/tape/file10.txt", getNewCtime()));
        strategy.add(createJob(11, "/tape/file11.txt", getExpiredCtime()));
        assertEquals(2, strategy.size());

        assertEquals(Long.valueOf(11), strategy.remove());
        assertEquals(null, strategy.remove());
        assertEquals(1, strategy.size());
    }

    @Test
    public void shouldSelectTapesWithExpiredJobsAndOldestExpiryFirst() throws Exception {
        tapeInfoProvider.addTapeFileInfo("/tape/file10.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file11.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file12.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file20.txt", new TapefileInfo(10, "tape2"));
        tapeInfoProvider.addTapeFileInfo("/tape/file30.txt", new TapefileInfo(10, "tape3"));
        tapeInfoProvider.addTapeFileInfo("/tape/file31.txt", new TapefileInfo(10, "tape3"));

        assertEquals(0, strategy.size());
        strategy.add(createJob(10, "/tape/file10.txt", getNewCtime()));
        strategy.add(createJob(12, "/tape/file12.txt", getNewCtime()));
        strategy.add(createJob(11, "/tape/file11.txt", getNewCtime()));
        strategy.add(createJob(20, "/tape/file20.txt", getExpiredCtime() - 10));
        strategy.add(createJob(31, "/tape/file31.txt", getExpiredCtime()));
        strategy.add(createJob(30, "/tape/file30.txt", getNewCtime()));
        assertEquals(6, strategy.size());

        assertEquals(Long.valueOf(20), strategy.remove());
        assertEquals(Long.valueOf(31), strategy.remove());
        assertEquals(Long.valueOf(30), strategy.remove());
        assertEquals(Long.valueOf(10), strategy.remove());
        assertEquals(Long.valueOf(12), strategy.remove());
        assertEquals(Long.valueOf(11), strategy.remove());

        assertEquals(0, strategy.size());
    }

    @Test
    public void shouldOnlySelectTapesFromWhichEnoughVolumeIsRecalledByRecallVolume()
          throws Exception {
        requirementsChecker.setMinNumberOfRequestsForTapeSelection(1000);
        requirementsChecker.setMinTapeRecallPercentage(40);
        tapeInfoProvider.addTapeFileInfo("/tape/file10.txt", new TapefileInfo(2, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file11.txt", new TapefileInfo(2, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file12.txt", new TapefileInfo(2, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file20.txt", new TapefileInfo(100, "tape2"));
        tapeInfoProvider.addTapeFileInfo("/tape/file30.txt", new TapefileInfo(50, "tape3"));
        tapeInfoProvider.addTapeFileInfo("/tape/file31.txt", new TapefileInfo(18, "tape3"));
        // tape1 40/100, we recall 6 (not neough)
        // tape2 100/100, we recall 100 (enough)
        // tape3 70/100, we recall 68 (enough)

        assertEquals(0, strategy.size());
        strategy.add(createJob(10, "/tape/file10.txt", getNewCtime()));
        strategy.add(createJob(12, "/tape/file12.txt", getNewCtime()));
        strategy.add(createJob(11, "/tape/file11.txt", getNewCtime()));
        strategy.add(createJob(20, "/tape/file20.txt", getNewCtime()));
        strategy.add(createJob(31, "/tape/file31.txt", getNewCtime()));
        strategy.add(createJob(30, "/tape/file30.txt", getNewCtime()));
        assertEquals(6, strategy.size());

        assertEquals(Long.valueOf(20), strategy.remove());
        assertEquals(Long.valueOf(31), strategy.remove());
        assertEquals(Long.valueOf(30), strategy.remove());
        assertEquals(null, strategy.remove());

        assertEquals(3, strategy.size());
    }

    @Test
    public void shouldOnlySelectTapesWithCooldownSinceLastJobArrival() throws Exception {
        requirementsChecker.setMinJobWaitingTime(Duration.ofMinutes(2));
        tapeInfoProvider.addTapeFileInfo("/tape/file10.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file11.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file12.txt", new TapefileInfo(10, "tape1"));
        tapeInfoProvider.addTapeFileInfo("/tape/file20.txt", new TapefileInfo(10, "tape2"));
        tapeInfoProvider.addTapeFileInfo("/tape/file30.txt", new TapefileInfo(10, "tape3"));
        tapeInfoProvider.addTapeFileInfo("/tape/file31.txt", new TapefileInfo(10, "tape3"));

        assertEquals(0, strategy.size());
        strategy.add(createJob(10, "/tape/file10.txt", getNewCtime()));
        strategy.add(createJob(12, "/tape/file12.txt", getNewCtime()));
        strategy.add(createJob(11, "/tape/file11.txt", getNewCtime()));
        strategy.add(createJob(20, "/tape/file20.txt", getNewCtime()));
        strategy.add(createJob(31, "/tape/file31.txt", getCooldownedCtime()));
        strategy.add(createJob(30, "/tape/file30.txt", getCooldownedCtime()));
        assertEquals(6, strategy.size());

        assertEquals(Long.valueOf(31), strategy.remove());
        assertEquals(Long.valueOf(30), strategy.remove());
        assertEquals(null, strategy.remove());

        assertEquals(4, strategy.size());
    }
}
