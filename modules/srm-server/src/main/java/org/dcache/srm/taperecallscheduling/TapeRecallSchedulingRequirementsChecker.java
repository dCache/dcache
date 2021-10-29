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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.dcache.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TapeRecallSchedulingRequirementsChecker implements CellCommandListener,
      CellInfoProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(
          TapeRecallSchedulingRequirementsChecker.class);

    /**
     * time safety margin in milliseconds
     */
    private static final long TIME_SAFETY_MARGIN = 10;
    private static final long MIN_RELATIVE_TAPE_RECALL_PERCENTAGE = 95; // of _used_ space

    private final ReentrantReadWriteLock configLock = new ReentrantReadWriteLock();

    // scheduling parameters
    private int maxActiveTapes = 1;
    private int minTapeRecallPercentage = 80;
    private OptionalLong minNumberOfRequestsForTapeSelection = OptionalLong.empty();
    private long minJobWaitingTime = MINUTES.toMillis(2);
    private long maxJobWaitingTime = HOURS.toMillis(1);
    private OptionalLong tapeinfolessJobWaitingTime = OptionalLong.of(MINUTES.toMillis(10));

    public void setMaxActiveTapes(int tapeCount) {
        checkArgument(tapeCount > 0, "There need to be more than 0 max. active tapes");
        configLock.writeLock().lock();
        try {
            maxActiveTapes = tapeCount;
        } finally {
            configLock.writeLock().unlock();
        }
    }

    public void setMinJobWaitingTime(Duration time) {
        checkArgument(time.toMillis() >= 0, "The min. job waiting time needs to be 0 or larger");
        configLock.writeLock().lock();
        try {
            minJobWaitingTime = time.toMillis();
        } finally {
            configLock.writeLock().unlock();
        }
    }

    public void setMaxJobWaitingTime(Duration time) {
        checkArgument(time.toMillis() >= 0, "The max. job waiting time needs to be 0 or larger");
        configLock.writeLock().lock();
        try {
            this.maxJobWaitingTime = time.toMillis();
        } finally {
            configLock.writeLock().unlock();
        }
    }

    public void setMinNumberOfRequestsForTapeSelection(long number) {
        checkArgument(number >= 0 || number == -1,
              "The min. number of requests for tape selection needs to be positive or -1");
        configLock.writeLock().lock();
        try {
            minNumberOfRequestsForTapeSelection =
                  number == -1 ? OptionalLong.empty() : OptionalLong.of(number);
        } finally {
            configLock.writeLock().unlock();
        }
    }

    public void setMinTapeRecallPercentage(int percentage) {
        checkArgument(percentage >= 0 && percentage <= 100,
              "The minimum tape recall percentage needs to be between 0 and 100");
        configLock.writeLock().lock();
        try {
            minTapeRecallPercentage = percentage;
        } finally {
            configLock.writeLock().unlock();
        }
    }

    public void setTapeinfolessJobWaitingTime(Duration time) {
        configLock.writeLock().lock();
        try {
            this.tapeinfolessJobWaitingTime =
                  time.toMillis() < 0 ? OptionalLong.empty() : OptionalLong.of(time.toMillis());
        } finally {
            configLock.writeLock().unlock();
        }
    }

    public int maxActiveTapes() {
        configLock.readLock().lock();
        try {
            return maxActiveTapes;
        } finally {
            configLock.readLock().unlock();
        }
    }

    public int minTapeRecallPercentage() {
        configLock.readLock().lock();
        try {
            return minTapeRecallPercentage;
        } finally {
            configLock.readLock().unlock();
        }
    }

    public OptionalLong minNumberOfRequestsForTapeSelection() {
        configLock.readLock().lock();
        try {
            return minNumberOfRequestsForTapeSelection;
        } finally {
            configLock.readLock().unlock();
        }
    }

    public long minJobWaitingTime() {
        configLock.readLock().lock();
        try {
            return minJobWaitingTime;
        } finally {
            configLock.readLock().unlock();
        }
    }

    public long maxJobWaitingTime() {
        configLock.readLock().lock();
        try {
            return maxJobWaitingTime;
        } finally {
            configLock.readLock().unlock();
        }
    }

    public OptionalLong tapeinfolessJobWaitingTime() {
        configLock.readLock().lock();
        try {
            return tapeinfolessJobWaitingTime;
        } finally {
            configLock.readLock().unlock();
        }
    }

    public int getRemainingTapeSlots(int taken) {
        int remaining = maxActiveTapes() - taken;
        return remaining < 0 ? 0 : remaining;
    }

    public boolean isDefinedMinRequestCount() {
        return minNumberOfRequestsForTapeSelection().isPresent();
    }

    public boolean isRequestCountSufficient(long count) {
        OptionalLong minRequestCount = minNumberOfRequestsForTapeSelection();
        return minRequestCount.isPresent() && count >= minRequestCount.getAsLong();
    }

    /**
     * If the oldest request for a tape has exceeded its max. queue lifetime.
     *
     * @param tape the scheduling info for the tape in question
     * @return whether the tape's oldest jost is expired
     */
    public boolean isOldestTapeJobExpired(SchedulingInfoTape tape) {
        if (tape == null) {
            return false;
        }

        OptionalLong oldestArrival = tape.getOldestJobArrival();
        if (oldestArrival.isEmpty()) {
            return false;
        }

        long ageOfOldestJobArrival = System.currentTimeMillis() - oldestArrival.getAsLong();
        long correctedMaxAge = maxJobWaitingTime() + TIME_SAFETY_MARGIN;
        return ageOfOldestJobArrival >= correctedMaxAge;
    }

    /**
     * @param tape the scheduling info for the tape in question
     * @return if newest job for a tape is old enough for the tape to be selected
     */
    public boolean isNewestTapeJobOldEnough(SchedulingInfoTape tape) {
        if (tape == null) {
            return false;
        }
        OptionalLong newestArrival = tape.getNewestJobArrival();
        if (newestArrival.isEmpty()) {
            return false;
        }

        long ageOfNewestJobArrival = System.currentTimeMillis() - newestArrival.getAsLong();
        long minWaitingTime = minJobWaitingTime();
        long correctedMinAge = minWaitingTime + TIME_SAFETY_MARGIN;
        return ageOfNewestJobArrival >= correctedMinAge;
    }

    /**
     * Assesses if the provided absolute recall volume exceeds the configured percentage of overall
     * tape capacity. Always returns true if more than 90 percent of the tape's used space is
     * requested, irrespective of the overall percentage. If the tape's capacity and used space is
     * unknown, false is returned.
     *
     * @param tape         the scheduling info for the tape in question
     * @param recallVolume Recall volume in kB
     * @return Whether the recall volume is sufficient
     */
    public boolean isTapeRecallVolumeSufficient(SchedulingInfoTape tape, long recallVolume) {
        int percentage = minTapeRecallPercentage();
        if (tape == null || !tape.hasTapeInfo()) {
            return percentage == 0;
        }

        float percentOfUsedSpace =
              ((float) recallVolume / (float) tape.getUsedSpace().getAsLong()) * 100;
        boolean recalledMostOfUsedSpace = percentOfUsedSpace >= MIN_RELATIVE_TAPE_RECALL_PERCENTAGE;
        if (recalledMostOfUsedSpace) {
            LOGGER.info("Tape recall volume sufficient: {} of contained",
                  String.format("%.0f%%", percentOfUsedSpace));
            return true;
        }

        float percentOfCapacity =
              ((float) recallVolume / (float) tape.getCapacity().getAsLong()) * 100;
        boolean recallVolumeSufficient = percentOfCapacity >= percentage;
        if (recallVolumeSufficient) {
            LOGGER.info("Tape recall volume sufficient: {} of capacity",
                  String.format("%.0f%%", percentOfCapacity));
        }
        return recallVolumeSufficient;
    }

    public int compareOldestTapeRequestAge(SchedulingInfoTape first, SchedulingInfoTape second) {
        if (first == null || second == null) {
            return 0;
        }
        OptionalLong oldestArrival = first.getOldestJobArrival();
        OptionalLong otherArrival = second.getOldestJobArrival();
        if (oldestArrival.isEmpty() && otherArrival.isEmpty()) {
            return 0;
        }
        if (oldestArrival.isEmpty()) {
            return -1;
        }
        if (otherArrival.isEmpty()) {
            return 1;
        }
        return Long.compare(oldestArrival.getAsLong(), otherArrival.getAsLong());
    }

    public boolean isJobExpired(SchedulingItemJob job) {
        long age = System.currentTimeMillis() - job.getCreationTime();
        long correctedMaxAge = maxJobWaitingTime() + TIME_SAFETY_MARGIN;
        return age > correctedMaxAge;
    }

    public boolean isTapeinfolessJobExpired(SchedulingItemJob job) {
        OptionalLong waitingTime = tapeinfolessJobWaitingTime();
        if (waitingTime.isEmpty()) {
            return isJobExpired(job);
        }
        long age = System.currentTimeMillis() - job.getCreationTime();
        long correctedMaxAge = waitingTime.getAsLong() + TIME_SAFETY_MARGIN;
        return age > correctedMaxAge;
    }

    @Command(name = "trs set tape selection",
          hint =
                "Changes the parameters used for selecting which and how many tapes will be activated "
                      +
                      "at any point in time in order for their associated requests to be forwarded "
                      +
                      "to the tape system for recall.")
    public class TrsSetTapeSelection implements Callable<String> {

        @Option(name = "active", metaVar = "count",
              usage = "The maximum number of tapes which may be active at the same time")
        Integer active;

        @Option(name = "volume", metaVar = "percent",
              usage = "The minimum percent of a tape's capacity that needs to be requested for tape selection")
        Integer volume;

        @Option(name = "requests", metaVar = "count",
              usage = "The minimum number of requests needed for a tape for its selection. '-1' disables this criterion")
        Integer requests;

        @Override
        public String call() {
            StringBuilder sb = new StringBuilder();
            try {
                if (active != null) {
                    setMaxActiveTapes(active);
                    sb.append("maximum active tapes set to ").append(active).append("\n");
                }
                if (volume != null) {
                    setMinTapeRecallPercentage(volume);
                    sb.append("minimum recall percentage set to ").append(volume).append("\n");
                }
                if (requests != null) {
                    setMinNumberOfRequestsForTapeSelection(requests);
                    sb.append("minimum number of requests per tape set to " + requests)
                          .append("\n");
                }
            } catch (IllegalArgumentException e) {
                sb.append("Failure: ").append(e.getMessage());
            } finally {
                return sb.toString();
            }
        }
    }

    @Command(name = "trs set request stay",
          hint = "Changes the time parameters that requests stay in the scheduler before leaving or the associated tape can be selected.")
    public class TrsSetTimeInQueue implements Callable<String> {

        @Option(name = "min", metaVar = "minutes",
              usage = "Minimum time a request stays in the scheduler before tape selection")
        Long min;

        @Option(name = "max", metaVar = "minutes",
              usage = "Maximum time a request stays in the scheduler before tape selection")
        Long max;

        @Option(name = "tapeinfoless", metaVar = "minutes",
              usage = "Time a request without associated tape information stays in the scheduler")
        Long tapeinfoless;

        @Override
        public String call() {
            StringBuilder sb = new StringBuilder();
            try {
                if (min != null) {
                    setMinJobWaitingTime(Duration.ofMinutes(min));
                    sb.append("minimum job waiting time set to ").append(min).append(" minutes\n");
                }
                if (max != null) {
                    setMaxJobWaitingTime(Duration.ofMinutes(max));
                    sb.append("maximum job waiting time set to ").append(max).append(" minutes\n");
                }
                if (tapeinfoless != null) {
                    setTapeinfolessJobWaitingTime(Duration.ofMinutes(tapeinfoless));
                    sb.append("tapeinfoless job waiting time set to ").append(tapeinfoless)
                          .append(" minutes\n");
                }
            } catch (IllegalArgumentException e) {
                sb.append("Failure: ").append(e.getMessage());
            } finally {
                return sb.toString();
            }
        }
    }

    @Override
    public void getInfo(PrintWriter pw) {
        configLock.readLock().lock();
        try {
            pw.printf("Bring online scheduler parameters:\n");
            pw.printf("    Max. active tapes: %s\n", maxActiveTapes);
            pw.printf("    Min. recall volume percentage for tape selection: %s\n",
                  minTapeRecallPercentage);
            pw.printf("    Min. number of requests for tape selection: %s\n",
                  minNumberOfRequestsForTapeSelection.isPresent()
                        ? minNumberOfRequestsForTapeSelection.getAsLong() : "-");
            pw.printf("    Min. time requests stay in the queue: %s\n",
                  TimeUtils.describe(Duration.ofMillis(minJobWaitingTime)).orElse("not set!"));
            pw.printf("    Max. time requests stay in the queue: %s\n",
                  TimeUtils.describe(Duration.ofMillis(maxJobWaitingTime)).orElse("not set!"));
            pw.printf("    Min. time requests without tape info stay in the queue: %s\n",
                  tapeinfolessJobWaitingTime.isEmpty() ? "-" : TimeUtils.describe(
                              Duration.ofMillis(tapeinfolessJobWaitingTime.getAsLong()))
                        .orElse("not set!"));
        } finally {
            configLock.readLock().unlock();
        }
    }
}
