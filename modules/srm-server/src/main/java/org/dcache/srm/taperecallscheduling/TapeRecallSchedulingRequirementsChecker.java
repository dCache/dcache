package org.dcache.srm.taperecallscheduling;

import dmg.cells.nucleus.CellInfoProvider;
import org.dcache.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TapeRecallSchedulingRequirementsChecker implements CellInfoProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(TapeRecallSchedulingRequirementsChecker.class);


    public static final long NO_VALUE = -1;
    /** time safety margin in milliseconds */
    private static final long TIME_SAFETY_MARGIN = 10;
    private static final long MIN_RELATIVE_TAPE_RECALL_PERCENTAGE = 95; // of _used_ space

    // scheduling parameters

    private int maxActiveTapes = 1;
    private int minTapeRecallPercentage = 80;
    private long minNumberOfRequestsForTapeSelection = NO_VALUE;
    private long minJobWaitingTime = MINUTES.toMillis(2);
    private long maxJobWaitingTime = HOURS.toMillis(1);
    private long tapeinfolessJobWaitingTime = MINUTES.toMillis(10); // queue waiting time for jobs without tape info

    public void setMaxActiveTapes(int tapeCount) {
        checkArgument(tapeCount > 0, "There need to be more than 0 max. active tapes");
        this.maxActiveTapes = tapeCount;
    }

    public void setMinJobWaitingTime(Duration time) {
        checkArgument(time.toMillis() >= 0, "The min. job waiting time needs to be 0 or larger");
        minJobWaitingTime = time.toMillis();
    }

    public void setMaxJobWaitingTime(Duration time) {
        checkArgument(time.toMillis() >= 0, "The max. job waiting time needs to be 0 or larger");
        this.maxJobWaitingTime = time.toMillis();
    }

    public void setMinNumberOfRequestsForTapeSelection(long number) {
        checkArgument(number >= 0 || number == -1, "The min. number of requests for tape selection needs to be positive or -1");
        minNumberOfRequestsForTapeSelection = number == -1 ? NO_VALUE : number;
    }

    public void setMinTapeRecallPercentage(int percentage) {
        checkArgument(percentage >= 0 && percentage <= 100, "The minimum tape recall percentage needs to be between 0 and 100");
        minTapeRecallPercentage = percentage;
    }

    public void setTapeinfolessJobWaitingTime(Duration time) {
        this.tapeinfolessJobWaitingTime = time.toMillis() < 0 ? NO_VALUE : time.toMillis();
    }

    public int maxActiveTapes() {
        return maxActiveTapes;
    }

    public int minTapeRecallPercentage() {
        return minTapeRecallPercentage;
    }

    public long minNumberOfRequestsForTapeSelection() {
        return minNumberOfRequestsForTapeSelection;
    }

    public long minJobWaitingTime() {
        return minJobWaitingTime;
    }

    public long maxJobWaitingTime() {
        return maxJobWaitingTime;
    }

    /**
     * If the oldest request for a tape has exceeded its max. queue lifetime.
     * @param tape the scheduling info for the tape in question
     * @return whether the tape's oldest jost is expired
     */
    public boolean isOldestTapeJobExpired(SchedulingInfoTape tape) {
        if (tape.getOldestJobArrival() == NO_VALUE) {
            return false;
        }
        long ageOfOldestJobArrival = System.currentTimeMillis() - tape.getOldestJobArrival();
        long correctedMaxAge = maxJobWaitingTime + TIME_SAFETY_MARGIN;
        return ageOfOldestJobArrival >= correctedMaxAge;
    }

    /**
     * @return if newest job for a tape is old enough for the tape to be selected
     * @param tape the scheduling info for the tape in question
     */
    public boolean isNewestTapeJobOldEnough(SchedulingInfoTape tape) {
        if (tape.getNewestJobArrival() == NO_VALUE) {
            return false;
        } else if (minJobWaitingTime == NO_VALUE) {
            return true;
        }
        long ageOfNewestJobArrival = System.currentTimeMillis() - tape.getNewestJobArrival();
        long correctedMinAge = minJobWaitingTime + TIME_SAFETY_MARGIN;
        return ageOfNewestJobArrival >= correctedMinAge;
    }

    /**
     * Assesses if the provided absolute recall volume exceeds the configured percentage of overall tape capacity.
     * Always returns true if more than 90 percent of the tape's used space is requested, irrespective of the
     * overall percentage.
     * If the tape's capacity and used space is unknown, false is returned.
     *
     * @param tape the scheduling info for the tape in question
     * @param recallVolume Recall volume in kB
     * @return Whether the recall volume is sufficient
     */
    public boolean isTapeRecallVolumeSufficient(SchedulingInfoTape tape, long recallVolume) {
        if (!tape.hasTapeInfo()) {
            return minTapeRecallPercentage == 0;
        }

        float percentOfUsedSpace = ((float)recallVolume / (float)tape.getUsedSpace()) * 100;
        boolean recalledMostOfUsedSpace = percentOfUsedSpace >= MIN_RELATIVE_TAPE_RECALL_PERCENTAGE;
        if (recalledMostOfUsedSpace) {
            LOGGER.info("Tape recall volume sufficient: {} of contained", String.format("%.0f%%",percentOfUsedSpace));
            return true;
        }

        float percentOfCapacity =((float)recallVolume / (float)tape.getCapacity()) * 100;
        boolean recallVolumeSufficient = percentOfCapacity >= minTapeRecallPercentage;
        if (recallVolumeSufficient) {
            LOGGER.info("Tape recall volume sufficient: {} of capacity", String.format("%.0f%%",percentOfCapacity));
        }
        return recallVolumeSufficient;
    }

    public int compareOldestTapeRequestAge(SchedulingInfoTape first, SchedulingInfoTape second) {
        long arrivalFirst = first.getOldestJobArrival();
        long arrivalSecond = second.getOldestJobArrival();
        if (arrivalFirst == NO_VALUE && arrivalSecond == NO_VALUE) {
            return 0;
        } else if(arrivalFirst == NO_VALUE && arrivalSecond != NO_VALUE) {
            return -1;
        } else if(arrivalFirst != NO_VALUE && arrivalSecond == NO_VALUE) {
            return 1;
        }
        return Long.compare(arrivalFirst, arrivalSecond);
    }

    public boolean isJobExpired(SchedulingItemJob job) {
        long age = System.currentTimeMillis() - job.getCreationTime();
        long correctedMaxAge = maxJobWaitingTime + TIME_SAFETY_MARGIN;
        return age > correctedMaxAge;
    }

    public boolean isTapeinfolessJobExpired(SchedulingItemJob job) {
        if (tapeinfolessJobWaitingTime == NO_VALUE) {
            return isJobExpired(job);
        }
        long age = System.currentTimeMillis() - job.getCreationTime();
        long correctedMaxAge = tapeinfolessJobWaitingTime + TIME_SAFETY_MARGIN;
        return age > correctedMaxAge;
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.printf("Bring online scheduler parameters:\n");
        pw.printf("    Max. active tapes (usually drive count): %s\n", maxActiveTapes);
        pw.printf("    Min. recall volume percentage for tape selection: %s\n", minTapeRecallPercentage);
        pw.printf("    Min. number of requests for tape selection: %s\n", minNumberOfRequestsForTapeSelection == NO_VALUE ? "-" : minNumberOfRequestsForTapeSelection);
        pw.printf("    Min. time requests stay in the queue: %s\n", TimeUtils.describe(Duration.ofMillis(minJobWaitingTime)).orElse("-"));
        pw.printf("    Max. time requests stay in the queue: %s\n", TimeUtils.describe(Duration.ofMillis(maxJobWaitingTime)).orElse("-"));
        pw.printf("    Min. time requests without tape info stay in the queue: %s\n", tapeinfolessJobWaitingTime == NO_VALUE ? "-" : TimeUtils.describe(Duration.ofMillis(tapeinfolessJobWaitingTime)).orElse("-"));
    }
}
