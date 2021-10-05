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

/**
 * Bring online tape scheduling item for tracking a tape's meta-information relevant for job
 * scheduling Used for keeping track of and evaluating oldest and newest job arrival targeting the
 * associated tape
 */
public class SchedulingInfoTape {

    private static final long NO_VALUE = -1;

    // scheduling info
    private long oldestJobArrival = NO_VALUE;
    private long newestJobArrival = NO_VALUE;

    // tape capacity info
    private boolean hasTapeInfo = false;
    private long capacity = NO_VALUE;
    private long usedSpace = NO_VALUE;

    public boolean addTapeInfo(long capacity, long usedSpace) {
        if (hasTapeInfo) {
            return false;
        }
        this.capacity = capacity;
        this.usedSpace = usedSpace;
        hasTapeInfo = true;
        return true;
    }

    public boolean hasTapeInfo() {
        return hasTapeInfo;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getUsedSpace() {
        return usedSpace;
    }

    public void resetJobArrivalTimes() {
        this.oldestJobArrival = NO_VALUE;
        this.newestJobArrival = NO_VALUE;
    }

    public Long getNewestJobArrival() {
        return newestJobArrival == NO_VALUE ? null : newestJobArrival;
    }

    /**
     * Sets the newest job arrival to the given time. If there is no oldest job arrival time, it is
     * set to the same value.
     *
     * @param jobArrival
     */
    public void setNewestJobArrival(Long jobArrival) {

        if (jobArrival == null || jobArrival <= 0) {
            this.newestJobArrival = NO_VALUE;

        } else {
            this.newestJobArrival = jobArrival;
            if (oldestJobArrival == NO_VALUE) {
                oldestJobArrival = jobArrival;
            }
        }
    }

    public Long getOldestJobArrival() {
        return oldestJobArrival == NO_VALUE ? null : oldestJobArrival;
    }

    public void setOldestJobArrival(Long jobArrival) {
        if (jobArrival == null || jobArrival <= 0) {
            this.oldestJobArrival = NO_VALUE;
        } else {
            this.oldestJobArrival = jobArrival;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Oldest job: ").append(oldestJobArrival).append("\n");
        sb.append("Newest job: ").append(newestJobArrival).append("\n");
        sb.append("Has").append(hasTapeInfo ? "" : " no").append(" tape info");
        if (hasTapeInfo) {
            sb.append("\n");
            sb.append("  Tape capacity: ").append(capacity).append(" in KB").append("\n");
            sb.append("  Used space: ").append(usedSpace).append(" in KB").append("\n");
        }
        return sb.toString();
    }
}
