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

import java.util.OptionalLong;

/**
 * Bring online job scheduling item for tracking a job's meta-information relevant for job
 * scheduling
 */
public class SchedulingItemJob {

    private final long jobid;
    private final String fileid;
    private final long creationTime;
    private OptionalLong fileSize = OptionalLong.empty();
    private boolean attemptedToRetrieveTapeLocationInfo = false;

    public SchedulingItemJob(long jobid, String fileid, long ctime) {
        this.jobid = jobid;
        this.fileid = fileid;
        this.creationTime = ctime;
    }

    public long getJobid() {
        return jobid;
    }

    public String getFileid() {
        return fileid;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public OptionalLong getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = OptionalLong.of(fileSize);
    }

    public boolean attemptedToRetrieveTapeLocationInfo() {
        return attemptedToRetrieveTapeLocationInfo;
    }

    public void setAttemptedToRetrieveTapeLocationInfo() {
        attemptedToRetrieveTapeLocationInfo = true;
    }

}
