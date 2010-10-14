/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.myproxy;

import java.util.Date;

/**
 * Holds the credential information returned by the
 * <code>info</code> operation.
 */
public class CredentialInfo {

    private String owner;
    private long startTime;
    private long endTime;
    private String name;
    private String description; // optional
    private String renewers;     // optional
    private String retrievers;   // optional

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public String getRetrievers() {
        return this.retrievers;
    }

    public void setRetrievers(String retrievers) {
        this.retrievers = retrievers;
    }

    public String getRenewers() {
        return this.renewers;
    }

    public void setRenewers(String renewers) {
        this.renewers = renewers;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOwner() {
        return this.owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public void setStartTime(long time) {
        this.startTime = time;
    }

    public long getEndTime() {
        return this.endTime;
    }

    public void setEndTime(long time) {
        this.endTime = time;
    }

    public Date getEndTimeAsDate() {
        return new Date(this.endTime);
    }

    public Date getStartTimeAsDate() {
        return new Date(this.startTime);
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        if (this.name != null) {
            buf.append(this.name).append(" ");
        }
        buf.append(owner).append(" ");
        buf.append(String.valueOf(startTime)).append(" ");
        buf.append(String.valueOf(endTime));
        if (this.description != null) {
            buf.append(' ');
            buf.append(this.description);
        }
        if (this.renewers != null) {
            buf.append(' ');
            buf.append(this.renewers);
        }
        if (this.retrievers != null) {
            buf.append(' ');
            buf.append(this.retrievers);
        }
        return buf.toString();
    }

}
