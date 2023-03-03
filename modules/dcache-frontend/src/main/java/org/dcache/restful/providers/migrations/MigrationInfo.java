package org.dcache.restful.providers.migrations;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;

@ApiModel(description = "Container for migration information regarding a pool.")
public class MigrationInfo {
    private static final long serialVersionUID = -7942464845703567335L;

    @ApiModelProperty("Current state of the migration.")
    private String state;

    @ApiModelProperty("Number of queued PNFSIDs.")
    private Integer queued;

    @ApiModelProperty("Number of attempts.")
    private Integer attempts;

    @ApiModelProperty("List of target pools for the migration.")
    private List<String> targetPools;

    @ApiModelProperty("Number of completed files, bytes and a percentage (if not finished or failed). (X files; Y bytes; Z%)")
    private String completed;

    @ApiModelProperty("Number of total bytes.")
    private Integer total;

    @ApiModelProperty("Representation of the running tasks for the migration job.")
    private String runningTasks;

    @ApiModelProperty("Representation of the most recent errors for the migration job.")
    private String mostRecentErrors;

    public void setState(String state) {
        this.state = state;
    }

    public String getState() {
        return state;
    }

    public void setQueued(int queued) {
        this.queued = queued;
    }

    public int getQueued() {
        return queued;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setTargetPools(List<String> targetPools) {
        this.targetPools = targetPools;
    }

    public List<String> getTargetPools() {
        return targetPools;
    }

    public void setCompleted(String completed) {
        this.completed = completed;
    }

    public String getCompleted() {
        return completed;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getTotal() {
        return total;
    }

    public void setRunningTasks(String runningTasks) {
        this.runningTasks = runningTasks;
    }

    public String getRunningTasks() {
        return runningTasks;
    }

    public void setMostRecentErrors(String mostRecentErrors) {
        this.mostRecentErrors = mostRecentErrors;
    }

    public String getMostRecentErrors() {
        return mostRecentErrors;
    }
}
