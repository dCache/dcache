package org.dcache.webadmin.model.businessobjects;

import java.io.Serializable;

/**
 * This is a simple Data-Container Object for the relevant information
 * of dCache-Pools for later displaying.
 *
 * @author jan schaefer
 */
public class Pool implements Serializable {

    private String name = "";
    private boolean enabled = false;
    private long totalSpace = 0;
    private long freeSpace = 0;
    private long preciousSpace = 0;
    private long usedSpace = 0;

    public Pool() {
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(long freeSpace) {
        this.freeSpace = freeSpace;
    }

    public long getUsedSpace() {
        return usedSpace;
    }

    public void setUsedSpace(long usedSpace) {
        this.usedSpace = usedSpace;
    }

    public long getPreciousSpace() {
        return preciousSpace;
    }

    public void setPreciousSpace(long preciousSpace) {
        this.preciousSpace = preciousSpace;
    }

    public long getTotalSpace() {
        return totalSpace;
    }

    public void setTotalSpace(long totalSpace) {
        this.totalSpace = totalSpace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int hashCode() {
        return (int) (name.hashCode() ^ totalSpace ^
                freeSpace ^ preciousSpace ^ usedSpace);
    }

    @Override
    public boolean equals(Object testObject) {
        if (this == testObject) {
            return true;
        }

        if (!(testObject instanceof Pool)) {
            return false;
        }

        Pool otherPool = (Pool) testObject;

        if (!(otherPool.name.equals(name))) {
            return false;
        }

        if (!(otherPool.freeSpace == freeSpace)) {
            return false;
        }
        if (!(otherPool.preciousSpace == preciousSpace)) {
            return false;
        }
        if (!(otherPool.totalSpace == totalSpace)) {
            return false;
        }
        if (!(otherPool.usedSpace == usedSpace)) {
            return false;
        }

        return true;
    }
}
