package org.dcache.webadmin.model.dataaccess.impl;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.pools.PoolV2Mode;
import java.util.Collections;
import java.util.Collection;
import java.util.Set;

/**
 *
 * @author jans
 */
public class SelectionPoolHelper implements SelectionPool {

    private boolean enabled = true;
    private boolean active = true;
    private boolean readonly = false;
    private String name = "";
    private PoolV2Mode mode =
            new PoolV2Mode(PoolV2Mode.ENABLED);

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getActive() {
        return 10000L;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isReadOnly() {
        return readonly;
    }

    public void setReadOnly(boolean rdOnly) {
        this.readonly = rdOnly;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return (mode.getMode() != PoolV2Mode.DISABLED_RDONLY &&
                mode.getMode() != PoolV2Mode.DISABLED_STRICT);
    }

    public boolean setSerialId(long serialId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean isActive() {
        return active;
    }

    public void setPoolMode(PoolV2Mode mode) {
        this.mode = mode;
    }

    public PoolV2Mode getPoolMode() {
        return mode;
    }

    public boolean canRead() {
        return true;
    }

    public boolean canWrite() {
        return !readonly;
    }

    public boolean canReadFromTape() {
        return false;
    }

    public boolean canReadForP2P() {
        return false;
    }

    public Set<String> getHsmInstances() {
        return Collections.EMPTY_SET;
    }

    public void setHsmInstances(Set<String> hsmInstances) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Collection<SelectionPoolGroup> getPoolGroupsMemberOf() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Collection<SelectionLink> getLinksTargetingPool() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
