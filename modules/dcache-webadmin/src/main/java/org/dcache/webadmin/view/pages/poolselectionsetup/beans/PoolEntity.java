package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import java.util.List;

/**
 *
 * @author jans
 */
public class PoolEntity implements DCacheEntity {

    private static final String ENTITY_TITLE_RESOURCE = "pool.singleview.title";
    private static final String FIRST_DESCRIPTION = "We are member of the following pool groups";
    private static final String SECOND_DESCRIPTION = "The following links target us";
    private static final long serialVersionUID = 5918161823902824234L;
    private String _name = "";
    private boolean _isEnabled;
    private String _mode = "unknown";
    private boolean _isActive;
    private List<EntityReference> _poolGroupsBelongingTo;
    private List<EntityReference> _linksTargetingUs;

    @Override
    public EntityType getFirstReferenceType() {
        return EntityType.POOLGROUP;
    }

    @Override
    public EntityType getSecondReferenceType() {
        return EntityType.LINK;
    }

    @Override
    public List<EntityReference> getFirstReferences() {
        return _poolGroupsBelongingTo;
    }

    @Override
    public List<EntityReference> getSecondReferences() {
        return _linksTargetingUs;
    }

    @Override
    public String getSingleEntityViewTitleResource() {
        return ENTITY_TITLE_RESOURCE;
    }

    @Override
    public String getFirstreferenceDescription() {
        return FIRST_DESCRIPTION;
    }

    @Override
    public String getSecondReferenceDescription() {
        return SECOND_DESCRIPTION;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void setName(String name) {
        _name = name;
    }

    public boolean isActive() {
        return _isActive;
    }

    public void setIsActive(boolean isActive) {
        _isActive = isActive;
    }

    public boolean isEnabled() {
        return _isEnabled;
    }

    public void setIsEnabled(boolean isEnabled) {
        _isEnabled = isEnabled;
    }

    public String getMode() {
        return _mode;
    }

    public void setMode(String mode) {
        _mode = mode;
    }

    public void setLinksTargetingUs(List<EntityReference> linksTargetingUs) {
        _linksTargetingUs = linksTargetingUs;
    }

    public void setPoolGroupsBelongingTo(List<EntityReference> poolGroupsBelongingTo) {
        _poolGroupsBelongingTo = poolGroupsBelongingTo;
    }
}
