package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import java.util.List;

/**
 *
 * @author jans
 */
public class PoolGroupEntity implements DCacheEntity {

    private static final String ENTITY_TITLE_RESOURCE = "poolGroup.singleview.title";
    private static final String FIRST_DESCRIPTION = "We have the following members";
    private static final String SECOND_DESCRIPTION = "The following links target us";
    private static final long serialVersionUID = -9056596826440135834L;
    private String _name = "";
    private List<EntityReference> _poolMembers;
    private List<EntityReference> _linksTargetingUs;

    @Override
    public List<EntityReference> getFirstReferences() {
        return _poolMembers;
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

    public void setLinksTargetingUs(List<EntityReference> linksTargetingUs) {
        _linksTargetingUs = linksTargetingUs;
    }

    public void setPoolMembers(List<EntityReference> poolMembers) {
        _poolMembers = poolMembers;
    }

    @Override
    public EntityType getFirstReferenceType() {
        return EntityType.POOL;
    }

    @Override
    public EntityType getSecondReferenceType() {
        return EntityType.LINK;
    }
}
