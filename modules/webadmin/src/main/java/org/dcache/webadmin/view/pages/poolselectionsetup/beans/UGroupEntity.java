package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import java.util.List;

/**
 *
 * @author jans
 */
public class UGroupEntity implements DCacheEntity {

    private static final String ENTITY_TITLE_RESOURCE = "unitGroup.singleview.title";
    private static final String FIRST_DESCRIPTION = "We have the following members";
    private static final String SECOND_DESCRIPTION = "We are pointing to the following links";
    private static final long serialVersionUID = 6898250445365404576L;
    private String _name = "";
    private List<EntityReference> _units;
    private List<EntityReference> _links;

    @Override
    public EntityType getFirstReferenceType() {
        return EntityType.UNIT;
    }

    @Override
    public EntityType getSecondReferenceType() {
        return EntityType.LINK;
    }

    @Override
    public List<EntityReference> getFirstReferences() {
        return _units;
    }

    @Override
    public List<EntityReference> getSecondReferences() {
        return _links;
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

    public void setLinks(List<EntityReference> links) {
        _links = links;
    }

    public void setUnits(List<EntityReference> units) {
        _units = units;
    }
}
