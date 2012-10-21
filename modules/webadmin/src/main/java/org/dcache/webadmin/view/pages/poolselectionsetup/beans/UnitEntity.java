package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import java.util.Collections;
import java.util.List;

/**
 *
 * @author jans
 */
public class UnitEntity implements DCacheEntity {

    private static final String ENTITY_TITLE_RESOURCE = "unit.singleview.title";
    private static final String FIRST_DESCRIPTION = "We are member of the following Selection Unit Groups";
    private static final String SECOND_DESCRIPTION = "";
    private static final long serialVersionUID = -299254888268795209L;
    private String _name = "";
    private String _type = "";
    private List<EntityReference> _unitGroups;

    @Override
    public EntityType getFirstReferenceType() {
        return EntityType.UNIT;
    }

    @Override
    public EntityType getSecondReferenceType() {
        return EntityType.NONE;
    }

    @Override
    public List<EntityReference> getFirstReferences() {
        return _unitGroups;
    }

    @Override
    public List<EntityReference> getSecondReferences() {
        return Collections.emptyList();
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

    public void setUnitGroups(List<EntityReference> unitGroups) {
        _unitGroups = unitGroups;
    }

    public String getType() {
        return _type;
    }

    public void setType(String type) {
        _type = type;
    }
}
