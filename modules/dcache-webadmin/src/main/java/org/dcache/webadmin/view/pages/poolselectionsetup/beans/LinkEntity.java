package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import java.io.Serializable;
import java.util.List;

/**
 *
 * @author jans
 */
public class LinkEntity implements DCacheEntity, Serializable {

    private static final String ENTITY_TITLE_RESOURCE = "link.singleview.title";
    private static final String FIRST_DESCRIPTION = "We point to the following Pool Groups";
    private static final String SECOND_DESCRIPTION = "We follow these Selection Units";
    private static final long serialVersionUID = 5790292636869304476L;
    private String _name = "";
    private int _writePreference;
    private int _readPreference;
    private int _restorePreference;
    private int _p2pPreference;
    private String _partition = "";
    private List<EntityReference> _targetPoolGroups;
    private List<EntityReference> _unitGroupsFollowed;

    @Override
    public List<EntityReference> getFirstReferences() {
        return _targetPoolGroups;
    }

    @Override
    public List<EntityReference> getSecondReferences() {
        return _unitGroupsFollowed;
    }

    @Override
    public EntityType getFirstReferenceType() {
        return EntityType.POOLGROUP;
    }

    @Override
    public EntityType getSecondReferenceType() {
        return EntityType.UNITGROUP;
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

    public int getP2pPreference() {
        return _p2pPreference;
    }

    public void setP2pPreference(int p2pPreference) {
        _p2pPreference = p2pPreference;
    }

    public String getPartition() {
        return _partition;
    }

    public void setPartition(String partition) {
        _partition = partition;
    }

    public int getReadPreference() {
        return _readPreference;
    }

    public void setReadPreference(int readPreference) {
        _readPreference = readPreference;
    }

    public int getRestorePreference() {
        return _restorePreference;
    }

    public void setRestorePreference(int restorePreference) {
        _restorePreference = restorePreference;
    }

    public int getWritePreference() {
        return _writePreference;
    }

    public void setWritePreference(int writePreference) {
        _writePreference = writePreference;
    }

    public void setPoolGroupsPointingTo(List<EntityReference> poolGroupsPointingTo) {
        _targetPoolGroups = poolGroupsPointingTo;
    }

    public void setUnitGroupsFollowed(List<EntityReference> selectionUnitsFollowed) {
        _unitGroupsFollowed = selectionUnitsFollowed;
    }

    public List<EntityReference> getTargetPoolGroups() {
        return _targetPoolGroups;
    }

    public List<EntityReference> getUnitGroupsFollowed() {
        return _unitGroupsFollowed;
    }
}
