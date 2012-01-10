package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import java.io.Serializable;
import java.util.List;

/**
 *
 * @author jans
 */
public interface DCacheEntity extends Serializable {

    public String getSingleEntityViewTitleResource();

    public String getFirstreferenceDescription();

    public String getSecondReferenceDescription();

    public String getName();

    public void setName(String name);

    public abstract List<EntityReference> getFirstReferences();

    public abstract List<EntityReference> getSecondReferences();

    public EntityType getFirstReferenceType();

    public EntityType getSecondReferenceType();
}
