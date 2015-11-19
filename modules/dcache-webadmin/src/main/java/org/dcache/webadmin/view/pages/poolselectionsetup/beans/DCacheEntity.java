package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import java.io.Serializable;
import java.util.List;

/**
 *
 * @author jans
 */
public interface DCacheEntity extends Serializable {

    String getSingleEntityViewTitleResource();

    String getFirstreferenceDescription();

    String getSecondReferenceDescription();

    String getName();

    void setName(String name);

    List<EntityReference> getFirstReferences();

    List<EntityReference> getSecondReferences();

    EntityType getFirstReferenceType();

    EntityType getSecondReferenceType();
}
