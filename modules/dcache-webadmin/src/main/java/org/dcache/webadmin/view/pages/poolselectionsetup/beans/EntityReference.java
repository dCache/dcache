package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import java.io.Serializable;

/**
 *
 * @author jans
 */
public class EntityReference implements Serializable{

    private static final long serialVersionUID = 574209399821494921L;
    private String _name;
    private EntityType _type;

    public EntityReference(String name, EntityType type) {
        _name = name;
        _type = type;
    }

    public String getName() {
        return _name;
    }

    public EntityType getEntityType() {
        return _type;
    }
}
