package org.dcache.vehicles;

import static java.util.Objects.requireNonNull;

public class BeanQuerySinglePropertyMessage extends BeanQueryMessage
{
    private static final long serialVersionUID = 7672381271212819332L;
    private final String propertyName;

    public BeanQuerySinglePropertyMessage(String aPropertyName)
    {
        requireNonNull(aPropertyName);
        propertyName = aPropertyName;
    }

    public String getPropertyName()
    {

        return propertyName;
    }
}
