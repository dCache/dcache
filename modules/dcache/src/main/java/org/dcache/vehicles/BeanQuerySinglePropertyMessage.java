package org.dcache.vehicles;

import static com.google.common.base.Preconditions.checkNotNull;

public class BeanQuerySinglePropertyMessage extends BeanQueryMessage
{
    private static final long serialVersionUID = 7672381271212819332L;
    private final String propertyName;

    public BeanQuerySinglePropertyMessage(String aPropertyName)
    {
        checkNotNull(aPropertyName);
        propertyName = aPropertyName;
    }

    public String getPropertyName()
    {

        return propertyName;
    }
}
