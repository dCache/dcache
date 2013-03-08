package org.dcache.vehicles;

import diskCacheV111.vehicles.Message;

/**
 * Request to probe bean properties of a cell.
 */
public abstract class BeanQueryMessage extends Message
{
    private static final long serialVersionUID = -8564720957441570916L;
    private Object result;

    public Object getResult()
    {
        return result;
    }

    public void setResult(Object aResult)
    {
        result = aResult;
    }
}
