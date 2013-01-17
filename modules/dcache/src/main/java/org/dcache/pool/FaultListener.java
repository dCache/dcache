package org.dcache.pool;

public interface FaultListener
{
    void faultOccurred(FaultEvent event);
}
