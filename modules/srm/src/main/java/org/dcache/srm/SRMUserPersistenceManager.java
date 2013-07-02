package org.dcache.srm;

public interface SRMUserPersistenceManager
{
    SRMUser find(long persistenceId);
}
