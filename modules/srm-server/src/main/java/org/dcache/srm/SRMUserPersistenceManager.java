package org.dcache.srm;

public interface SRMUserPersistenceManager
{
    /**
     * Retrieves a user that was previously persisted.
     *
     * @param clientHost Internet address from which the client initiated the original request
     * @param persistenceId An identifier associated with the user - how this relation was
     *                      established is not a concern of this interface.
     */
    SRMUser find(String clientHost, long persistenceId);

    /**
     * Returns an anonymous user. This is used for requests that do not reference any user. This
     * is typically only the case when the user identifier has been removed from the database
     * due to schema changes.
     */
    SRMUser createAnonymous();
}
