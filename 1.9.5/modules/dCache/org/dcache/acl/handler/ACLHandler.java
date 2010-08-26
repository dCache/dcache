package org.dcache.acl.handler;

import org.dcache.acl.ACL;
import org.dcache.acl.ACLException;

/**
 * The ACL handler provides interfaces for manipulating ACLs.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public interface ACLHandler {
    /**
     * Returns the access control information of the resource specified by the resource ID
     * parameter.
     *
     * @param rsId
     *            resource ID
     * @return Returns ACL
     * @throws ACLException
     */
    public ACL getACL(String rsId) throws ACLException;

    /**
     * Sets the access control information of the resource specified by the resource ID parameter.
     *
     * @param acl
     *            ACL
     * @return Returns true if operation succeed, otherwise false
     * @throws ACLException
     */
    public boolean setACL(ACL acl) throws ACLException;

    /**
     * Removes the ACL specified by the resource ID parameter.
     *
     * @param rsId
     *            resource ID
     * @return Returns number of removed ACEs
     * @throws ACLException
     */
    public int removeACL(String rsId) throws ACLException;
}
