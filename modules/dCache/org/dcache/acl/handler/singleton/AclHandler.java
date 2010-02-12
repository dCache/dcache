/**
 *
 */
package org.dcache.acl.handler.singleton;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.acl.ACL;
import org.dcache.acl.ACLException;
import org.dcache.acl.config.AclConfig;

/**
 * Draft version. Only for Testing.
 *
 * Generic component for managing the ACLs. It provides an interface to
 * the ACL database table and methods to retrieve and manipulate ACLs.
 *
 * Note: Implemented as singleton class.
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class AclHandler {

    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + AclHandler.class.getName());

    private static org.dcache.acl.handler.DefaultACLHandler _aclHandler;
    private static AclConfig _aclConfig;

    private static AclHandler _SINGLETON;
    static {
        _SINGLETON = new AclHandler();
    }

    private AclHandler() {
        try {
            if (_aclConfig != null)
                _aclHandler = new org.dcache.acl.handler.DefaultACLHandler(_aclConfig);

        } catch (ACLException e) {
            logger.error("Initialize ACL handler failed, reason: " + e.getMessage());
        }
    }

    public static AclHandler instance() {
        return _SINGLETON;
    }

    public static void refresh() {
        _SINGLETON = new AclHandler();
    }

    public static ACL getACL(String rsId) throws ACLException {
        if (_aclHandler == null)
            throw new ACLException("aclHandler is NULL");

        return _aclHandler.getACL(rsId);
    }

    public static int removeACL(String rsId) throws ACLException {
        if (_aclHandler == null)
            throw new ACLException("aclHandler is NULL");

        return _aclHandler.removeACL(rsId);
    }

    public static boolean setACL(ACL acl) throws ACLException {
        if (_aclHandler == null)
            throw new ACLException("aclHandler is NULL");

        return _aclHandler.setACL(acl);
    }

    public static void close() throws ACLException {
        if (_aclHandler != null)
            _aclHandler.close();
    }

    public static void setAclConfig(Properties aclProps) throws ACLException {
        _aclConfig = new AclConfig(aclProps);
        refresh();
    }

    public static void setAclConfig(String configFile) throws ACLException {
        _aclConfig = new AclConfig(configFile);
        refresh();
    }

    public static void setAclConfig(AclConfig aclConfig) {
        _aclConfig = aclConfig;
        refresh();
    }

    public static AclConfig getAclConfig() {
        return _aclConfig;
    }
}
